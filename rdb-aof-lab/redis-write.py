#!/usr/bin/env python
from __future__ import annotations

"""持续向 Redis 生成写流量，供持久化实验使用。

这个脚本扮演一个简单的客户端写入器：
1. 默认连接到本机 6380 端口上的 Redis。
2. 按批次持续写入结构化数据。
3. 额外维护一组辅助元数据，方便另一个检查脚本观察当前状态。
"""

import argparse
import json
import signal
import sys
import time
import uuid
from dataclasses import dataclass
from datetime import datetime, timezone

import redis


DEFAULT_PREFIX = "lab:redis"


def utc_now_iso() -> str:
    """返回当前 UTC 时间，格式为紧凑的 ISO 字符串。"""
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


@dataclass
class WriterStats:
    """记录当前写入进程的运行统计信息。"""

    total_writes: int = 0
    total_batches: int = 0
    started_at: float = time.perf_counter()

    def qps(self) -> float:
        """估算从进程启动到现在的每秒写入次数。"""
        elapsed = max(time.perf_counter() - self.started_at, 1e-9)
        return self.total_writes / elapsed


def positive_int(value: str) -> int:
    """把命令行参数解析成正整数。"""
    number = int(value)
    if number <= 0:
        raise argparse.ArgumentTypeError("该参数必须是大于 0 的整数")
    return number


def build_parser() -> argparse.ArgumentParser:
    """定义写入脚本的命令行参数。"""
    parser = argparse.ArgumentParser(
        description="持续向 Redis 写入实验数据，默认连接本机 6380 端口。",
        add_help=False,
    )
    parser.add_argument("-h", "--help", action="help", help="显示帮助信息并退出")
    parser.add_argument("--host", default="127.0.0.1", help="Redis 主机地址")
    parser.add_argument("--port", default=6380, type=int, help="Redis 端口")
    parser.add_argument("--db", default=0, type=int, help="Redis 数据库编号")
    parser.add_argument(
        "--prefix",
        default=DEFAULT_PREFIX,
        help="实验数据 key 的统一前缀",
    )
    parser.add_argument(
        "--batch-size",
        default=100,
        type=positive_int,
        help="每个 pipeline 批次写入多少个 key",
    )
    parser.add_argument(
        "--sleep",
        default=0.2,
        type=float,
        help="每个批次之间休眠多少秒",
    )
    parser.add_argument(
        "--payload-size",
        default=256,
        type=positive_int,
        help="每条 value 的近似大小",
    )
    parser.add_argument(
        "--ttl",
        default=0,
        type=int,
        help="给实验 key 设置过期时间，单位秒；0 表示不过期",
    )
    parser.add_argument(
        "--recent-keep",
        default=20,
        type=positive_int,
        help="辅助列表里保留最近多少个 key",
    )
    parser.add_argument(
        "--count",
        default=0,
        type=int,
        help="总共写入多少条数据；0 表示持续写到手动停止",
    )
    return parser


def create_payload(seq: int, run_id: str, payload_size: int) -> str:
    """构造一条要写入 Redis 的 JSON 值。

    每条记录里包含：
    - run_id：标识是哪一次写入进程生成的数据
    - seq：递增序号，便于检查顺序和是否丢数据
    - written_at：写入时间
    - filler：填充内容，用来放大数据量，方便观察持久化文件变化
    """
    filler_len = max(payload_size - 128, 16)
    payload = {
        "run_id": run_id,
        "seq": seq,
        "written_at": utc_now_iso(),
        "filler": "x" * filler_len,
    }
    return json.dumps(payload, ensure_ascii=True, separators=(",", ":"))


def write_batch(
    client: redis.Redis,
    prefix: str,
    run_id: str,
    seq_start: int,
    batch_size: int,
    payload_size: int,
    ttl: int,
    recent_keep: int,
) -> int:
    """写入一批 key，并同步更新辅助元数据。

    除了真实实验数据外，这个函数还会维护：
    - `<prefix>:recent`：最近写入的 key 列表
    - `<prefix>:meta`：供检查脚本读取的摘要信息

    返回值是下一批写入应当使用的起始序号。
    """
    recent_list = f"{prefix}:recent"
    meta_hash = f"{prefix}:meta"
    pipe = client.pipeline(transaction=False)
    last_key = ""

    for offset in range(batch_size):
        seq = seq_start + offset
        key = f"{prefix}:data:{seq:09d}"
        value = create_payload(seq=seq, run_id=run_id, payload_size=payload_size)
        if ttl > 0:
            pipe.setex(key, ttl, value)
        else:
            pipe.set(key, value)
        # 维护一个“最近写入历史”列表，检查脚本可以快速抽样查看。
        pipe.lpush(recent_list, key)
        last_key = key

    pipe.ltrim(recent_list, 0, max(recent_keep - 1, 0))
    # 把这次运行的最新进度写到单独的 hash 中。
    pipe.hset(
        meta_hash,
        mapping={
            "run_id": run_id,
            "last_seq": seq_start + batch_size - 1,
            "last_key": last_key,
            "last_write_at": utc_now_iso(),
            "last_batch_size": batch_size,
        },
    )
    pipe.hincrby(meta_hash, "total_writes", batch_size)
    pipe.execute()
    return seq_start + batch_size


def next_batch_size(total_writes: int, batch_size: int, count: int) -> int:
    """根据总目标写入量，算出当前这一批实际要写多少条。"""
    if count <= 0:
        return batch_size
    remain = count - total_writes
    return min(batch_size, max(remain, 0))


def main() -> int:
    """写入脚本入口函数。"""
    args = build_parser().parse_args()
    client = redis.Redis(
        host=args.host,
        port=args.port,
        db=args.db,
        decode_responses=True,
        socket_timeout=5,
    )

    try:
        client.ping()
    except redis.RedisError as exc:
        print(f"Redis 连接失败：{exc}", file=sys.stderr)
        return 1

    stop = False

    def handle_stop(signum: int, frame: object) -> None:
        del signum, frame
        nonlocal stop
        stop = True

    signal.signal(signal.SIGINT, handle_stop)
    signal.signal(signal.SIGTERM, handle_stop)

    run_id = uuid.uuid4().hex[:8]
    stats = WriterStats()
    next_seq = 1
    # 启动时先重置本次运行的元数据，避免上一次实验残留信息干扰观察。
    client.hset(
        f"{args.prefix}:meta",
        mapping={
            "run_id": run_id,
            "started_at": utc_now_iso(),
            "last_seq": 0,
            "last_key": "",
            "last_write_at": "",
            "last_batch_size": 0,
            "total_writes": 0,
            "target_writes": args.count,
        },
    )
    print(
        "启动写入器：",
        f"host={args.host}",
        f"port={args.port}",
        f"db={args.db}",
        f"prefix={args.prefix}",
        f"run_id={run_id}",
        f"目标写入={args.count if args.count > 0 else '持续写入'}",
    )

    # 持续制造写流量，直到用户按下 Ctrl+C。
    while not stop:
        current_batch_size = next_batch_size(
            total_writes=stats.total_writes,
            batch_size=args.batch_size,
            count=args.count,
        )
        if current_batch_size <= 0:
            break

        next_seq = write_batch(
            client=client,
            prefix=args.prefix,
            run_id=run_id,
            seq_start=next_seq,
            batch_size=current_batch_size,
            payload_size=args.payload_size,
            ttl=args.ttl,
            recent_keep=args.recent_keep,
        )
        stats.total_batches += 1
        stats.total_writes += current_batch_size

        # 定期打印进度，方便确认写入器仍在工作。
        if stats.total_batches % 5 == 0 or current_batch_size < args.batch_size:
            print(
                f"[{utc_now_iso()}] 批次={stats.total_batches} "
                f"写入总数={stats.total_writes} 每秒写入={stats.qps():.1f}"
            )

        if args.count > 0 and stats.total_writes >= args.count:
            print(f"已达到目标写入数：{args.count}")
            break

        if args.sleep > 0:
            time.sleep(args.sleep)

    print(
        f"写入器已停止。写入总数={stats.total_writes} "
        f"批次数={stats.total_batches} 每秒写入={stats.qps():.1f}"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
