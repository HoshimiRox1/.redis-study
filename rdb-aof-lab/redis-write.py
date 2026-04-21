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


def build_parser() -> argparse.ArgumentParser:
    """定义写入脚本的命令行参数。"""
    parser = argparse.ArgumentParser(
        description="Continuously write Redis experiment data to localhost:6380."
    )
    parser.add_argument("--host", default="127.0.0.1", help="Redis host")
    parser.add_argument("--port", default=6380, type=int, help="Redis port")
    parser.add_argument("--db", default=0, type=int, help="Redis database index")
    parser.add_argument(
        "--prefix",
        default=DEFAULT_PREFIX,
        help="Base prefix for generated experiment keys",
    )
    parser.add_argument(
        "--batch-size",
        default=100,
        type=int,
        help="How many keys to write per pipeline batch",
    )
    parser.add_argument(
        "--sleep",
        default=0.2,
        type=float,
        help="Sleep seconds between batches",
    )
    parser.add_argument(
        "--payload-size",
        default=256,
        type=int,
        help="Approximate payload size for each value",
    )
    parser.add_argument(
        "--ttl",
        default=0,
        type=int,
        help="Optional TTL for generated keys in seconds; 0 means persistent",
    )
    parser.add_argument(
        "--recent-keep",
        default=20,
        type=int,
        help="How many recent keys to keep in the helper list",
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
        print(f"Redis connection failed: {exc}", file=sys.stderr)
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
        },
    )
    print(
        "Starting Redis writer:",
        f"host={args.host}",
        f"port={args.port}",
        f"db={args.db}",
        f"prefix={args.prefix}",
        f"run_id={run_id}",
    )

    # 持续制造写流量，直到用户按下 Ctrl+C。
    while not stop:
        next_seq = write_batch(
            client=client,
            prefix=args.prefix,
            run_id=run_id,
            seq_start=next_seq,
            batch_size=args.batch_size,
            payload_size=args.payload_size,
            ttl=args.ttl,
            recent_keep=args.recent_keep,
        )
        stats.total_batches += 1
        stats.total_writes += args.batch_size

        # 定期打印进度，方便确认写入器仍在工作。
        if stats.total_batches % 5 == 0:
            print(
                f"[{utc_now_iso()}] batches={stats.total_batches} "
                f"writes={stats.total_writes} qps={stats.qps():.1f}"
            )

        if args.sleep > 0:
            time.sleep(args.sleep)

    print(
        f"Stopped. total_writes={stats.total_writes} "
        f"total_batches={stats.total_batches} qps={stats.qps():.1f}"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
