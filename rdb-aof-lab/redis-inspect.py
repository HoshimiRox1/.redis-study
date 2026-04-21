#!/usr/bin/env python
from __future__ import annotations

"""在持久化实验运行时检查 Redis 当前状态。

这个脚本会同时读取两类信息：
1. Redis 服务器本身的指标和持久化状态。
2. redis-write.py 维护的辅助 key。

这样可以观察：
- 写入是否还在持续发生
- 当前数据库里大概有多少 key
- RDB/AOF 是否正在工作
- 最近几条样本数据长什么样
"""

import argparse
import json
import re
import sys
import time
from datetime import datetime, timezone

import redis


DEFAULT_PREFIX = "lab:redis"
SEQ_PATTERN = re.compile(r":data:(\d+)$")


def parse_args() -> argparse.Namespace:
    """定义检查脚本的命令行参数。"""
    parser = argparse.ArgumentParser(
        description="检查 Redis 当前状态，默认连接本机 6380 端口。",
        add_help=False,
    )
    parser.add_argument("-h", "--help", action="help", help="显示帮助信息并退出")
    parser.add_argument("--host", default="127.0.0.1", help="Redis 主机地址")
    parser.add_argument("--port", default=6380, type=int, help="Redis 端口")
    parser.add_argument("--db", default=0, type=int, help="Redis 数据库编号")
    parser.add_argument(
        "--prefix",
        default=DEFAULT_PREFIX,
        help="redis-write.py 使用的数据前缀",
    )
    parser.add_argument(
        "--samples",
        default=5,
        type=int,
        help="查看最近多少条样本 key",
    )
    parser.add_argument(
        "--watch",
        default=0,
        type=float,
        help="刷新间隔，单位秒；0 表示只执行一次",
    )
    parser.add_argument(
        "--gap-limit",
        default=10,
        type=int,
        help="最多展示多少段缺失区间",
    )
    parser.add_argument(
        "--expected-last-seq",
        default=0,
        type=int,
        help="如果你记下了宕机前最后看到的 seq，可填在这里用于计算实际尾部丢失",
    )
    return parser.parse_args()


def safe_json_loads(value: str | None) -> object:
    """尝试把字符串解析成 JSON；失败时直接返回原始值。"""
    if not value:
        return value
    try:
        return json.loads(value)
    except json.JSONDecodeError:
        return value


def format_ago(iso_timestamp: str | None) -> str:
    """把 ISO 时间戳转成“距今多久”这样的可读文本。"""
    if not iso_timestamp:
        return "unknown"
    try:
        written_at = datetime.fromisoformat(iso_timestamp)
    except ValueError:
        return iso_timestamp

    if written_at.tzinfo is None:
        written_at = written_at.replace(tzinfo=timezone.utc)
    delta = datetime.now(timezone.utc) - written_at.astimezone(timezone.utc)
    return f"{delta.total_seconds():.1f}s ago"


def parse_seq_from_key(key: str) -> int | None:
    """从 key 名里提取序号，例如从 `xxx:data:000000123` 提取 123。"""
    match = SEQ_PATTERN.search(key)
    if not match:
        return None
    return int(match.group(1))


def collect_data_keys(client: redis.Redis, prefix: str) -> tuple[list[str], list[int]]:
    """扫描当前前缀下的所有实验数据 key，并提取它们的 seq。"""
    pattern = f"{prefix}:data:*"
    keys = list(client.scan_iter(match=pattern, count=1000))
    seqs: list[int] = []
    for key in keys:
        seq = parse_seq_from_key(key)
        if seq is not None:
            seqs.append(seq)
    seqs.sort()
    return keys, seqs


def summarize_gaps(seqs: list[int]) -> list[tuple[int, int]]:
    """把序号缺口整理成区间列表，例如 [(4, 6), (11, 11)]。"""
    if len(seqs) < 2:
        return []

    gaps: list[tuple[int, int]] = []
    previous = seqs[0]
    for current in seqs[1:]:
        if current > previous + 1:
            gaps.append((previous + 1, current - 1))
        previous = current
    return gaps


def gap_count(gaps: list[tuple[int, int]]) -> int:
    """统计所有缺口一共缺了多少条 seq。"""
    return sum(end - start + 1 for start, end in gaps)


def format_gap_ranges(gaps: list[tuple[int, int]], gap_limit: int) -> str:
    """把缺口区间列表格式化成更易读的文本。"""
    if not gaps:
        return "无缺口"

    display = gaps[: max(gap_limit, 0)]
    parts = []
    for start, end in display:
        parts.append(str(start) if start == end else f"{start}-{end}")

    suffix = ""
    if gap_limit >= 0 and len(gaps) > gap_limit:
        suffix = f" ... 还有 {len(gaps) - gap_limit} 段未展示"
    return "，".join(parts) + suffix


def print_server_snapshot(
    client: redis.Redis,
    meta: dict[str, str],
    prefix: str,
    gap_limit: int,
    samples: int,
    expected_last_seq: int,
) -> None:
    """打印 Redis 服务状态和当前实验前缀的数据概况。"""
    info = client.info()
    persistence = client.info("persistence")
    memory = client.info("memory")
    keyspace = client.info("keyspace")

    # 这些辅助 key 由 redis-write.py 维护。
    recent_list = f"{prefix}:recent"
    recent_keys = client.lrange(recent_list, 0, max(samples - 1, 0))
    db_index = client.connection_pool.connection_kwargs.get("db", 0)
    db_stats = keyspace.get(f"db{db_index}", {})
    _, seqs = collect_data_keys(client=client, prefix=prefix)
    gaps = summarize_gaps(seqs)
    min_seq = seqs[0] if seqs else None
    max_seq = seqs[-1] if seqs else None
    data_key_count = len(seqs)

    print("=" * 72)
    print(f"时间：{datetime.now().isoformat(timespec='seconds')}")
    print(
        f"Redis 版本={info.get('redis_version')} "
        f"角色={info.get('role')} 已连接客户端={info.get('connected_clients')}"
    )
    print(
        f"db{db_index} key 数={db_stats.get('keys', 0)} "
        f"设置过期的 key 数={db_stats.get('expires', 0)}"
    )
    print(
        f"已用内存={memory.get('used_memory_human')} "
        f"AOF 已开启={persistence.get('aof_enabled')} "
        f"RDB 后台保存中={persistence.get('rdb_bgsave_in_progress')}"
    )
    print(
        f"加载中={persistence.get('loading')} "
        f"AOF 重写中={persistence.get('aof_rewrite_in_progress')} "
        f"最近一次 RDB 保存时间={persistence.get('rdb_last_save_time')}"
    )
    print(f"当前实验前缀：{prefix}")

    if meta:
        last_seq = int(meta.get("last_seq", "0") or 0)
        total_writes = int(meta.get("total_writes", "0") or 0)
        target_writes = int(meta.get("target_writes", "0") or 0)
        tail_loss = max(last_seq - (max_seq or 0), 0)
        print(
            f"run_id={meta.get('run_id')} 写入总数={total_writes} "
            f"最后序号={last_seq} 最近写入时间={meta.get('last_write_at')} "
            f"({format_ago(meta.get('last_write_at'))})"
        )
        print(
            f"目标写入数={target_writes if target_writes > 0 else '持续写入'} "
            f"恢复出的最大 seq={max_seq or 0} 估算尾部丢失={tail_loss}"
        )
    else:
        print("元数据：暂时还没有发现写入器留下的状态信息")

    if expected_last_seq > 0:
        actual_tail_loss = max(expected_last_seq - (max_seq or 0), 0)
        print(
            f"按你记录的宕机前最后 seq={expected_last_seq} 计算，"
            f"实际尾部丢失={actual_tail_loss}"
        )

    if not seqs:
        print("当前前缀下的数据 key 数=0")
        print("最近样本：暂无")
        return

    print(
        f"当前前缀下的数据 key 数={data_key_count} "
        f"最小 seq={min_seq} 最大 seq={max_seq}"
    )
    print(
        f"序号缺口总数={gap_count(gaps)} "
        f"缺口区间={format_gap_ranges(gaps, gap_limit)}"
    )

    if not recent_keys:
        print("最近样本：暂无")
        return

    print("最近样本：")
    for key in recent_keys:
        # ttl=-1 表示永久 key，ttl=-2 表示这个 key 已经不存在了。
        ttl = client.ttl(key)
        value = safe_json_loads(client.get(key))
        if isinstance(value, dict):
            # 这里只打印最关键的字段，避免输出太长不易读。
            preview = {
                "seq": value.get("seq"),
                "run_id": value.get("run_id"),
                "written_at": value.get("written_at"),
            }
        else:
            preview = value
        print(f"- key={key} ttl={ttl} value={preview}")


def print_snapshot(
    client: redis.Redis,
    prefix: str,
    samples: int,
    gap_limit: int,
    expected_last_seq: int,
) -> None:
    """打印一份当前 Redis 状态快照，便于人工观察。"""
    meta_hash = f"{prefix}:meta"
    meta = client.hgetall(meta_hash)
    print_server_snapshot(
        client=client,
        meta=meta,
        prefix=prefix,
        gap_limit=gap_limit,
        samples=samples,
        expected_last_seq=expected_last_seq,
    )


def main() -> int:
    """检查脚本入口函数。"""
    args = parse_args()
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

    while True:
        try:
            print_snapshot(
                client=client,
                prefix=args.prefix,
                samples=args.samples,
                gap_limit=args.gap_limit,
                expected_last_seq=args.expected_last_seq,
            )
        except redis.RedisError as exc:
            print(f"检查失败：{exc}", file=sys.stderr)
            return 1

        # watch=0 表示只打印一次；大于 0 时按间隔持续刷新。
        if args.watch <= 0:
            return 0
        time.sleep(args.watch)


if __name__ == "__main__":
    raise SystemExit(main())
