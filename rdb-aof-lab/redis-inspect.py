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
import sys
import time
from datetime import datetime, timezone

import redis


DEFAULT_PREFIX = "lab:redis"


def parse_args() -> argparse.Namespace:
    """定义检查脚本的命令行参数。"""
    parser = argparse.ArgumentParser(
        description="Inspect Redis experiment data on localhost:6380."
    )
    parser.add_argument("--host", default="127.0.0.1", help="Redis host")
    parser.add_argument("--port", default=6380, type=int, help="Redis port")
    parser.add_argument("--db", default=0, type=int, help="Redis database index")
    parser.add_argument(
        "--prefix",
        default=DEFAULT_PREFIX,
        help="Base prefix used by redis-write.py",
    )
    parser.add_argument(
        "--samples",
        default=5,
        type=int,
        help="How many recent keys to inspect",
    )
    parser.add_argument(
        "--watch",
        default=0,
        type=float,
        help="Refresh interval in seconds; 0 means run once",
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


def print_snapshot(client: redis.Redis, prefix: str, samples: int) -> None:
    """打印一份当前 Redis 状态快照，便于人工观察。"""
    info = client.info()
    persistence = client.info("persistence")
    memory = client.info("memory")
    keyspace = client.info("keyspace")

    # 这些辅助 key 由 redis-write.py 维护。
    recent_list = f"{prefix}:recent"
    meta_hash = f"{prefix}:meta"
    recent_keys = client.lrange(recent_list, 0, max(samples - 1, 0))
    meta = client.hgetall(meta_hash)
    db_index = client.connection_pool.connection_kwargs.get("db", 0)
    db_stats = keyspace.get(f"db{db_index}", {})

    print("=" * 72)
    print(f"time: {datetime.now().isoformat(timespec='seconds')}")
    print(
        f"redis_version={info.get('redis_version')} "
        f"role={info.get('role')} connected_clients={info.get('connected_clients')}"
    )
    print(
        f"db{db_index} keys={db_stats.get('keys', 0)} "
        f"expires={db_stats.get('expires', 0)}"
    )
    print(
        f"used_memory_human={memory.get('used_memory_human')} "
        f"aof_enabled={persistence.get('aof_enabled')} "
        f"rdb_bgsave_in_progress={persistence.get('rdb_bgsave_in_progress')}"
    )
    print(
        f"loading={persistence.get('loading')} "
        f"aof_rewrite_in_progress={persistence.get('aof_rewrite_in_progress')} "
        f"last_save_time={persistence.get('rdb_last_save_time')}"
    )

    if meta:
        print(
            f"run_id={meta.get('run_id')} total_writes={meta.get('total_writes')} "
            f"last_seq={meta.get('last_seq')} last_write={meta.get('last_write_at')} "
            f"({format_ago(meta.get('last_write_at'))})"
        )
    else:
        print("meta: no writer metadata found yet")

    if not recent_keys:
        print("recent samples: none")
        return

    print("recent samples:")
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
        print(f"- {key} ttl={ttl} value={preview}")


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
        print(f"Redis connection failed: {exc}", file=sys.stderr)
        return 1

    while True:
        try:
            print_snapshot(client=client, prefix=args.prefix, samples=args.samples)
        except redis.RedisError as exc:
            print(f"Inspect failed: {exc}", file=sys.stderr)
            return 1

        # watch=0 表示只打印一次；大于 0 时按间隔持续刷新。
        if args.watch <= 0:
            return 0
        time.sleep(args.watch)


if __name__ == "__main__":
    raise SystemExit(main())
