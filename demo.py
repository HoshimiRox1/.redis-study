#   实战目标:"学习营任务台"

#   - Hash：存用户资料
#   - String：存登录次数计数器
#   - List：存待完成任务队列
#   - Set：存今天签到过的用户
#   - ZSet：存积分排行榜

from datetime import date
import shlex
import redis

# 1. 连接 Redis
r = redis.Redis(
    host="127.0.0.1",
    port=6379,
    decode_responses=True,
)

# 2. 统一定义本次小实战会用到的 key
today = date.today().isoformat()
checkin_key = f"checkin:{today}"
queue_key = "study:task_queue"
rank_key = "study:rank"
feed_key = "study:feed"

user_ids = ["1", "2", "3"]


def reset_demo():
    # 清空本次演示会用到的 key，保证每次启动都是干净环境
    keys = [
        checkin_key,
        queue_key,
        rank_key,
        feed_key,
        "user:1", "user:2", "user:3",
        "user:1:login_count", "user:2:login_count", "user:3:login_count",
    ]
    r.delete(*keys)


def add_feed(msg):
    # 用 List 记录最近动态
    r.lpush(feed_key, msg)
    r.ltrim(feed_key, 0, 9)


def create_user(user_id, name, city):
    # 用 Hash 存对象型数据：用户资料
    r.hset(
        f"user:{user_id}",
        mapping={
            "name": name,
            "city": city,
            "level": "bronze",
        },
    )

    # 用 String 存计数器：登录次数
    r.set(f"user:{user_id}:login_count", 0)


def init_demo_data():
    # 程序启动时初始化数据库
    reset_demo()

    create_user("1", "Rover", "Shanghai")
    create_user("2", "Mika", "Hangzhou")
    create_user("3", "Nina", "Suzhou")

    add_feed("demo initialized")


def login(user_id):
    # String: 登录次数 +1
    count = r.incr(f"user:{user_id}:login_count")
    add_feed(f"user:{user_id} login -> count={count}")
    print(f"user:{user_id} 登录成功，当前登录次数: {count}")


def checkin(user_id):
    # Set: 今天是否已经签到过
    added = r.sadd(checkin_key, user_id)

    if added == 1:
        # 第一次签到才加分
        score = r.zincrby(rank_key, 5, user_id)
        add_feed(f"user:{user_id} checkin -> +5 points")
        print(f"user:{user_id} 签到成功，当前积分: {score}")
    else:
        add_feed(f"user:{user_id} duplicate checkin -> ignored")
        print(f"user:{user_id} 今天已经签到过了")


def publish_tasks():
    # List: 向任务队列尾部追加任务
    r.rpush(queue_key, "task:redis-string")
    r.rpush(queue_key, "task:redis-list")
    r.rpush(queue_key, "task:redis-zset")
    add_feed("3 tasks published")
    print("已发布 3 个任务到队列")


def finish_one_task(user_id):
    # List: 从队列头部取一个任务
    task = r.lpop(queue_key)

    if not task:
        add_feed(f"user:{user_id} tried to take task -> queue empty")
        print("任务队列为空，没有可完成的任务")
        return

    # ZSet: 完成任务后加分
    score = r.zincrby(rank_key, 20, user_id)
    add_feed(f"user:{user_id} finished {task} -> +20 points")
    print(f"user:{user_id} 完成了 {task}，当前积分: {score}")


def show_state():
    # 把当前 Redis 状态完整打印出来，方便观察
    print("\n===== 用户资料 Hash =====")
    for user_id in user_ids:
        print(f"user:{user_id}", r.hgetall(f"user:{user_id}"))

    print("\n===== 登录次数 String =====")
    for user_id in user_ids:
        print(f"user:{user_id}:login_count =", r.get(f"user:{user_id}:login_count"))

    print(f"\n===== 今日签到 Set: {checkin_key} =====")
    print(r.smembers(checkin_key))

    print("\n===== 任务队列 List =====")
    print(r.lrange(queue_key, 0, -1))

    print("\n===== 积分榜 ZSet =====")
    print(r.zrevrange(rank_key, 0, -1, withscores=True))

    print("\n===== 最新动态 List =====")
    print(r.lrange(feed_key, 0, -1))
    print()


def help_cmd():
    print(
        """
可用命令：
help
show_state
publish_tasks
login <user_id>
checkin <user_id>
finish_one_task <user_id>
reset_demo
exit

示例：
login 1
checkin 2
publish_tasks
finish_one_task 1
show_state
""".strip()
    )


def dispatch(command_line):
    # 解析终端输入
    parts = shlex.split(command_line)
    if not parts:
        return

    cmd = parts[0]
    args = parts[1:]

    # 让你输入的命令名，直接对应到函数行为
    if cmd == "help":
        help_cmd()
    elif cmd == "show_state":
        show_state()
    elif cmd == "publish_tasks":
        publish_tasks()
    elif cmd == "login":
        if len(args) != 1:
            print("用法: login <user_id>")
            return
        login(args[0])
    elif cmd == "checkin":
        if len(args) != 1:
            print("用法: checkin <user_id>")
            return
        checkin(args[0])
    elif cmd == "finish_one_task":
        if len(args) != 1:
            print("用法: finish_one_task <user_id>")
            return
        finish_one_task(args[0])
    elif cmd == "reset_demo":
        init_demo_data()
        print("演示数据已重置")
    elif cmd in {"exit", "quit"}:
        raise SystemExit
    else:
        print("未知命令，输入 help 查看可用命令")


def main():
    print("PING ->", r.ping())

    # 启动时初始化数据库
    init_demo_data()

    print("Redis 学习营任务台已启动")
    print("输入 help 查看命令\n")

    # 主循环：持续等待用户输入
    while True:
        try:
            command_line = input("redis-demo> ").strip()
            dispatch(command_line)
        except KeyboardInterrupt:
            print("\n收到中断，输入 exit 可退出")
        except SystemExit:
            print("Bye")
            break
        except Exception as e:
            print(f"执行失败: {e}")


if __name__ == "__main__":
    main()