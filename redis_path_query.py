import redis
import hashlib
import sys


def query_redis(path, redis_host="localhost", redis_port=6379, redis_db=0):
    # 连接到 Redis
    r = redis.Redis(host=redis_host, port=redis_port, db=redis_db)

    # 生成哈希键
    hashed_key = generate_hash(path)  # 替换为您的哈希函数
    print(f"Querying Redis for key: {hashed_key}")

    # 查询数据
    data = r.get(hashed_key)
    if data:
        print(f"Data found for {path}: {data}")
    else:
        print(f"No data found for {path}")


def generate_hash(s):
    hasher = hashlib.sha256()
    hasher.update(s.encode())
    hash_value = hasher.hexdigest()
    print(f"Generated hash for '{s}': {hash_value}")
    return hash_value


if __name__ == "__main__":
    path = "/media/mirror2/music/经典老歌/华语群星/郑秀文/郑秀文/【车载CD定制、自选歌曲、自排曲目】郑秀文《2002 我左眼见到鬼电影原声碟》[WAV 分轨]/郑秀文 - 我左眼见到鬼电影原声CD.wav"
    query_redis(path)
