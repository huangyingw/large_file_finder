import redis
import hashlib
import sys


def query_redis(path, redis_host="localhost", redis_port=6379, redis_db=0):
    # 连接到 Redis
    r = redis.Redis(host=redis_host, port=redis_port, db=redis_db)

    # 生成哈希键
    hashed_key = generate_hash(path)  # 替换为您的哈希函数
    file_info = r.get(hashed_key)
    if file_info:
        print(f"Data found for {path}: {file_info}")

        # 获取并打印文件哈希值
        file_hash = r.get("fileHashToPathset:" + hashed_key)
        if file_hash:
            print(f"File hash for {path}: {file_hash.decode('utf-8')}")
        else:
            print(f"No file hash found for {path}")
    else:
        print(f"No data found for {path}")


def generate_hash(s):
    hasher = hashlib.sha256()
    hasher.update(s.encode())
    hash_value = hasher.hexdigest()
    print(f"Generated hash for '{s}': {hash_value}")
    return hash_value


if __name__ == "__main__":
    query_redis(
        "/media/mirror2/music/经典老歌/华语群星/郑秀文/郑秀文/【车载CD定制、自选歌曲、自排曲目】郑秀文《2002 我左眼见到鬼电影原声碟》[WAV 分轨]/郑秀文 - 我左眼见到鬼电影原声CD.wav"
    )
    query_redis(
        "/media/av91/av/旬果/Pizzaboy and Hubby Creampied Me Successively.vd.1080.mp4.bak"
    )
    query_redis(
        "/media/av91/av/旬果/Pizzaboy and Hubby Creampied Me Successively.vd.1080.mp4"
    )
    query_redis(
        "/media/secure_bcache/av/onlyfans/OnlyFans.2022.Musclebarbie.Primal.Instincts.Anal.XXX.1080p.HEVC.x265.PRT[XvX]/OnlyFans.2022.Musclebarbie.Primal.Instincts.Anal.XXX.1080p.HEVC.x265.PRT.mkv"
    )
    query_redis(
        "/media/secure_bcache/av/onlyfans/OnlyFans.22.11.10.Lani.Rails.Aka.HotSouthernFreedom.A27hopsonxxx.XXX.720p.HEVC.x265.PRT[XvX]/OnlyFans.22.11.10.Lani.Rails.Aka.HotSouthernFreedom.A27hopsonxxx.XXX.720p.HEVC.x265.PRT.mkv"
    )
