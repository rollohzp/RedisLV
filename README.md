[RedisLV-支持基于LevelDB的数据磁盘存储方式](https://github.com/ivanabc/RedisLV)
========================================

###Why

1. Save耗内存
2. Aof恢复慢

###RedisLV目前支持的Redis写入操作

| Key    | Hash         | Set    | SortedSet        |
| ------ | ------------ | ------ | ---------------- |
| DEL    | HINCRBY      | SADD   | ZADD             |
|        | HINCRBYFLOAT | SREM   | ZINCRBY          | 
|        | HMSET        |        | ZREM             |
|        | HSET         |        | ZREMRANGEBYRANK  |
|        | HSETNX       |        | ZREMRANGEBYSCORE |
|        |              |        | ZREMRANGEBYLEX   |
