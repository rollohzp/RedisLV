[RedisRT-支持基于LevelDB的数据磁盘存储方式](https://github.com/ivanabc/RedisRT)
========================================

###Why
===
1. Save耗内存
2. Aof恢复慢

###RedisRT目前支持的Redis写入操作
===

| Key    | Hash         | Set    | SortedSet |
| ------ | ------------ | ------ | --------- |
| DEL    | HINCRBY      | SADD   | ZADD      |
|        | HINCRBYFLOAT | SREM   | ZINCRBY   | 
|        | HMSET        |        | ZREM      |
|        | HSET         |        |           |
|        | HSETNX       |        |           |
