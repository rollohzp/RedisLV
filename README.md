RedisRT-提供基于LevelDB的数据存储方案
=====================================

###Why
1. Save耗内存
2. Aof恢复慢

###RedisRT目前支持的Redis写入操作

| Key    | Hash         | Set    | SortedSet |
| ------ | ------------ | ------ | --------- |
| DEL    | HINCRBY      | SADD   | ZADD      |
|        | HINCRBYFLOAT | SREM   | ZINCRBY   | 
|        | HMSET        |        | ZREM      |
|        | HSET         |        |           |
|        | HSETNX       |        |           |
