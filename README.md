[RedisLV-支持基于LevelDB的数据磁盘存储方式](https://github.com/ivanabc/RedisLV)
---

### why
---
1. save方式保存数据耗内存
2. Aof方式保存数据恢复慢

### Redis命令支持状况(yes: 支持; no: 不支持)
---

| Key         |     |
|-------------|-----|
| DEL         | yes |
| DUMP        | yes |
| EXISTS      | yes |
| EXPIRE      | no  |
| EXPIREAT    | no  |
| KEYS        | yes |
| MIGRATE     | no  |
| MOVE        | no  |
| OBJECT      | yes |
| PERSIST     | no  |
| PEXPIRE     | no  |
| PEXPIREAT   | no  |
| PTTL        | no  |
| RANDOMKEY   | yes |
| RENAME      | no  |
| RENAMENX    | no  |
| RESTORE     | no  |
| SORT        | yes |
| TTL         | no  |
| TYPE        | yes |
| SCAN        | yes |

| Hash        |     |
|-------------|-----|
| HDEL        | yes | 
| HEXISTS     | yes |
| HGET        | yes |
| HGETALL     | yes |
| HINCRBY     | yes |
| HINCRBYFLOAT| yes |
| HKEYS       | yes |
| HLEN        | yes |
| HMGET       | yes |
| HMSET       | yes |
| HSET        | yes |
| HSETNX      | yes |
| HVALS       | yes |
| HSCAN       | yes |

| Set         |     |
|-------------|-----|
| SADD        | yes |
| SCARD       | yes |
| SDIFF       | yes |
| SDIFFSTORE  | no  |
| SINTER      | yes |
| SINTERSTORE | no  |
| SISMEMBERS  | yes |
| SMEMBERS    | yes |
| SMOVE       | no  |
| SPOP        | no  |
| SRANDMEMBER | yes |
| SREM        | yes |
| SUNION      | yes |
| SUNIONSTORE | no  |
| SSCAN       | yes |

| SortedSet       |     |
|-----------------|-----|
| ZADD            | yes |
| ZCARD           | yes |
| ZCOUNT          | yes |
| ZINCRBY         | yes |
| ZRANGE          | yes |
| ZRANGEBYSCORE   | yes |
| ZRANK           | yes |
| ZREM            | yes |
| ZREMRANGEBYRANK | yes |  
| ZREMRANGEBYSCORE| yes |
| ZREVRANGE       | yes |
| ZREVRANKBYSCORE | yes |
| ZREVRANK        | yes |
| ZSCORE          | yes |
| ZUNIONSTORE     | no  |
| ZINTERSTORE     | no  |
| ZSCAN           | yes |
| ZRANGEBYLEX     | yes |
| ZLEXCOUNT       | yes |
| ZREMRANGEBYLEX  | yes |  

