[RedisLV-支持基于LevelDB的数据磁盘存储方式](https://github.com/ivanabc/RedisLV)
---

### why
---
1. save方式保存数据耗内存
2. aof方式保存数据恢复慢

### RedisLV优点
1. 数据落地不会带来额外的内存消耗
2. 服务启动快

### RedisLV缺点
1. redis更改数据操作同时更改leveldb, 导致损耗部分性能

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

### RedisLV备份命令
```
redis-cli backup dir(备份文件目录)
```
* 当目录中包含BACKUP.log文件并且文件中有SUCCESS字段，表示备份成功

