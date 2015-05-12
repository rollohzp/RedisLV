#include "redis.h"

#include <pthread.h>

#define LEVELDB_KEY_FLAG_DATABASE_ID 0
#define LEVELDB_KEY_FLAG_TYPE 1 
#define LEVELDB_KEY_FLAG_SET_KEY_LEN 2
#define LEVELDB_KEY_FLAG_SET_KEY 3

int loadleveldb(char *path) {
  redisLog(REDIS_NOTICE, "load leveldb path: %s", path);

  struct redisClient *fakeClient = createFakeClient();
  int old_leveldb_state = server.leveldb_state;
  long loops = 0;

  server.leveldb_state = REDIS_LEVELDB_OFF;
  initleveldb(&server.ldb, path);
  startLoading(NULL);

  int success = 1;
  int dbid = 0;
  char *data = NULL;
  char *value = NULL;
  size_t dataLen = 0;
  size_t valueLen = 0;
  int tmpdbid, tmptype;
  leveldb_iterator_t *iterator = leveldb_create_iterator(server.ldb.db, server.ldb.roptions);

  for(leveldb_iter_seek_to_first(iterator); leveldb_iter_valid(iterator); leveldb_iter_next(iterator)) {
    int argc;
    unsigned long len;
    robj **argv;
    struct redisCommand *cmd;

    if (!(loops++ % 1000000)) {
      processEventsWhileBlocked();
      redisLog(REDIS_NOTICE, "load leveldb: %lu", loops);
    }
    data = (char*) leveldb_iter_key(iterator, &dataLen);
    tmpdbid = data[LEVELDB_KEY_FLAG_DATABASE_ID];
    tmptype = data[LEVELDB_KEY_FLAG_TYPE];
    if(tmpdbid != dbid) {
      if(selectDb(fakeClient,tmpdbid) == REDIS_OK ){
        dbid = tmpdbid;
      }else{
        redisLog(REDIS_WARNING, "load leveldb select db error: %d", tmpdbid);
        success = 0;
        break;
      }
    }
    if(tmptype == 'h'){
      argc = 4;
      argv = zmalloc(sizeof(robj*)*argc);
      fakeClient->argc = argc;
      fakeClient->argv = argv;
      argv[0] = createStringObject("hset",4);
      len = data[LEVELDB_KEY_FLAG_SET_KEY_LEN];
      argv[1] = createStringObject(data+LEVELDB_KEY_FLAG_SET_KEY,len);
      argv[2] = createStringObject(data+LEVELDB_KEY_FLAG_SET_KEY+len+1,dataLen-LEVELDB_KEY_FLAG_SET_KEY-len-1);
      value = (char*) leveldb_iter_value(iterator, &valueLen);
      argv[3] = createStringObject(value, valueLen);
    }else if(tmptype == 's'){
      argc = 3;
      argv = zmalloc(sizeof(robj*)*argc);
      fakeClient->argc = argc;
      fakeClient->argv = argv;
      argv[0] = createStringObject("sadd",4);
      len = data[LEVELDB_KEY_FLAG_SET_KEY_LEN];
      argv[1] = createStringObject(data+LEVELDB_KEY_FLAG_SET_KEY,len);
      argv[2] = createStringObject(data+LEVELDB_KEY_FLAG_SET_KEY+len+1,dataLen-LEVELDB_KEY_FLAG_SET_KEY-len-1);
    }else if(tmptype == 'z'){
      argc = 4;
      argv = zmalloc(sizeof(robj*)*argc);
      fakeClient->argc = argc;
      fakeClient->argv = argv;
      argv[0] = createStringObject("zadd",4);
      len = data[LEVELDB_KEY_FLAG_SET_KEY_LEN];
      argv[1] = createStringObject(data+LEVELDB_KEY_FLAG_SET_KEY,len);
      argv[3] = createStringObject(data+LEVELDB_KEY_FLAG_SET_KEY+len+1,dataLen-LEVELDB_KEY_FLAG_SET_KEY-len-1);
      value = (char*) leveldb_iter_value(iterator, &valueLen);
      argv[2] = createStringObject(value, valueLen);
    }else{
      redisLog(REDIS_WARNING,"load leveldb no found type: %d %d", dbid, tmptype);
      freeFakeClientArgv(fakeClient);
      success = 0;
      break;
    }
    cmd = lookupCommand(argv[0]->ptr);
    if (!cmd) {
      redisLog(REDIS_WARNING,"Unknown command '%s' from leveldb", (char*)argv[0]->ptr);
      exit(1);
    }
    cmd->proc(fakeClient);

    /* The fake client should not have a reply */
    redisAssert(fakeClient->bufpos == 0 && listLength(fakeClient->reply) == 0);
    /* Clean up. Command code may have changed argv/argc so we use the
     * argv/argc of the client instead of the local variables. */
    freeFakeClientArgv(fakeClient);
  }
  redisLog(REDIS_NOTICE, "load leveldb sum: %lu", loops);

  freeFakeClient(fakeClient);
  stopLoading();
  server.leveldb_state = old_leveldb_state;

  char *err = NULL;
  leveldb_iter_get_error(iterator, &err);
  if(err != NULL) {
    redisLog(REDIS_WARNING, "load leveldb iterator err: %s", err);
    leveldb_free(err);
    err = NULL;
  }

  leveldb_iter_destroy(iterator);
  if(success == 1) {
    return REDIS_OK;
  }
  return REDIS_ERR;
}

void initleveldb(struct leveldb* ldb, char *path) {
  ldb->options = leveldb_options_create();
  leveldb_options_set_create_if_missing(ldb->options, 1);
  leveldb_options_set_compression(ldb->options, 1);
  leveldb_options_set_write_buffer_size(ldb->options, 64 * 1024 * 1024);
  leveldb_options_set_max_open_files(ldb->options, 500);

  char *err = NULL;
  ldb->db = leveldb_open(ldb->options, path, &err);

  if (err != NULL) {
    redisLog(REDIS_WARNING, "open leveldb err: %s", err);
    leveldb_free(err); 
    err = NULL;
    exit(1);
  }

  ldb->roptions = leveldb_readoptions_create();
  leveldb_readoptions_set_fill_cache(ldb->roptions, 0);

  ldb->woptions = leveldb_writeoptions_create();
  leveldb_writeoptions_set_sync(ldb->woptions, 0);
}

void closeleveldb(struct leveldb *ldb) {
  leveldb_writeoptions_destroy(ldb->woptions);
  leveldb_readoptions_destroy(ldb->roptions);
  leveldb_options_destroy(ldb->options);
  leveldb_close(ldb->db);
}

sds createleveldbHashHead(int dbid, sds name) {
  char tmp[LEVELDB_KEY_FLAG_SET_KEY];

  tmp[LEVELDB_KEY_FLAG_DATABASE_ID] = dbid;
  tmp[LEVELDB_KEY_FLAG_TYPE] = 'h';
  tmp[LEVELDB_KEY_FLAG_SET_KEY_LEN] = sdslen(name);

  sds key = sdsnewlen(tmp, LEVELDB_KEY_FLAG_SET_KEY);

  key = sdscatsds(key, name);
  key = sdscat(key, "=");
  return key;
}

void leveldbHset(int dbid, struct leveldb *ldb, robj** argv) {
  leveldbHsetDirect(dbid, ldb, argv[1], argv[2], argv[3]);
}

void leveldbHsetDirect(int dbid, struct leveldb *ldb, robj *argv1, robj *argv2, robj *argv3) {
  if(server.leveldb_state == REDIS_LEVELDB_OFF) {
    return;
  }

  robj *r1 = getDecodedObject(argv1);
  robj *r2 = getDecodedObject(argv2);
  robj *r3 = getDecodedObject(argv3);
  sds key = createleveldbHashHead(dbid, r1->ptr);
  char *err = NULL;

  key = sdscatsds(key, r2->ptr);
  leveldb_put(ldb->db, ldb->woptions, key, sdslen(key), r3->ptr, sdslen(r3->ptr), &err);
  if (err != NULL) {
    redisLog(REDIS_WARNING, "hset direct leveldb err: %s", err);
    leveldb_free(err); 
    err = NULL;
  }
  server.leveldb_op_num++;

  sdsfree(key);
  decrRefCount(r1);
  decrRefCount(r2);
  decrRefCount(r3);
}

void leveldbHmset(int dbid, struct leveldb *ldb, robj** argv, int argc) {
  if(server.leveldb_state == REDIS_LEVELDB_OFF) {
    return;
  }

  robj *r1 = getDecodedObject(argv[1]);
  sds key = createleveldbHashHead(dbid, r1->ptr);
  leveldb_writebatch_t* wb = leveldb_writebatch_create();
  robj **rs = zmalloc(sizeof(robj*)*(argc - 2));
  size_t klen = sdslen(key);
  int i, j = 0;
  char *err = NULL;

  for (i = 2; i < argc; i += 2) {
    rs[j] = getDecodedObject(argv[i]);
    rs[j+1] = getDecodedObject(argv[i+1]);
    key = sdscatsds(key, rs[j]->ptr);
    leveldb_writebatch_put(wb, key, sdslen(key), rs[j+1]->ptr, sdslen(rs[j+1]->ptr));
    sdsrange(key, 0, klen - 1);
    j += 2;
  }
  leveldb_write(ldb->db, ldb->woptions, wb, &err);
  if (err != NULL) {
    redisLog(REDIS_WARNING, "hmset leveldb err: %s", err);
    leveldb_free(err); 
    err = NULL;
  }
  server.leveldb_op_num++;

  sdsfree(key);
  decrRefCount(r1);
  leveldb_writebatch_destroy(wb);
  for(j = 0; j < argc - 2; j++) {
    decrRefCount(rs[j]);
  }
  zfree(rs);
}

void leveldbHdel(int dbid, struct leveldb *ldb, robj** argv, int argc) {
  if(server.leveldb_state == REDIS_LEVELDB_OFF) {
    return;
  }

  robj *r1 = getDecodedObject(argv[1]);
  sds key = createleveldbHashHead(dbid, r1->ptr);
  leveldb_writebatch_t* wb = leveldb_writebatch_create();
  robj **rs = zmalloc(sizeof(robj*)*(argc - 2));
  size_t klen = sdslen(key);
  int i, j = 0;
  char *err = NULL;

  for (i = 2; i < argc; i++) {
    rs[j] = getDecodedObject(argv[i]);
    key = sdscatsds(key, rs[j]->ptr);
    leveldb_writebatch_delete(wb, key, sdslen(key));
    sdsrange(key, 0, klen - 1);
    j++;
  }
  leveldb_write(ldb->db, ldb->woptions, wb, &err);
  if (err != NULL) {
    redisLog(REDIS_WARNING, "hdel leveldb err: %s", err);
    leveldb_free(err); 
    err = NULL;
  }
  server.leveldb_op_num++;

  sdsfree(key);
  decrRefCount(r1);
  leveldb_writebatch_destroy(wb);
  for(j = 0; j < argc - 2; j++) {
    decrRefCount(rs[j]);
  }
  zfree(rs);
}

void leveldbHclear(int dbid, struct leveldb *ldb, robj *argv) {
  if(server.leveldb_state == REDIS_LEVELDB_OFF) {
    return;
  }

  robj *r1 = getDecodedObject(argv);
  sds key = createleveldbHashHead(dbid, r1->ptr);
  leveldb_iterator_t *iterator = leveldb_create_iterator(ldb->db, ldb->roptions);
  char *data = NULL;
  size_t dataLen = 0;
  size_t keyLen = sdslen(r1->ptr);
  int cmp;
  size_t klen = sdslen(key);
  char *err = NULL;

  for(leveldb_iter_seek(iterator, key, klen); leveldb_iter_valid(iterator); leveldb_iter_next(iterator)) {
    data = (char*) leveldb_iter_key(iterator, &dataLen);
    size_t len = data[LEVELDB_KEY_FLAG_SET_KEY_LEN];
    if(len != keyLen) break;
    if(data[LEVELDB_KEY_FLAG_DATABASE_ID] != dbid) break;
    cmp = memcmp(r1->ptr, data + LEVELDB_KEY_FLAG_SET_KEY, len);
    if(cmp != 0) break;
    leveldb_delete(ldb->db, ldb->woptions, data, dataLen, &err);
    if (err != NULL) {
      redisLog(REDIS_WARNING, "hclear leveldb err: %s", err);
      leveldb_free(err); 
      err = NULL;
    }
  }
  server.leveldb_op_num++;

  sdsfree(key);
  decrRefCount(r1);
  leveldb_iter_get_error(iterator, &err);
  if(err != NULL) {
    redisLog(REDIS_WARNING, "hclear leveldb iterator err: %s", err);
    leveldb_free(err);
    err = NULL;
  }
  leveldb_iter_destroy(iterator);
}

sds createleveldbSetHead(int dbid, sds name) {
  char tmp[LEVELDB_KEY_FLAG_SET_KEY];

  tmp[LEVELDB_KEY_FLAG_DATABASE_ID] = dbid;
  tmp[LEVELDB_KEY_FLAG_TYPE] = 's';
  tmp[LEVELDB_KEY_FLAG_SET_KEY_LEN] = sdslen(name);

  sds key = sdsnewlen(tmp, LEVELDB_KEY_FLAG_SET_KEY);

  key = sdscatsds(key, name);
  key = sdscat(key, "=");
  return key;
}

void leveldbSadd(int dbid, struct leveldb *ldb, robj** argv, int argc) {
  if(server.leveldb_state == REDIS_LEVELDB_OFF) {
    return;
  }

  robj *r1 = getDecodedObject(argv[1]);
  sds key = createleveldbSetHead(dbid, r1->ptr);
  leveldb_writebatch_t* wb = leveldb_writebatch_create();
  robj **rs = zmalloc(sizeof(robj*)*(argc - 2));
  size_t klen = sdslen(key);
  int i, j = 0;
  char *err = NULL;

  for (i = 2; i < argc; i++) {
    rs[j] = getDecodedObject(argv[i]);
    key = sdscatsds(key, rs[j]->ptr);
    leveldb_writebatch_put(wb, key, sdslen(key), NULL, 0);
    sdsrange(key, 0, klen - 1);
    j++;
  }
  leveldb_write(ldb->db, ldb->woptions, wb, &err);
  if (err != NULL) {
    redisLog(REDIS_WARNING, "sadd leveldb err: %s", err);
    leveldb_free(err); 
    err = NULL;
  }
  server.leveldb_op_num++;

  sdsfree(key);
  decrRefCount(r1);
  leveldb_writebatch_destroy(wb);
  for(j = 0; j < argc - 2; j++) {
    decrRefCount(rs[j]);
  }
  zfree(rs);
}

void leveldbSrem(int dbid, struct leveldb *ldb, robj** argv, int argc) {
  if(server.leveldb_state == REDIS_LEVELDB_OFF) {
    return;
  }

  robj *r1 = getDecodedObject(argv[1]);
  sds key = createleveldbSetHead(dbid, r1->ptr);
  leveldb_writebatch_t* wb = leveldb_writebatch_create();
  robj **rs = zmalloc(sizeof(robj*)*(argc - 2));
  size_t klen = sdslen(key);
  int i, j = 0;
  char *err = NULL;

  for (i = 2; i < argc; i++ ) {
    rs[j] = getDecodedObject(argv[i]);
    key = sdscatsds(key, rs[j]->ptr);
    leveldb_writebatch_delete(wb, key, sdslen(key));
    sdsrange(key, 0, klen - 1);
    j++;
  }
  leveldb_write(ldb->db, ldb->woptions, wb, &err);
  if (err != NULL) {
    redisLog(REDIS_WARNING, "srem leveldb err: %s", err);
    leveldb_free(err); 
    err = NULL;
  }
  server.leveldb_op_num++;

  sdsfree(key);
  decrRefCount(r1);
  leveldb_writebatch_destroy(wb);
  for(j = 0; j < argc - 2; j++) {
    decrRefCount(rs[j]);
  }
  zfree(rs);
}

void leveldbSclear(int dbid, struct leveldb *ldb, robj* argv) {
  if(server.leveldb_state == REDIS_LEVELDB_OFF) {
    return;
  }

  robj *r1 = getDecodedObject(argv);
  sds key = createleveldbSetHead(dbid, r1->ptr);
  leveldb_iterator_t *iterator = leveldb_create_iterator(ldb->db, ldb->roptions);
  char *data = NULL;
  size_t dataLen = 0;
  size_t keyLen = sdslen(r1->ptr);
  int cmp;
  size_t klen = sdslen(key);
  char *err = NULL;

  for(leveldb_iter_seek(iterator, key, klen); leveldb_iter_valid(iterator); leveldb_iter_next(iterator)) {
    data = (char*) leveldb_iter_key(iterator, &dataLen);
    size_t len = data[LEVELDB_KEY_FLAG_SET_KEY_LEN];
    if(len != keyLen) break;
    if(data[LEVELDB_KEY_FLAG_DATABASE_ID] != dbid) break;
    cmp = memcmp(r1->ptr, data + LEVELDB_KEY_FLAG_SET_KEY, len);
    if(cmp != 0) break;
    leveldb_delete(ldb->db, ldb->woptions, data, dataLen, &err);
    if (err != NULL) {
      redisLog(REDIS_WARNING, "sclear leveldb err: %s", err);
      leveldb_free(err); 
      err = NULL;
    }
  }
  server.leveldb_op_num++;

  sdsfree(key);
  decrRefCount(r1);
  leveldb_iter_get_error(iterator, &err);
  if(err != NULL) {
    redisLog(REDIS_WARNING, "sclear leveldb iterator err: %s", err);
    leveldb_free(err);
    err = NULL;
  }
  leveldb_iter_destroy(iterator);
}

sds createleveldbSortedSetHead(int dbid, sds name) {
  char tmp[LEVELDB_KEY_FLAG_SET_KEY];

  tmp[LEVELDB_KEY_FLAG_DATABASE_ID] = dbid;
  tmp[LEVELDB_KEY_FLAG_TYPE] = 'z';
  tmp[LEVELDB_KEY_FLAG_SET_KEY_LEN] = sdslen(name);

  sds key = sdsnewlen(tmp, LEVELDB_KEY_FLAG_SET_KEY);

  key = sdscatsds(key, name);
  key = sdscat(key, "=");
  return key;
}

void leveldbZadd(int dbid, struct leveldb *ldb, robj** argv, int argc) {
  if(server.leveldb_state == REDIS_LEVELDB_OFF) {
    return;
  }

  robj *r1 = getDecodedObject(argv[1]);
  sds key = createleveldbSortedSetHead(dbid, r1->ptr);
  leveldb_writebatch_t* wb = leveldb_writebatch_create();
  robj **rs = zmalloc(sizeof(robj*)*(argc - 2));
  size_t klen = sdslen(key);
  int i, j = 0;
  char *err = NULL;

  for (i = 2; i < argc; i += 2) {
    rs[j] = getDecodedObject(argv[i]);
    rs[j+1] = getDecodedObject(argv[i+1]);
    key = sdscatsds(key, rs[j+1]->ptr);
    leveldb_writebatch_put(wb, key, sdslen(key), rs[j]->ptr, sdslen(rs[j]->ptr));
    sdsrange(key, 0, klen - 1);
    j += 2;
  }
  leveldb_write(ldb->db, ldb->woptions, wb, &err);
  if (err != NULL) {
    redisLog(REDIS_WARNING, "zadd leveldb err: %s", err);
    leveldb_free(err); 
    err = NULL;
  }
  server.leveldb_op_num++;

  sdsfree(key);
  decrRefCount(r1);
  leveldb_writebatch_destroy(wb);
  for(j = 0; j < argc - 2; j++) {
    decrRefCount(rs[j]);
  }
  zfree(rs);
}

void leveldbZaddDirect(int dbid, struct leveldb *ldb, robj* argv1, robj* argv2, double score) {
  if(server.leveldb_state == REDIS_LEVELDB_OFF) {
    return;
  }

  robj *r1 = getDecodedObject(argv1);
  robj *r2 = getDecodedObject(argv2);
  sds key = createleveldbSortedSetHead(dbid, r1->ptr);
  key = sdscatsds(key, r2->ptr);
  size_t klen = sdslen(key);
  char *err = NULL;
  char buf[128];
  int len = snprintf(buf,sizeof(buf),"%.17g",score);

  leveldb_put(ldb->db, ldb->woptions, key, klen, buf, len, &err);
  if (err != NULL) {
    redisLog(REDIS_WARNING, "zadd direct leveldb err: %s", err);
    leveldb_free(err); 
    err = NULL;
  }
  server.leveldb_op_num++;

  sdsfree(key);
  decrRefCount(r1);
  decrRefCount(r2);
}

void leveldbZrem(int dbid, struct leveldb *ldb, robj** argv, int argc) {
  if(server.leveldb_state == REDIS_LEVELDB_OFF) {
    return;
  }

  robj *r1 = getDecodedObject(argv[1]);
  sds key = createleveldbSortedSetHead(dbid, r1->ptr);
  leveldb_writebatch_t* wb = leveldb_writebatch_create();
  robj **rs = zmalloc(sizeof(robj*)*(argc - 2));
  size_t klen = sdslen(key);
  int i, j = 0;
  char *err = NULL;

  for (i = 2; i < argc; i++ ) {
    rs[j] = getDecodedObject(argv[i]);
    key = sdscatsds(key, rs[j]->ptr);
    leveldb_writebatch_delete(wb, key, sdslen(key));
    sdsrange(key, 0, klen - 1);
    j++;
  }
  leveldb_write(ldb->db, ldb->woptions, wb, &err);
  if (err != NULL) {
    redisLog(REDIS_WARNING, "zrem leveldb err: %s", err);
    leveldb_free(err); 
    err = NULL;
  }
  server.leveldb_op_num++;

  sdsfree(key);
  decrRefCount(r1);
  leveldb_writebatch_destroy(wb);
  for(j = 0; j < argc - 2; j++) {
    decrRefCount(rs[j]);
  }
  zfree(rs);
}

void leveldbZremByLongLong(int dbid, struct leveldb *ldb, robj *arg, long long vlong) {
  if(server.leveldb_state == REDIS_LEVELDB_OFF) {
    return;
  }

  robj *r1 = getDecodedObject(arg);
  sds key = createleveldbSortedSetHead(dbid, r1->ptr);
  char *err = NULL;
  char buf[64];
  int len;

  len = ll2string(buf,64,vlong);
  key = sdscatlen(key, buf, len);
  //redisLog(REDIS_NOTICE, "leveldbZremByLongLong %s", key);
  leveldb_delete(ldb->db, ldb->woptions, key, sdslen(key), &err);
  if (err != NULL) {
    redisLog(REDIS_WARNING, "zrem by long long leveldb err: %s", err);
    leveldb_free(err); 
    err = NULL;
  }
  server.leveldb_op_num++;

  sdsfree(key);
  decrRefCount(r1);
}

void leveldbZremByCBuffer(int dbid, struct leveldb *ldb, robj *arg, unsigned char *vstr, unsigned int vlen) {
  if(server.leveldb_state == REDIS_LEVELDB_OFF) {
    return;
  }

  robj *r1 = getDecodedObject(arg);
  sds key = createleveldbSortedSetHead(dbid, r1->ptr);
  char *err = NULL;

  key = sdscatlen(key, vstr, vlen);
  //redisLog(REDIS_NOTICE, "leveldbZremByCBuffer %s", key);
  leveldb_delete(ldb->db, ldb->woptions, key, sdslen(key), &err);
  if (err != NULL) {
    redisLog(REDIS_WARNING, "zrem by c buffer leveldb err: %s", err);
    leveldb_free(err); 
    err = NULL;
  }
  server.leveldb_op_num++;

  sdsfree(key);
  decrRefCount(r1);
}

void leveldbZremByObject(int dbid, struct leveldb *ldb, robj *arg, robj *field) {
  if(server.leveldb_state == REDIS_LEVELDB_OFF) {
    return;
  }

  robj *r1 = getDecodedObject(arg);
  robj *r2 = getDecodedObject(field);
  sds key = createleveldbSortedSetHead(dbid, r1->ptr);
  char *err = NULL;

  key = sdscatsds(key,  r2->ptr);
  //redisLog(REDIS_NOTICE, "leveldbZremByObject %s", key);
  leveldb_delete(ldb->db, ldb->woptions, key, sdslen(key), &err);
  if (err != NULL) {
    redisLog(REDIS_WARNING, "zrem by object leveldb err: %s", err);
    leveldb_free(err); 
    err = NULL;
  }
  server.leveldb_op_num++;

  sdsfree(key);
  decrRefCount(r1);
  decrRefCount(r2);
}

void leveldbZclear(int dbid, struct leveldb *ldb, robj* argv) {
  if(server.leveldb_state == REDIS_LEVELDB_OFF) {
    return;
  }

  robj *r1 = getDecodedObject(argv);
  sds key = createleveldbSortedSetHead(dbid, r1->ptr);
  char *data = NULL;
  size_t dataLen = 0;
  size_t keyLen = sdslen(r1->ptr);
  int cmp;
  size_t klen = sdslen(key);
  char *err = NULL;

  leveldb_iterator_t *iterator = leveldb_create_iterator(ldb->db, ldb->roptions);
  for(leveldb_iter_seek(iterator, key, klen); leveldb_iter_valid(iterator); leveldb_iter_next(iterator)) {
    data = (char*) leveldb_iter_key(iterator, &dataLen);
    size_t len = data[LEVELDB_KEY_FLAG_SET_KEY_LEN];
    if(len != keyLen) break;
    if(data[LEVELDB_KEY_FLAG_DATABASE_ID] != dbid) break;
    cmp = memcmp(r1->ptr, data + LEVELDB_KEY_FLAG_SET_KEY, len);
    if(cmp != 0) break;
    leveldb_delete(ldb->db, ldb->woptions, data, dataLen, &err);
    if (err != NULL) {
      redisLog(REDIS_WARNING, "zclear leveldb err: %s", err);
      leveldb_free(err); 
      err = NULL;
    }
  }
  server.leveldb_op_num++;

  sdsfree(key);
  decrRefCount(r1);
  leveldb_iter_get_error(iterator, &err);
  if(err != NULL) {
    redisLog(REDIS_WARNING, "zclear leveldb iterator err: %s", err);
    leveldb_free(err);
    err = NULL;
  }
  leveldb_iter_destroy(iterator);
}

void leveldbFlushdb(int dbid, struct leveldb* ldb) {
  char tmp[1];
  char *err = NULL;
  char *data = NULL;
  size_t dataLen = 0;
  leveldb_iterator_t *iterator = leveldb_create_iterator(ldb->db, ldb->roptions);

  tmp[LEVELDB_KEY_FLAG_DATABASE_ID] = dbid;
  for(leveldb_iter_seek(iterator, tmp, 1); leveldb_iter_valid(iterator); leveldb_iter_next(iterator)) {
    data = (char*) leveldb_iter_key(iterator, &dataLen);
    if(data[LEVELDB_KEY_FLAG_DATABASE_ID] != dbid) break;
    leveldb_delete(ldb->db, ldb->woptions, data, dataLen, &err);
    if (err != NULL) {
      redisLog(REDIS_WARNING, "flushdb leveldb err: %s", err);
      leveldb_free(err); 
      err = NULL;
    }
  }
  server.leveldb_op_num++;

  leveldb_iter_get_error(iterator, &err);
  if(err != NULL) {
    redisLog(REDIS_WARNING, "flushdb leveldb iterator err: %s", err);
    leveldb_free(err);
    err = NULL;
  }
  leveldb_iter_destroy(iterator);
}

void leveldbFlushall(struct leveldb* ldb) {
  char *err = NULL;
  char *data = NULL;
  size_t dataLen = 0;
  leveldb_iterator_t *iterator = leveldb_create_iterator(ldb->db, ldb->roptions);

  for(leveldb_iter_seek_to_first(iterator); leveldb_iter_valid(iterator); leveldb_iter_next(iterator)) {
    data = (char*) leveldb_iter_key(iterator, &dataLen);
    leveldb_delete(ldb->db, ldb->woptions, data, dataLen, &err);
    if (err != NULL) {
      redisLog(REDIS_WARNING, "flushall leveldb err: %s", err);
      leveldb_free(err); 
      err = NULL;
    }
  }
  server.leveldb_op_num++;

  leveldb_iter_get_error(iterator, &err);
  if(err != NULL) {
    redisLog(REDIS_WARNING, "flushall leveldb iterator err: %s", err);
    leveldb_free(err);
    err = NULL;
  }
  leveldb_iter_destroy(iterator);
}

void *leveldbBackup(void *arg) {
  char *path = (char *)arg;
  time_t backup_start = time(NULL);
  leveldb_options_t *options = leveldb_options_create();
  int success = 0;

  leveldb_options_set_create_if_missing(options, 1);
  leveldb_options_set_error_if_exists(options, 1);
  leveldb_options_set_compression(options, 1);
  leveldb_options_set_write_buffer_size(options, 64 * 1024 * 1024);

  char *err = NULL;
  leveldb_t *db = leveldb_open(options, path, &err);

  if (err != NULL) {
    redisLog(REDIS_WARNING, "open leveldb err: %s", err);
    leveldb_free(err); 
    err = NULL;
    goto cleanup;
  }

  char *data = NULL;
  size_t dataLen = 0;
  char *value = NULL;
  size_t valueLen = 0;
  int i = 0;
  leveldb_writebatch_t* wb = leveldb_writebatch_create();
  leveldb_writeoptions_t *woptions = leveldb_writeoptions_create();
  leveldb_iterator_t *iterator = leveldb_create_iterator(server.ldb.db, server.ldb.roptions);

  leveldb_writeoptions_set_sync(woptions, 0);
  for(leveldb_iter_seek_to_first(iterator); leveldb_iter_valid(iterator); leveldb_iter_next(iterator)) {
    data = (char*) leveldb_iter_key(iterator, &dataLen);
    value = (char*) leveldb_iter_value(iterator, &valueLen);
    leveldb_writebatch_put(wb, data, dataLen, value, valueLen);
    i++;
    if(i == 1000) {
      i = 0;
      leveldb_write(db, woptions, wb, &err);
      if (err != NULL) {
        redisLog(REDIS_WARNING, "backup write leveldb err: %s", err);
        leveldb_free(err); 
        err = NULL;
        goto closehandler;
      }
      leveldb_writebatch_destroy(wb);
      wb = leveldb_writebatch_create();
    }
  }
  leveldb_write(db, woptions, wb, &err);
  if (err != NULL) {
    redisLog(REDIS_WARNING, "backup write leveldb err: %s", err);
    leveldb_free(err); 
    err = NULL;
    goto closehandler;
  }
  leveldb_writebatch_destroy(wb);
  leveldb_iter_get_error(iterator, &err);
  if(err != NULL) {
    redisLog(REDIS_WARNING, "backup leveldb iterator err: %s", err);
    leveldb_free(err);
    err = NULL;
    goto closehandler;
  }
  success = 1;
closehandler:
  leveldb_iter_destroy(iterator);
  leveldb_writeoptions_destroy(woptions);
  leveldb_close(db);
cleanup:
  leveldb_options_destroy(options);
  if( success == 1) {
    time_t backup_end = time(NULL);
    char info[1024];
    char tmpfile[512];
    char backupfile[512];
    snprintf(tmpfile,512,"%s/temp.log", path);
    snprintf(backupfile,512,"%s/BACKUP.log", path);
    FILE *fp = fopen(tmpfile,"w");

    if (!fp) {
      redisLog(REDIS_WARNING, "Failed opening .log for saving: %s",
          strerror(errno));
    }else{
      int infolen = snprintf(info, sizeof(info), "BACKUP\n\tSTART:\t%jd\n\tEND:\t\t%jd\n\tCOST:\t\t%jd\nSUCCESS", backup_start, backup_end, backup_end-backup_start);

      fwrite(info, infolen, 1, fp);
      fclose(fp);
      if (rename(tmpfile,backupfile) == -1) {
        redisLog(REDIS_WARNING,"Error moving temp backup file on the final destination: %s", strerror(errno));
      }
      unlink(tmpfile);
      redisLog(REDIS_NOTICE, "backup leveldb path: %s", path);
    }
  }
  zfree(path);
  return (void *)NULL;
}

void backupCommand(redisClient *c) {
  if(server.leveldb_state == REDIS_LEVELDB_OFF) {
    addReplyError(c,"leveldb off");
    return;
  }
  size_t len = sdslen(c->argv[1]->ptr);
  char *path = zmalloc(len + 1);
  pthread_t tid;

  memcpy(path, c->argv[1]->ptr, len);
  path[len] = '\0';
  int err = pthread_create(&tid, NULL, &leveldbBackup, path);
  if(err != 0){ 
    redisLog(REDIS_WARNING, "backup leveldb thread err: %d", err);
  }
  addReplyStatus(c,"backup leveldb started");
}
