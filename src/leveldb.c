#include "redis.h"

#include <pthread.h>

int loadleveldb(char *path) {
  redisLog(REDIS_NOTICE, "load leveldb path: %s", path);

  struct redisClient *fakeClient;
  int old_leveldb_state = server.leveldb_state;
  long loops = 0;

  server.leveldb_state = REDIS_LEVELDB_OFF;

  initleveldb(&server.ldb, path);

  leveldb_iterator_t *iterator = leveldb_create_iterator(server.ldb.db, server.ldb.roptions);

  startLoading(NULL);
  fakeClient = createFakeClient();

  char *data = NULL;
  char *value = NULL;
  size_t dataLen = 0;
  size_t valueLen = 0;

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
    if(data[0] == 'h'){
      argc = 4;
      argv = zmalloc(sizeof(robj*)*argc);
      fakeClient->argc = argc;
      fakeClient->argv = argv;
      argv[0] = createStringObject("hset",4);
      len = data[1];
      argv[1] = createStringObject(data+2,len);
      argv[2] = createStringObject(data+3+len,dataLen-3-len);
      value = (char*) leveldb_iter_value(iterator, &valueLen);
      argv[3] = createStringObject(value, valueLen);
    }else if(data[0] == 's'){
      argc = 3;
      argv = zmalloc(sizeof(robj*)*argc);
      fakeClient->argc = argc;
      fakeClient->argv = argv;
      argv[0] = createStringObject("sadd",4);
      len = data[1];
      argv[1] = createStringObject(data+2,len);
      argv[2] = createStringObject(data+3+len,dataLen-3-len);
    }else if(data[0] == 'z'){
      argc = 4;
      argv = zmalloc(sizeof(robj*)*argc);
      fakeClient->argc = argc;
      fakeClient->argv = argv;
      argv[0] = createStringObject("zadd",4);
      len = data[1];
      argv[1] = createStringObject(data+2,len);
      argv[3] = createStringObject(data+3+len,dataLen-3-len);
      value = (char*) leveldb_iter_value(iterator, &valueLen);
      argv[2] = createStringObject(value, valueLen);
    }else{
      redisLog(REDIS_WARNING,"load leveldb no found type: %d", data[0]);
      continue;
    }
    /* Command lookup */
    cmd = lookupCommand(argv[0]->ptr);
    if (!cmd) {
      redisLog(REDIS_WARNING,"Unknown command '%s' from leveldb", (char*)argv[0]->ptr);
      exit(1);
    }
    /* Run the command in the context of a fake client */
    cmd->proc(fakeClient);

    /* The fake client should not have a reply */
    redisAssert(fakeClient->bufpos == 0 && listLength(fakeClient->reply) == 0);
    /* The fake client should never get blocked */
    redisAssert((fakeClient->flags & REDIS_BLOCKED) == 0);

    /* Clean up. Command code may have changed argv/argc so we use the
     * argv/argc of the client instead of the local variables. */
    freeFakeClientArgv(fakeClient);
  }
  redisLog(REDIS_NOTICE, "load leveldb sum: %lu", loops);

  freeFakeClient(fakeClient);
  stopLoading();
  server.leveldb_state = old_leveldb_state;

  leveldb_iter_destroy(iterator);

  return REDIS_OK;
}

void initleveldb(struct leveldb* ldb, char *path) {
  ldb->options = leveldb_options_create();
  leveldb_options_set_create_if_missing(ldb->options, 1);
  leveldb_options_set_compression(ldb->options, 1);
  leveldb_options_set_write_buffer_size(ldb->options, 64 * 1024 * 1024);
  leveldb_options_set_block_size(ldb->options, 4 * 1024);
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
  ldb->woptions = leveldb_writeoptions_create();
  leveldb_writeoptions_set_sync(ldb->woptions, 0);
}

void closeleveldb(struct leveldb *ldb) {
  leveldb_writeoptions_destroy(ldb->woptions);
  leveldb_readoptions_destroy(ldb->roptions);
  leveldb_options_destroy(ldb->options);
  leveldb_close(ldb->db);
}

sds createleveldbHashHead(sds name) {
  char tmp[2];
  tmp[0] = sdslen(name);
  tmp[1] = '\0';

  sds key = sdsnewlen("h",1);
  key = sdscat(key, tmp);
  key = sdscatsds(key, name);
  tmp[0] = '=';
  key = sdscat(key, tmp);
  return key;
}

void leveldbHset(struct leveldb *ldb, robj *argv1, robj *argv2, robj *argv3) {
  if(server.leveldb_state == REDIS_LEVELDB_OFF) {
    return;
  }
  sds key = createleveldbHashHead(argv1->ptr);
  key = sdscatsds(key, argv2->ptr);
  size_t klen = sdslen(key);

  char *err = NULL;
  
  leveldb_put(ldb->db, ldb->woptions, key, klen, argv3->ptr, sdslen(argv3->ptr), &err);

  if (err != NULL) {
    redisLog(REDIS_WARNING, "hset leveldb err: %s", err);
    leveldb_free(err); 
  }

  sdsfree(key);
}

void leveldbHmset(struct leveldb *ldb, robj** argv, int argc) {
  if(server.leveldb_state == REDIS_LEVELDB_OFF) {
    return;
  }
  sds key = createleveldbHashHead(argv[1]->ptr);
  size_t klen = sdslen(key);

  leveldb_writebatch_t* wb = leveldb_writebatch_create();
  int i;
  for (i = 2; i < argc; i += 2) {
    key = sdscatsds(key, argv[i]->ptr);
    leveldb_writebatch_put(wb, key, sdslen(key), argv[i+1]->ptr, sdslen(argv[i+1]->ptr));
    sdsrange(key, 0, klen - 1);
  }

  char *err = NULL;
  leveldb_write(ldb->db, ldb->woptions, wb, &err);
  if (err != NULL) {
    redisLog(REDIS_WARNING, "hmset leveldb err: %s", err);
    leveldb_free(err); 
  }

  leveldb_writebatch_destroy(wb);

  sdsfree(key);
}

void leveldbHdel(struct leveldb *ldb, robj** argv, int argc) {
  if(server.leveldb_state == REDIS_LEVELDB_OFF) {
    return;
  }
  sds key = createleveldbHashHead(argv[1]->ptr);
  size_t klen = sdslen(key);

  leveldb_writebatch_t* wb = leveldb_writebatch_create();
  int i;
  for (i = 2; i < argc; i++ ) {
    key = sdscatsds(key, argv[i]->ptr);
    leveldb_writebatch_delete(wb, key, sdslen(key));
    sdsrange(key, 0, klen - 1);
  }

  char *err = NULL;
  leveldb_write(ldb->db, ldb->woptions, wb, &err);
  if (err != NULL) {
    redisLog(REDIS_WARNING, "hdel leveldb err: %s", err);
    leveldb_free(err); 
  }

  leveldb_writebatch_destroy(wb);

  sdsfree(key);
}

void leveldbHclear(struct leveldb *ldb, robj* argv) {
  if(server.leveldb_state == REDIS_LEVELDB_OFF) {
    return;
  }
  char *data = NULL;
  size_t dataLen = 0;
  size_t keyLen = sdslen(argv->ptr);
  int cmp;

  sds key = createleveldbHashHead(argv->ptr);
  size_t klen = sdslen(key);
  leveldb_iterator_t *iterator = leveldb_create_iterator(ldb->db, ldb->roptions);

  for(leveldb_iter_seek(iterator, key, klen); leveldb_iter_valid(iterator); leveldb_iter_next(iterator)) {
    data = (char*) leveldb_iter_key(iterator, &dataLen);
    size_t len = data[1];
    if(len != keyLen) break;
    cmp = memcmp(argv->ptr, data + 2, len);
    if(cmp != 0) break;
    char *err = NULL;
    leveldb_delete(ldb->db, ldb->woptions, data, dataLen, &err);
    if (err != NULL) {
      redisLog(REDIS_WARNING, "hclear leveldb err: %s", err);
      leveldb_free(err); 
      err = NULL;
    }
  }
  leveldb_iter_destroy(iterator);
  sdsfree(key);
}

sds createleveldbSetHead(sds name) {
  char tmp[2];
  tmp[0] = sdslen(name);
  tmp[1] = '\0';

  sds key = sdsnewlen("s",1);
  key = sdscat(key, tmp);
  key = sdscatsds(key, name);
  tmp[0] = '=';
  key = sdscat(key, tmp);
  return key;
}

void leveldbSadd(struct leveldb *ldb, robj** argv, int argc) {
  if(server.leveldb_state == REDIS_LEVELDB_OFF) {
    return;
  }
  sds key = createleveldbSetHead(argv[1]->ptr);
  size_t klen = sdslen(key);

  leveldb_writebatch_t* wb = leveldb_writebatch_create();
  int i;
  for (i = 2; i < argc; i++) {
    key = sdscatsds(key, argv[i]->ptr);
    leveldb_writebatch_put(wb, key, sdslen(key), NULL, 0);
    sdsrange(key, 0, klen - 1);
  }

  char *err = NULL;
  leveldb_write(ldb->db, ldb->woptions, wb, &err);
  if (err != NULL) {
    redisLog(REDIS_WARNING, "sadd leveldb err: %s", err);
    leveldb_free(err); 
  }

  leveldb_writebatch_destroy(wb);

  sdsfree(key);
}

void leveldbSrem(struct leveldb *ldb, robj** argv, int argc) {
  if(server.leveldb_state == REDIS_LEVELDB_OFF) {
    return;
  }
  sds key = createleveldbSetHead(argv[1]->ptr);
  size_t hlen = sdslen(key);

  leveldb_writebatch_t* wb = leveldb_writebatch_create();
  int i;
  for (i = 2; i < argc; i++ ) {
    key = sdscatsds(key, argv[i]->ptr);
    leveldb_writebatch_delete(wb, key, sdslen(key));
    sdsrange(key, 0, hlen - 1);
  }

  char *err = NULL;
  leveldb_write(ldb->db, ldb->woptions, wb, &err);
  if (err != NULL) {
    redisLog(REDIS_WARNING, "srem leveldb err: %s", err);
    leveldb_free(err); 
  }

  leveldb_writebatch_destroy(wb);

  sdsfree(key);
}

void leveldbSclear(struct leveldb *ldb, robj* argv) {
  if(server.leveldb_state == REDIS_LEVELDB_OFF) {
    return;
  }
  char *data = NULL;
  size_t dataLen = 0;
  size_t keyLen = sdslen(argv->ptr);
  int cmp;

  sds key = createleveldbSetHead(argv->ptr);
  size_t klen = sdslen(key);

  leveldb_iterator_t *iterator = leveldb_create_iterator(ldb->db, ldb->roptions);

  for(leveldb_iter_seek(iterator, key, klen); leveldb_iter_valid(iterator); leveldb_iter_next(iterator)) {
    data = (char*) leveldb_iter_key(iterator, &dataLen);
    size_t len = data[1];
    if(len != keyLen) break;
    cmp = memcmp(argv->ptr, data + 2, len);
    if(cmp != 0) break;
    char *err = NULL;
    leveldb_delete(ldb->db, ldb->woptions, data, dataLen, &err);
    if (err != NULL) {
      redisLog(REDIS_WARNING, "hclear leveldb err: %s", err);
      leveldb_free(err); 
      err = NULL;
    }
  }
  leveldb_iter_destroy(iterator);
  sdsfree(key);
}

sds createleveldbSortedSetHead(sds name) {
  char tmp[2];
  tmp[0] = sdslen(name);
  tmp[1] = '\0';

  sds key = sdsnewlen("z",1);
  key = sdscat(key, tmp);
  key = sdscatsds(key, name);
  tmp[0] = '=';
  key = sdscat(key, tmp);
  return key;
}

void leveldbZadd(struct leveldb *ldb, sds mkey, sds field, double score) {
  if(server.leveldb_state == REDIS_LEVELDB_OFF) {
    return;
  }
  sds key = createleveldbSortedSetHead(mkey);
  key = sdscatsds(key, field);
  size_t klen = sdslen(key);

  char *err = NULL;

  char buf[128];
  int len = snprintf(buf,sizeof(buf),"%.17g",score);
  
  leveldb_put(ldb->db, ldb->woptions, key, klen, buf, len, &err);

  if (err != NULL) {
    redisLog(REDIS_WARNING, "hset leveldb err: %s", err);
    leveldb_free(err); 
  }

  sdsfree(key);
}

void leveldbZrem(struct leveldb *ldb, robj** argv, int argc) {
  if(server.leveldb_state == REDIS_LEVELDB_OFF) {
    return;
  }
  sds key = createleveldbSortedSetHead(argv[1]->ptr);
  size_t hlen = sdslen(key);

  leveldb_writebatch_t* wb = leveldb_writebatch_create();
  int i;
  for (i = 2; i < argc; i++ ) {
    key = sdscatsds(key, argv[i]->ptr);
    leveldb_writebatch_delete(wb, key, sdslen(key));
    sdsrange(key, 0, hlen - 1);
  }

  char *err = NULL;
  leveldb_write(ldb->db, ldb->woptions, wb, &err);
  if (err != NULL) {
    redisLog(REDIS_WARNING, "zrem leveldb err: %s", err);
    leveldb_free(err); 
  }

  leveldb_writebatch_destroy(wb);

  sdsfree(key);
}

void leveldbZclear(struct leveldb *ldb, robj* argv) {
  if(server.leveldb_state == REDIS_LEVELDB_OFF) {
    return;
  }
  char *data = NULL;
  size_t dataLen = 0;
  size_t keyLen = sdslen(argv->ptr);
  int cmp;

  sds key = createleveldbSortedSetHead(argv->ptr);
  size_t klen = sdslen(key);

  leveldb_iterator_t *iterator = leveldb_create_iterator(ldb->db, ldb->roptions);

  for(leveldb_iter_seek(iterator, key, klen); leveldb_iter_valid(iterator); leveldb_iter_next(iterator)) {
    data = (char*) leveldb_iter_key(iterator, &dataLen);
    size_t len = data[1];
    if(len != keyLen) break;
    cmp = memcmp(argv->ptr, data + 2, len);
    if(cmp != 0) break;
    char *err = NULL;
    leveldb_delete(ldb->db, ldb->woptions, data, dataLen, &err);
    if (err != NULL) {
      redisLog(REDIS_WARNING, "zclear leveldb err: %s", err);
      leveldb_free(err); 
      err = NULL;
    }
  }
  leveldb_iter_destroy(iterator);
  sdsfree(key);
}

void *leveldbBackup(void *arg) {
  char *path = (char *)arg;
  leveldb_options_t *options = leveldb_options_create();
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
    goto clean;
  }

  leveldb_writeoptions_t *woptions = leveldb_writeoptions_create();
  leveldb_writeoptions_set_sync(woptions, 0);

  char *data = NULL;
  size_t dataLen = 0;
  char *value = NULL;
  size_t valueLen = 0;

  int i = 0;
  leveldb_writebatch_t* wb = leveldb_writebatch_create();
  leveldb_iterator_t *iterator = leveldb_create_iterator(server.ldb.db, server.ldb.roptions);
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
      }
      leveldb_writebatch_destroy(wb);
      wb = leveldb_writebatch_create();
    }
  }
  leveldb_write(db, woptions, wb, &err);
  if (err != NULL) {
    redisLog(REDIS_WARNING, "backup write leveldb err: %s", err);
    leveldb_free(err); 
  }
  leveldb_writebatch_destroy(wb);
  leveldb_iter_destroy(iterator);
  leveldb_writeoptions_destroy(woptions);
  leveldb_close(db);
  redisLog(REDIS_NOTICE, "backup leveldb path: %s", path);

clean:
  leveldb_options_destroy(options);
  zfree(path);
  return (void *)NULL;
}

void backupCommand(redisClient *c) {
  size_t len = sdslen(c->argv[1]->ptr);
  char *path = zmalloc(len + 1);
  memcpy(path, c->argv[1]->ptr, len);
  path[len] = '\0';
  pthread_t tid;
  int err = pthread_create(&tid, NULL, &leveldbBackup, path);
  if(err != 0){ 
    redisLog(REDIS_WARNING, "backup leveldb thread err: %d", err);
  }
  addReplyStatus(c,"backup leveldb started");
}