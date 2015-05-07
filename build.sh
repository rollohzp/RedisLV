#!/bin/sh

SNAPPY="snappy-1.1.1"
LEVELDB="leveldb-1.18"

cd deps/ && tar -zxf ${SNAPPY}.tar.gz && cd ${SNAPPY} 

if [ ! -f Makefile ]; then
  ./configure --disable-shared --with-pic && make || exit 1
fi
SNAPPY_PATH=`pwd`

export LIBRARY_PATH=${SNAPPY_PATH}/.libs/
export C_INCLUDE_PATH=${SNAPPY_PATH}
export CPLUS_INCLUDE_PATH=${SNAPPY_PATH}

cd ../

tar -zxf ${LEVELDB}.tar.gz 

cd ${LEVELDB} &&  make || exit 1

LEVELDB_PATH=`pwd`

cd ../../

rm -f build_config.mk
echo "LEVELDB_INCLUDE=${LEVELDB_PATH}/include" > build_config.mk
echo "LEVELDB_LIB=${LEVELDB_PATH}/libleveldb.a" >> build_config.mk
echo "SNAPPY_LIB=${SNAPPY_PATH}/.libs/libsnappy.a" >> build_config.mk
