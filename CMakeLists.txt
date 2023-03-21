cmake_minimum_required(VERSION 3.10)
project(My_YCSB)

set(CMAKE_CXX_STANDARD 17)

find_package(yaml-cpp REQUIRED)
include_directories(${YAML_CPP_INCLUDE_DIR})
# Debug mode
# set(CMAKE_BUILD_TYPE Debug)
# set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -O0")

include_directories(/usr/local/include)
include_directories(core/include)

set(CoreSource core/client.cpp
               core/measurement.cpp
               core/worker.cpp
               core/workload.cpp)

set(WiredTigerClientSource wiredtiger/wt_client.cpp)
add_executable(init_wt ${CoreSource} ${WiredTigerClientSource} wiredtiger/init_wt.cpp)
add_executable(run_wt ${CoreSource} ${WiredTigerClientSource} wiredtiger/run_wt.cpp)
target_link_libraries(init_wt /usr/local/lib/libwiredtiger.so pthread ${YAML_CPP_LIBRARIES})
target_link_libraries(run_wt /usr/local/lib/libwiredtiger.so pthread ${YAML_CPP_LIBRARIES})

# Add the same for RocksDB
set(RocksDBClientSource rocksdb/rocksdb_client.cpp)
add_executable(init_rocksdb ${CoreSource} ${RocksDBClientSource} rocksdb/init_rocksdb.cpp)
add_executable(run_rocksdb ${CoreSource} ${RocksDBClientSource} rocksdb/run_rocksdb.cpp)
target_link_libraries(init_rocksdb /usr/local/lib/librocksdb.so pthread ${YAML_CPP_LIBRARIES})
target_link_libraries(run_rocksdb /usr/local/lib/librocksdb.so pthread ${YAML_CPP_LIBRARIES})

set(RedisClientSource redis/redis_client.cpp)
add_executable(init_redis ${CoreSource} ${RedisClientSource} redis/init_redis.cpp)
add_executable(run_redis ${CoreSource} ${RedisClientSource} redis/run_redis.cpp)
target_link_libraries(init_redis pthread hiredis ${YAML_CPP_LIBRARIES})
target_link_libraries(run_redis pthread hiredis ${YAML_CPP_LIBRARIES})

set(MemcachedClientSource memcached/memcached_client.cpp)
add_executable(init_memcached ${CoreSource} ${MemcachedClientSource} memcached/init_memcached.cpp)
add_executable(run_memcached ${CoreSource} ${MemcachedClientSource} memcached/run_memcached.cpp)
target_link_libraries(init_memcached pthread memcached ${YAML_CPP_LIBRARIES})
target_link_libraries(run_memcached pthread memcached ${YAML_CPP_LIBRARIES})

