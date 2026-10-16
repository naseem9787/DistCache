cmake_minimum_required(VERSION 3.16)
project(distcache CXX)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
if(NOT CMAKE_BUILD_TYPE)
  set(CMAKE_BUILD_TYPE Release)
endif()
add_compile_options(-Wall -Wextra)

find_package(Threads REQUIRED)

# Everything except main(), so tests can link the same code as the server.
add_library(distcache_core src/resp.cpp src/commands.cpp src/server.cpp src/wal.cpp src/persistence.cpp
            src/hash_ring.cpp src/resp_client.cpp src/router.cpp
            src/record_apply.cpp src/repl_log.cpp src/replication.cpp)
target_include_directories(distcache_core PUBLIC src)
target_link_libraries(distcache_core PUBLIC Threads::Threads)

add_executable(distcache src/main.cpp)
target_link_libraries(distcache distcache_core)

# Phase 7: the cluster router (consistent hashing proxy) and the ring analysis tool.
add_executable(distcache_router src/router_main.cpp)
target_link_libraries(distcache_router distcache_core)
add_executable(bench_ring bench/bench_ring.cpp)
target_link_libraries(bench_ring distcache_core)

# Tools: the race reproduction and the in-process benchmark.
add_executable(race_demo tests/race_demo.cpp)
target_link_libraries(race_demo distcache_core)
add_executable(bench_engine bench/bench_engine.cpp)
target_link_libraries(bench_engine distcache_core)
add_executable(bench_lru_accuracy bench/bench_lru_accuracy.cpp)
target_link_libraries(bench_lru_accuracy distcache_core)
add_executable(bench_wal bench/bench_wal.cpp)  # production code only: measures the normal build
target_link_libraries(bench_wal distcache_core)

# Diagnostics build: nanosecond timers around each step of a logged write and an in-process
# sampling profiler. Separate target so the production server has none of it. Use it to ATTRIBUTE
# time, never for headline throughput numbers.
add_library(distcache_core_diag src/resp.cpp src/commands.cpp src/server.cpp src/wal.cpp src/persistence.cpp src/diag.cpp
            src/record_apply.cpp)
target_include_directories(distcache_core_diag PUBLIC src)
target_compile_definitions(distcache_core_diag PUBLIC DISTCACHE_DIAGNOSTICS)
target_compile_options(distcache_core_diag PUBLIC -g -fno-omit-frame-pointer)
target_link_libraries(distcache_core_diag PUBLIC Threads::Threads)
add_executable(distcache_diag src/main.cpp)
target_link_libraries(distcache_diag distcache_core_diag)

# Standalone load generator. Its smart-client mode (--nodes) routes with the same HashRing code
# as the router, so it compiles that one file; nothing else from the server is linked.
add_executable(distcache_loadgen bench/loadgen.cpp src/hash_ring.cpp)
target_include_directories(distcache_loadgen PRIVATE src)
target_link_libraries(distcache_loadgen Threads::Threads)

enable_testing()
find_package(GTest REQUIRED)
add_executable(distcache_tests
  tests/test_resp.cpp tests/test_commands.cpp tests/test_store.cpp tests/test_lru.cpp
  tests/test_concurrency.cpp tests/test_server.cpp tests/test_wal.cpp tests/test_persistence.cpp
  tests/test_deferred.cpp tests/test_ring.cpp tests/test_router.cpp tests/test_repl_log.cpp tests/test_replication.cpp)
target_link_libraries(distcache_tests distcache_core GTest::gtest_main)
add_test(NAME unit COMMAND distcache_tests)
