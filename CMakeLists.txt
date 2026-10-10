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
add_library(distcache_core src/resp.cpp src/commands.cpp src/server.cpp src/wal.cpp src/persistence.cpp)
target_include_directories(distcache_core PUBLIC src)
target_link_libraries(distcache_core PUBLIC Threads::Threads)

add_executable(distcache src/main.cpp)
target_link_libraries(distcache distcache_core)

# Tools: the race reproduction and the in-process benchmark.
add_executable(race_demo tests/race_demo.cpp)
target_link_libraries(race_demo distcache_core)
add_executable(bench_engine bench/bench_engine.cpp)
target_link_libraries(bench_engine distcache_core)
add_executable(bench_lru_accuracy bench/bench_lru_accuracy.cpp)
target_link_libraries(bench_lru_accuracy distcache_core)

enable_testing()
find_package(GTest REQUIRED)
add_executable(distcache_tests
  tests/test_resp.cpp tests/test_commands.cpp tests/test_store.cpp tests/test_lru.cpp
  tests/test_concurrency.cpp tests/test_server.cpp tests/test_wal.cpp tests/test_persistence.cpp)
target_link_libraries(distcache_tests distcache_core GTest::gtest_main)
add_test(NAME unit COMMAND distcache_tests)
