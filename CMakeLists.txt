cmake_minimum_required(VERSION 3.16)
project(distcache CXX)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
if(NOT CMAKE_BUILD_TYPE)
  set(CMAKE_BUILD_TYPE Release)
endif()
add_compile_options(-Wall -Wextra)

# Everything except main(), so tests can link the same code as the server.
add_library(distcache_core src/resp.cpp src/commands.cpp)
target_include_directories(distcache_core PUBLIC src)

add_executable(distcache src/server.cpp)
target_link_libraries(distcache distcache_core)

enable_testing()
find_package(GTest REQUIRED)
add_executable(distcache_tests tests/test_resp.cpp tests/test_commands.cpp tests/test_store.cpp)
target_link_libraries(distcache_tests distcache_core GTest::gtest_main)
add_test(NAME unit COMMAND distcache_tests)
