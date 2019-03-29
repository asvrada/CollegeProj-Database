cmake_minimum_required(VERSION 3.10)
project(litedb_test C)

set(CMAKE_C_STANDARD 99)

add_library(litedb litedb.c)
add_executable(litedb_test test.c)
target_link_libraries(litedb_test litedb)
