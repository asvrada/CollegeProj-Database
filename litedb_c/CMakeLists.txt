cmake_minimum_required(VERSION 3.10)
project(litedb_test C)

set(CMAKE_C_STANDARD 99)

#add_library(litedb litedb.c)

# Since its a single source file, we don't need to link the header file
add_executable(litedb_test test.c)
add_executable(litedb main.c)

#target_link_libraries(litedb_test litedb)
