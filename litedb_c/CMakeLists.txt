cmake_minimum_required(VERSION 3.10)
project(litedb)

#set(CMAKE_C_STANDARD 99)
set(CMAKE_CXX_STANDARD 11)

#add_library(litedb litedb.c)

# Since its a single source file, we don't need to link the header file
add_executable(litedb_test test.cpp)
add_executable(litedb main.cpp)

#target_link_libraries(litedb_test litedb)
