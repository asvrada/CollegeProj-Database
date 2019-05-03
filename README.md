# database_course_project
This is my solution for PA3 in Database Management System course

The file you are interested in is [litedb.cpp](./litedb_c/litedb.cpp).

## How to compile/run

### CLion
I'd recommend CLion as your IDE for this repo. 

Open folder [litedb_c](./litedb_c) with CLion and it should automatically pick up the CMakeLists.txt and config the project. Then you can select one of the executables to run.

> Unfortunately, it requires a specific collection of data (csv files) to run.

### g++

The program can be compiled like `g++ -std=c++11 main.cpp`.

## What does it do?

It is a very limited database management system. The specification is in [PA3.pdf](./PA3.pdf). 

For short, It can read data from csv files which represent tables in a traditional DBMS, convert them into more computer-friendly format and store to disks. Then it can execute a subset of SQL, and output result to stdout.

## What exactly does it do?

1. Load CSV files
2. Read SQL from stdin (column store, no indexing)
3. Execute SQL

    1. Execute select/filter (things like A.c0 < 10) (push-down predicates)
    2. Optimize joins (Selinger's Algorithm, but no exactly correct)
    3. Execute joins (sorted nested loop join)
    4. Execute sums

## Is it good?

NO, don't use it anywhere else. This repo is for *educational* purpose only.

