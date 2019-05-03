
# echo "Compiling litedb using gcc c99 with flag DEBUG"
# gcc -std=c99 -D DEBUG_PROFILING main.c

# echo "Compiling litedb using gcc c99"
# gcc -std=c99 main.c

echo "Compiling litedb using g++ c++11"
g++ -O3 -std=c++11 main.cpp
