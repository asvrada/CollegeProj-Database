#include <stdio.h>
#include "litedb.c"

/*
 * 1. read first part
 * 2. read second part
 * 3. execute each query
 */
int main() {
    char *first_part = NULL;
    read_first_part_from_stdin(&first_part);

    char *second_part = NULL;
    read_second_part_from_stdin(&second_part);

    printf("%s", first_part);
    printf("%s", second_part);

    // handle path to files
    struct_input_files files;
    init_struct_input_files(&files);

    // parse first part
    parse_first_part(&files, first_part);

    // load files into memory
    struct_files loaded_files;
    init_struct_files(&loaded_files, files.length);

    // parse queries
    struct_queries queries;
    parse_second_part(&queries, second_part);

    // free data that we don't need
    free(first_part);
    free(second_part);
    free_struct_input_files(&files);

    // execute each query
    for (int i = 0; i < queries.length; i++) {
        execute(&loaded_files, &queries.queries[i]);
    }

    // free this at the end
    free_struct_queries(&queries);
    free_struct_files(&loaded_files);
    return 0;
}