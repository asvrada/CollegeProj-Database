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

    // handle path to files
    struct_input_files files;
    init_struct_input_files(&files);

    // parse first part
    parse_first_part(&files, first_part);

    // parse queries
    struct_queries queries;
    parse_second_part(&queries, second_part);

    struct_files loaded_files;

    // convert csv files into binary representation
    load_csv_files(&files, &loaded_files);

    // free data that we don't need
    free(first_part);
    free(second_part);
    free_struct_input_files(&files);

    // execute each query
    for (int i = 0; i < queries.length; i++) {
        execute(&loaded_files, &queries.queries[i]);

        // clean up df after each query
        free_only_struct_data_frames(&loaded_files);
    }

    // free this at the end
    free_struct_queries(&queries);
    free_struct_files(&loaded_files);
    return 0;
}