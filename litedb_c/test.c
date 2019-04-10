#include <stdio.h>
#include "litedb.c"

static int main_ret = 0;
static int test_count = 0;
static int test_pass = 0;

#define EXPECT_EQ_BASE(equality, expect, actual, format) \
do {\
    test_count++;\
    if (equality)\
        test_pass++;\
    else {\
        fprintf(stderr, "%s:%d: expect: " format " actual: " format "\n", __FILE__, __LINE__, expect, actual);\
        main_ret = 1;\
    }\
} while(0)

#define EXPECT_EQ_INT(expect, actual) EXPECT_EQ_BASE((expect) == (actual), expect, actual, "%d")
#define EXPECT_EQ_CHAR(expect, actual) EXPECT_EQ_BASE((expect) == (actual), expect, actual, "%c")


#define EXPECT_EQ_STRING(expect, actual, alength) \
EXPECT_EQ_BASE(sizeof(expect) - 1 == alength && memcmp(expect, actual, alength + 1) == 0, expect, actual, "%s")

#define EXPECT_RELATION_COLUMN(rc, re, col) \
do { \
    EXPECT_EQ_CHAR((re), (rc)->relation);\
    EXPECT_EQ_INT((col), (rc)->column);\
} while (0)

//////////////////
// Data loader //
/////////////////

static void test_load_csv_file() {
    struct_file file;
    init_struct_file(&file);

    load_csv_file('E', "../data/xxxs/E.csv", &file);

    EXPECT_EQ_INT(2, file.num_row);
    EXPECT_EQ_INT(5, file.num_col);

    free_struct_file(&file);
}

static void test_load_csv_file_l() {
    struct_file file;
    init_struct_file(&file);

    load_csv_file('A', "../data/l/A.csv", &file);

    EXPECT_EQ_INT(10000000, file.num_row);
    EXPECT_EQ_INT(50, file.num_col);

    free_struct_file(&file);
}

static void test_load_csv_files(const char *path) {
    // redirect stdin
    freopen(path, "r", stdin);

    char *input = NULL;
    read_first_part_from_stdin(&input);

    // path to files
    struct_input_files files;
    init_struct_input_files(&files);

    // parse paths
    parse_first_part(&files, input);

    // load files
    struct_files loaded_files;
    load_csv_files(&files, &loaded_files);

    // clean up
    free_struct_input_files(&files);
    free_struct_files(&loaded_files);
    free(input);
}


static void test_dataloader() {
    test_load_csv_file();
//    test_load_csv_file_l();
    test_load_csv_files("./test_input/first_part_xxxs.txt");
//    test_load_csv_files("./test_input/first_part_m.txt");
}

////////////
// Parser //
////////////

// below are tests for first part
static void test_parse_first_part() {
    const char path[] = "./test_input/first_part_xxxs.txt";
    // redirect stdin
    freopen(path, "r", stdin);

    char *input = NULL;
    read_first_part_from_stdin(&input);

    struct_input_files files;
    init_struct_input_files(&files);

    parse_first_part(&files, input);

    EXPECT_EQ_INT(5, (int) files.length);

    // clean up
    free_struct_input_files(&files);
    free(input);
}

// below are test for second part
static void test_parse_first_line() {
    struct_parse_context c;
    init_struct_parse_context(&c, "SELECT SUM(D.c0), SUM(D.c4), SUM(C.c1)\n");

    struct_first_line first_line;

    int ret = parse_first_line(&c, &first_line);
    // should return 0
    EXPECT_EQ_INT(0, ret);

    // check content size
    EXPECT_EQ_INT(3, (int) first_line.length);
    // check content
    EXPECT_RELATION_COLUMN(&first_line.sums[0], 'D', 0);
    EXPECT_RELATION_COLUMN(&first_line.sums[1], 'D', 4);
    EXPECT_RELATION_COLUMN(&first_line.sums[2], 'C', 1);

    free_struct_parse_context(&c);
    free_struct_first_line(&first_line);
}

static void test_parse_relation_column() {
    struct_parse_context c;
    init_struct_parse_context(&c, "D.c0)");

    struct_relation_column rc;

    parse_relation_column(&c, &rc);

    EXPECT_RELATION_COLUMN(&rc, 'D', 0);

    free_struct_relation_column(&rc);
    free_struct_parse_context(&c);
}

static void test_parse_sum() {
    struct_parse_context c;
    init_struct_parse_context(&c, "SUM(D.c0)\n");

    struct_relation_column rc;

    // run
    parse_first_line_sum(&c, &rc);

    EXPECT_RELATION_COLUMN(&rc, 'D', 0);

    free_struct_relation_column(&rc);
    free_struct_parse_context(&c);
}

static void test_parse_sums() {
    struct_parse_context c;
    init_struct_parse_context(&c, "SUM(A.c1), SUM(C.c0), SUM(C.c3), SUM(C.c4)\n");

    struct_first_line first;

    // run
    parse_first_line_sums(&c, &first);

    EXPECT_EQ_INT(4, (int) first.length);

    EXPECT_RELATION_COLUMN(&first.sums[0], 'A', 1);
    EXPECT_RELATION_COLUMN(&first.sums[1], 'C', 0);
    EXPECT_RELATION_COLUMN(&first.sums[2], 'C', 3);
    EXPECT_RELATION_COLUMN(&first.sums[3], 'C', 4);

    free_struct_parse_context(&c);
    free_struct_first_line(&first);
}

static void test_parse_query() {
    struct_parse_context c;
    init_struct_parse_context(&c,
                              "SELECT SUM(A.c1), SUM(C.c0), SUM(C.c3), SUM(C.c4)\nFROM A, C, D\nWHERE A.c2 = C.c0 AND A.c3 = D.c0 AND C.c2 = D.c2\nAND C.c1 < -4;");

    struct_query query;
    parse_query(&c, &query);

    // check first line
    struct_first_line first = query.first;

    EXPECT_EQ_INT(4, (int) first.length);

    EXPECT_RELATION_COLUMN(&first.sums[0], 'A', 1);
    EXPECT_RELATION_COLUMN(&first.sums[1], 'C', 0);
    EXPECT_RELATION_COLUMN(&first.sums[2], 'C', 3);
    EXPECT_RELATION_COLUMN(&first.sums[3], 'C', 4);

    // check second line
    struct_second_line second = query.second;

    EXPECT_EQ_INT(3, (int) second.length);
    EXPECT_EQ_CHAR('A', second.relations[0]);
    EXPECT_EQ_CHAR('C', second.relations[1]);
    EXPECT_EQ_CHAR('D', second.relations[2]);

    struct_third_line third = query.third;

    EXPECT_EQ_INT(3, (int) third.length);
    EXPECT_RELATION_COLUMN(&third.joins[0].lhs, 'A', 2);
    EXPECT_RELATION_COLUMN(&third.joins[0].rhs, 'C', 0);

    EXPECT_RELATION_COLUMN(&third.joins[1].lhs, 'A', 3);
    EXPECT_RELATION_COLUMN(&third.joins[1].rhs, 'D', 0);

    EXPECT_RELATION_COLUMN(&third.joins[2].lhs, 'C', 2);
    EXPECT_RELATION_COLUMN(&third.joins[2].rhs, 'D', 2);

    struct_fourth_line fourth = query.fourth;

    EXPECT_EQ_INT(1, (int) fourth.length);

    EXPECT_RELATION_COLUMN(&fourth.predicates[0].lhs, 'C', 1);
    EXPECT_EQ_INT(LESS_THAN, fourth.predicates[0].operator);
    EXPECT_EQ_INT(-4, fourth.predicates[0].rhs);

    // free
    free_struct_parse_context(&c);
    free_struct_query(&query);
}

static void test_parse_second_line_relation() {
    struct_parse_context c;
    init_struct_parse_context(&c, "A\n");

    char ch = 0;
    parse_second_line_relation(&c, &ch);

    EXPECT_EQ_CHAR('A', ch);

    free_struct_parse_context(&c);
}

static void test_parse_second_line_relations() {
    struct_parse_context c;
    init_struct_parse_context(&c, "A, B, C, D\n");

    struct_second_line sl;

    parse_second_line_relations(&c, &sl);

    EXPECT_EQ_INT(4, (int) sl.length);
    EXPECT_EQ_CHAR('A', sl.relations[0]);
    EXPECT_EQ_CHAR('B', sl.relations[1]);
    EXPECT_EQ_CHAR('C', sl.relations[2]);
    EXPECT_EQ_CHAR('D', sl.relations[3]);

    free_struct_parse_context(&c);
    free_struct_second_line(&sl);
}

static void test_parse_secondline() {
    struct_parse_context c;
    init_struct_parse_context(&c, "FROM A, B, C, E\n");

    struct_second_line sl;

    parse_second_line(&c, &sl);

    EXPECT_EQ_INT(4, (int) sl.length);
    EXPECT_EQ_CHAR('A', sl.relations[0]);
    EXPECT_EQ_CHAR('B', sl.relations[1]);
    EXPECT_EQ_CHAR('C', sl.relations[2]);
    EXPECT_EQ_CHAR('E', sl.relations[3]);

    free_struct_parse_context(&c);
    free_struct_second_line(&sl);
}

static void test_parse_third_line_join() {
    struct_parse_context c;
    init_struct_parse_context(&c, "A.c2 = C.c0\n");

    struct_join join;
    parse_third_line_join(&c, &join);

    EXPECT_RELATION_COLUMN(&join.lhs, 'A', 2);
    EXPECT_RELATION_COLUMN(&join.rhs, 'C', 0);

    free_struct_parse_context(&c);
    free_struct_join(&join);
}

static void test_parse_third_line_joins() {
    struct_parse_context c;
    init_struct_parse_context(&c, "A.c2 = C.c0 AND A.c1 = B.c0 AND C.c1 = E.c0\n");

    struct_third_line tl;
    parse_third_line_joins(&c, &tl);

    EXPECT_EQ_INT(3, (int) tl.length);

    EXPECT_RELATION_COLUMN(&tl.joins[0].lhs, 'A', 2);
    EXPECT_RELATION_COLUMN(&tl.joins[0].rhs, 'C', 0);

    EXPECT_RELATION_COLUMN(&tl.joins[1].lhs, 'A', 1);
    EXPECT_RELATION_COLUMN(&tl.joins[1].rhs, 'B', 0);

    EXPECT_RELATION_COLUMN(&tl.joins[2].lhs, 'C', 1);
    EXPECT_RELATION_COLUMN(&tl.joins[2].rhs, 'E', 0);

    free_struct_parse_context(&c);
    free_struct_third_line(&tl);
}

static void test_parse_third_line() {
    struct_parse_context c;
    init_struct_parse_context(&c, "WHERE A.c2 = C.c0 AND A.c1 = B.c0 AND C.c1 = E.c0\n");

    struct_third_line tl;
    parse_third_line(&c, &tl);

    EXPECT_EQ_INT(3, (int) tl.length);

    EXPECT_RELATION_COLUMN(&tl.joins[0].lhs, 'A', 2);
    EXPECT_RELATION_COLUMN(&tl.joins[0].rhs, 'C', 0);

    EXPECT_RELATION_COLUMN(&tl.joins[1].lhs, 'A', 1);
    EXPECT_RELATION_COLUMN(&tl.joins[1].rhs, 'B', 0);

    EXPECT_RELATION_COLUMN(&tl.joins[2].lhs, 'C', 1);
    EXPECT_RELATION_COLUMN(&tl.joins[2].rhs, 'E', 0);

    free_struct_parse_context(&c);
    free_struct_third_line(&tl);
}

static void test_parse_fourth_line_predicate() {
    struct_parse_context c;
    init_struct_parse_context(&c, "C.c2 = -2247;");

    struct_predicate p;
    parse_fourth_line_predicate(&c, &p);

    EXPECT_RELATION_COLUMN(&p.lhs, 'C', 2);
    EXPECT_EQ_INT(EQUAL, p.operator);
    EXPECT_EQ_INT(-2247, p.rhs);

    free_struct_parse_context(&c);
    free_struct_predicate(&p);
}

static void test_parse_fourth_line_predicates() {
    struct_parse_context c;
    init_struct_parse_context(&c, "C.c2 = -2247 AND A.c0 < -47;");

    struct_fourth_line fl;
    parse_fourth_line_predicates(&c, &fl);

    EXPECT_EQ_INT(2, (int) fl.length);

    EXPECT_RELATION_COLUMN(&fl.predicates[0].lhs, 'C', 2);
    EXPECT_EQ_INT(EQUAL, fl.predicates[0].operator);
    EXPECT_EQ_INT(-2247, fl.predicates[0].rhs);

    EXPECT_RELATION_COLUMN(&fl.predicates[1].lhs, 'A', 0);
    EXPECT_EQ_INT(LESS_THAN, fl.predicates[1].operator);
    EXPECT_EQ_INT(-47, fl.predicates[1].rhs);

    free_struct_parse_context(&c);
    free_struct_fourth_line(&fl);
}

static void test_parse_fourth_line() {
    struct_parse_context c;
    init_struct_parse_context(&c, "AND C.c2 = -2247 AND A.c0 < -47;");

    struct_fourth_line fl;
    parse_fourth_line(&c, &fl);

    EXPECT_EQ_INT(2, (int) fl.length);

    EXPECT_RELATION_COLUMN(&fl.predicates[0].lhs, 'C', 2);
    EXPECT_EQ_INT(EQUAL, fl.predicates[0].operator);
    EXPECT_EQ_INT(-2247, fl.predicates[0].rhs);

    EXPECT_RELATION_COLUMN(&fl.predicates[1].lhs, 'A', 0);
    EXPECT_EQ_INT(LESS_THAN, fl.predicates[1].operator);
    EXPECT_EQ_INT(-47, fl.predicates[1].rhs);

    free_struct_parse_context(&c);
    free_struct_fourth_line(&fl);
}

static void test_parse_queries() {
    const char path[] = "./test_input/queries.txt";

    // load content from file
    char *input;
    FILE *f = fopen(path, "r");
    fseek(f, 0, SEEK_END);
    long fsize = ftell(f);
    fseek(f, 0, SEEK_SET);  /* same as rewind(f); */

    input = malloc(fsize + 1);
    fread(input, fsize, 1, f);
    fclose(f);

    input[fsize] = 0;

    assert(strlen(input) == fsize);

    // load queries from input
    struct_queries queries;

    struct_parse_context c;
    init_struct_parse_context(&c, input);

    parse_queries(&c, &queries);

    EXPECT_EQ_INT(30, (int) queries.length);

    // check some results by hand
    EXPECT_EQ_STRING("ABCDE", queries.queries[29].second.relations, strlen("ABCDE"));
    EXPECT_EQ_INT(-4157, queries.queries[27].fourth.predicates[0].rhs);

    free_struct_parse_context(&c);
    free_struct_queries(&queries);
    free(input);
}

static void test_parse_second_part() {
    const char path[] = "./test_input/second_part.txt";

    // load content from file
    char *input;
    FILE *f = fopen(path, "r");
    fseek(f, 0, SEEK_END);
    long fsize = ftell(f);
    fseek(f, 0, SEEK_SET);  /* same as rewind(f); */

    input = malloc(fsize + 1);
    fread(input, fsize, 1, f);
    fclose(f);
    input[fsize] = 0;

    assert(strlen(input) == fsize);

    // load queries from input
    struct_queries queries;

    parse_second_part(&queries, input);

    EXPECT_EQ_INT(4, (int) queries.length);

    // check some results by hand
    EXPECT_EQ_STRING("ABCD", queries.queries[0].second.relations, strlen("ABCD"));
    EXPECT_EQ_INT(-9496, queries.queries[0].fourth.predicates[0].rhs);
    EXPECT_EQ_INT(1, queries.queries[0].fourth.predicates[1].rhs);

    EXPECT_EQ_INT(0, (int) queries.queries[1].fourth.length);

    // clean up
    free_struct_queries(&queries);
    free(input);
}

static void test_parse_second_part_from_stdin() {
    const char path[] = "./test_input/second_part.txt";
    // redirect stdin
    freopen(path, "r", stdin);

    char *input = NULL;
    read_second_part_from_stdin(&input);

    struct_queries queries;

    parse_second_part(&queries, input);

    EXPECT_EQ_INT(4, (int) queries.length);

    EXPECT_EQ_STRING("ABCDE", queries.queries[1].second.relations, strlen("ABCDE"));
    EXPECT_EQ_INT(0, (int) queries.queries[1].fourth.length);
    EXPECT_EQ_INT(0, (int) queries.queries[3].fourth.length);

    // clean up
    free_struct_queries(&queries);
    free(input);
}

static void test_read_first_part() {
    const char path[] = "./test_input/first_part_xxxs.txt";
    // redirect stdin
    freopen(path, "r", stdin);

    char *input = NULL;
    read_first_part_from_stdin(&input);

    EXPECT_EQ_STRING("data/xxxs/A.csv,data/xxxs/B.csv,data/xxxs/C.csv", input, strlen(input));

    // clean up
    free(input);
}

static void test_parse() {
    // test parser first part
    test_parse_first_part();

    // first line
    test_parse_relation_column();

    test_parse_sum();
    test_parse_sums();
    test_parse_first_line();

    // second line
    test_parse_second_line_relation();
    test_parse_second_line_relations();
    test_parse_secondline();

    // third line
    test_parse_third_line_join();
    test_parse_third_line_joins();
    test_parse_third_line();

    // fourth line
    test_parse_fourth_line_predicate();
    test_parse_fourth_line_predicates();
    test_parse_fourth_line();

    // query and queries
    test_parse_query();
    test_parse_queries();

    // second part
    test_parse_second_part();
    test_parse_second_part_from_stdin();
}

// test A.c0 = 4422
static void test_predicate_simple_1() {
    struct_file file;
    init_struct_file(&file);

    load_csv_file('A', "../data/xxxs/A.csv", &file);

    struct_predicate predicate;

    // A.c0 == 4422
    predicate.lhs.relation = 'A';
    predicate.lhs.column = 0;
    predicate.operator = EQUAL;
    predicate.rhs = 4422;

    filter_data_given_predicate(&file, &predicate);

    EXPECT_EQ_INT(100, file.num_row);
    EXPECT_EQ_INT(10, file.num_col);
    EXPECT_EQ_INT(1, file.df->num_row);
    EXPECT_EQ_INT(18, file.df->index[0]);

    free_struct_file(&file);
}

// test A.c1 > 5000
static void test_predicate_simple_2() {
    struct_file file;
    init_struct_file(&file);

    load_csv_file('A', "../data/xxxs/A.csv", &file);

    struct_predicate predicate;

    predicate.lhs.relation = 'A';
    predicate.lhs.column = 1;
    predicate.operator = GREATER_THAN;
    predicate.rhs = 5000;

    filter_data_given_predicate(&file, &predicate);
    EXPECT_EQ_INT(100, file.num_row);
    EXPECT_EQ_INT(10, file.num_col);
    EXPECT_EQ_INT(58, file.df->num_row);

    free_struct_file(&file);
}

static void test_predicate_combined() {
    struct_file file;
    init_struct_file(&file);

    load_csv_file('A', "../data/xxxs/A.csv", &file);

    struct_predicate predicate1;
    predicate1.lhs.relation = 'A';
    predicate1.lhs.column = 0;
    predicate1.operator = EQUAL;
    predicate1.rhs = 4422;

    struct_predicate predicate2;
    predicate2.lhs.relation = 'A';
    predicate2.lhs.column = 1;
    predicate2.operator = GREATER_THAN;
    predicate2.rhs = 5000;

    filter_data_given_predicate(&file, &predicate1);
    filter_data_given_predicate(&file, &predicate2);

    // what if we select from already empty file?
    filter_data_given_predicate(&file, &predicate2);

    EXPECT_EQ_INT(0, file.num_row);

    free_struct_file(&file);
}

static void test_predicate_xxs_1() {
    struct_file file;
    init_struct_file(&file);

    load_csv_file('A', "../data/xxs/A.csv", &file);

    struct_predicate predicate;

    predicate.lhs.relation = 'A';
    predicate.lhs.column = 1;
    predicate.operator = GREATER_THAN;
    predicate.rhs = 5000;

    filter_data_given_predicate(&file, &predicate);
    EXPECT_EQ_INT(1000, file.num_row);
    EXPECT_EQ_INT(50, file.num_col);
    EXPECT_EQ_INT(919, file.df->num_row);

    free_struct_file(&file);
}

static void test_predicate_m_1() {
    struct_file file;
    init_struct_file(&file);

    load_csv_file('A', "../data/m/A.csv", &file);

    struct_predicate predicate;

    predicate.lhs.relation = 'A';
    predicate.lhs.column = 1;
    predicate.operator = GREATER_THAN;
    predicate.rhs = 5000;

    filter_data_given_predicate(&file, &predicate);
    EXPECT_EQ_INT(1000000, file.num_row);
    EXPECT_EQ_INT(50, file.num_col);
    EXPECT_EQ_INT(434960, file.df->num_row);

    free_struct_file(&file);
}

static void test_predicates() {
    test_predicate_simple_1();
    test_predicate_simple_2();
    test_predicate_combined();

    test_predicate_xxs_1();
//    test_predicate_m_1();
}

/**
 * 1. load path from stdin
 * 2. load files given path
 * 3. join
 */
static void test_join_manual() {
    freopen("./test_input/join_manual.txt", "r", stdin);

    char *input = NULL;

    // 1. read path from stdin
    read_first_part_from_stdin(&input);

    struct_input_files inputs;
    init_struct_input_files(&inputs);

    parse_first_part(&inputs, input);

    // 2. load files from disk
    struct_files loaded_files;

    load_csv_files(&inputs, &loaded_files);

    // 3. join
    // A.c2 = B.c0
    struct_join join;
    join.lhs.relation = 'A';
    join.lhs.column = 2;
    join.rhs.relation = 'B';
    join.rhs.column = 0;

    // create intermediate data frame
    // free!
    struct_data_frame df;
    df.relations = (char *) malloc(2 * sizeof(char));
    df.relations[0] = 'A';
    df.relations[1] = '\0';

    df.index = (int *) malloc(2 * sizeof(int));
    df.index[0] = 0;
    df.index[1] = 1;

    df.num_row = 2;

    // init dataframe for file
    init_struct_data_frame_for_file(&loaded_files.files[0]);
    init_struct_data_frame_for_file(&loaded_files.files[1]);

    nested_loop_join(&loaded_files, &df, &loaded_files.files[1], &join);

    // check result
    // index = 0001
    EXPECT_EQ_INT(2, (int) strlen(df.relations));
    EXPECT_EQ_INT(0, df.index[0]);
    EXPECT_EQ_INT(0, df.index[1]);
    EXPECT_EQ_INT(0, df.index[2]);
    EXPECT_EQ_INT(1, df.index[3]);
    EXPECT_EQ_INT(2, df.num_row);

    free(input);
    free_struct_input_files(&inputs);
    free_struct_files(&loaded_files);
    free_struct_data_frame(&df);
}

static void test_join() {
    test_join_manual();
}

static void test_main() {
    freopen("./test_input/full.txt", "r", stdin);

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
    init_struct_files(&loaded_files, files.length);

    // load files into memory
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
}

int main() {
    // test assert
    ASSERT(1);
//    test_dataloader();
//    test_parse();
//    test_predicates();
//    test_join();
    test_main();

    printf("%d/%d (%3.2f%%) passed\n", test_pass, test_count, test_pass * 100.0 / test_count);
    return 0;
}
