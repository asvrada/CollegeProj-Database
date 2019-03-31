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
    EXPECT_EQ_STRING((col), (rc)->column, strlen((col)));\
    EXPECT_EQ_INT((int) (rc)->length_column, (int) strlen((rc)->column));\
} while (0)


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
    EXPECT_RELATION_COLUMN(&first_line.sums[0], 'D', "c0");
    EXPECT_RELATION_COLUMN(&first_line.sums[1], 'D', "c4");
    EXPECT_RELATION_COLUMN(&first_line.sums[2], 'C', "c1");

    free_struct_parse_context(&c);
    free_struct_first_line(&first_line);
}

static void test_parse_relation_column() {
    struct_parse_context c;
    init_struct_parse_context(&c, "D.c0)");

    struct_relation_column rc;

    parse_relation_column(&c, &rc);

    EXPECT_RELATION_COLUMN(&rc, 'D', "c0");

    free_struct_relation_column(&rc);
    free_struct_parse_context(&c);
}

static void test_parse_sum() {
    struct_parse_context c;
    init_struct_parse_context(&c, "SUM(D.c0)\n");

    struct_relation_column rc;

    // run
    parse_first_line_sum(&c, &rc);

    EXPECT_RELATION_COLUMN(&rc, 'D', "c0");

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

    EXPECT_RELATION_COLUMN(&first.sums[0], 'A', "c1");
    EXPECT_RELATION_COLUMN(&first.sums[1], 'C', "c0");
    EXPECT_RELATION_COLUMN(&first.sums[2], 'C', "c3");
    EXPECT_RELATION_COLUMN(&first.sums[3], 'C', "c4");

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

    EXPECT_RELATION_COLUMN(&first.sums[0], 'A', "c1");
    EXPECT_RELATION_COLUMN(&first.sums[1], 'C', "c0");
    EXPECT_RELATION_COLUMN(&first.sums[2], 'C', "c3");
    EXPECT_RELATION_COLUMN(&first.sums[3], 'C', "c4");

    // check second line
    struct_second_line second = query.second;

    EXPECT_EQ_INT(3, (int) second.length);
    EXPECT_EQ_CHAR('A', second.relations[0]);
    EXPECT_EQ_CHAR('C', second.relations[1]);
    EXPECT_EQ_CHAR('D', second.relations[2]);

    struct_third_line third = query.third;

    EXPECT_EQ_INT(3, (int) third.length);
    EXPECT_RELATION_COLUMN(&third.joins[0].lhs, 'A', "c2");
    EXPECT_RELATION_COLUMN(&third.joins[0].rhs, 'C', "c0");

    EXPECT_RELATION_COLUMN(&third.joins[1].lhs, 'A', "c3");
    EXPECT_RELATION_COLUMN(&third.joins[1].rhs, 'D', "c0");

    EXPECT_RELATION_COLUMN(&third.joins[2].lhs, 'C', "c2");
    EXPECT_RELATION_COLUMN(&third.joins[2].rhs, 'D', "c2");

    struct_fourth_line fourth = query.fourth;

    EXPECT_EQ_INT(1, (int) fourth.length);

    EXPECT_RELATION_COLUMN(&fourth.predicates[0].lhs, 'C', "c1");
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

    EXPECT_RELATION_COLUMN(&join.lhs, 'A', "c2");
    EXPECT_RELATION_COLUMN(&join.rhs, 'C', "c0");

    free_struct_parse_context(&c);
    free_struct_join(&join);
}

static void test_parse_third_line_joins() {
    struct_parse_context c;
    init_struct_parse_context(&c, "A.c2 = C.c0 AND A.c1 = B.c0 AND C.c1 = E.c0\n");

    struct_third_line tl;
    parse_third_line_joins(&c, &tl);

    EXPECT_EQ_INT(3, (int) tl.length);

    EXPECT_RELATION_COLUMN(&tl.joins[0].lhs, 'A', "c2");
    EXPECT_RELATION_COLUMN(&tl.joins[0].rhs, 'C', "c0");

    EXPECT_RELATION_COLUMN(&tl.joins[1].lhs, 'A', "c1");
    EXPECT_RELATION_COLUMN(&tl.joins[1].rhs, 'B', "c0");

    EXPECT_RELATION_COLUMN(&tl.joins[2].lhs, 'C', "c1");
    EXPECT_RELATION_COLUMN(&tl.joins[2].rhs, 'E', "c0");

    free_struct_parse_context(&c);
    free_struct_third_line(&tl);
}

static void test_parse_third_line() {
    struct_parse_context c;
    init_struct_parse_context(&c, "WHERE A.c2 = C.c0 AND A.c1 = B.c0 AND C.c1 = E.c0\n");

    struct_third_line tl;
    parse_third_line(&c, &tl);

    EXPECT_EQ_INT(3, (int) tl.length);

    EXPECT_RELATION_COLUMN(&tl.joins[0].lhs, 'A', "c2");
    EXPECT_RELATION_COLUMN(&tl.joins[0].rhs, 'C', "c0");

    EXPECT_RELATION_COLUMN(&tl.joins[1].lhs, 'A', "c1");
    EXPECT_RELATION_COLUMN(&tl.joins[1].rhs, 'B', "c0");

    EXPECT_RELATION_COLUMN(&tl.joins[2].lhs, 'C', "c1");
    EXPECT_RELATION_COLUMN(&tl.joins[2].rhs, 'E', "c0");

    free_struct_parse_context(&c);
    free_struct_third_line(&tl);
}

static void test_parse_fourth_line_predicate() {
    struct_parse_context c;
    init_struct_parse_context(&c, "C.c2 = -2247;");

    struct_predicate p;
    parse_fourth_line_predicate(&c, &p);

    EXPECT_RELATION_COLUMN(&p.lhs, 'C', "c2");
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

    EXPECT_RELATION_COLUMN(&fl.predicates[0].lhs, 'C', "c2");
    EXPECT_EQ_INT(EQUAL, fl.predicates[0].operator);
    EXPECT_EQ_INT(-2247, fl.predicates[0].rhs);

    EXPECT_RELATION_COLUMN(&fl.predicates[1].lhs, 'A', "c0");
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

    EXPECT_RELATION_COLUMN(&fl.predicates[0].lhs, 'C', "c2");
    EXPECT_EQ_INT(EQUAL, fl.predicates[0].operator);
    EXPECT_EQ_INT(-2247, fl.predicates[0].rhs);

    EXPECT_RELATION_COLUMN(&fl.predicates[1].lhs, 'A', "c0");
    EXPECT_EQ_INT(LESS_THAN, fl.predicates[1].operator);
    EXPECT_EQ_INT(-47, fl.predicates[1].rhs);

    free_struct_parse_context(&c);
    free_struct_fourth_line(&fl);
}

static void test_parse_queries() {
    const char path[] = "./queries.txt";

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

void test_parse_second_part() {
    const char path[] = "./second_part.txt";

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

    EXPECT_EQ_INT(3, (int) queries.length);

    // check some results by hand
    EXPECT_EQ_STRING("ABCD", queries.queries[0].second.relations, strlen("ABCD"));
    EXPECT_EQ_INT(7789, queries.queries[1].fourth.predicates[0].rhs);

    free_struct_queries(&queries);
    free(input);
}

void test_parse_second_part_from_stdin() {
    const char path[] = "./second_part.txt";
    // redirect stdin
    freopen(path, "r", stdin);

    char *input = NULL;
    read_second_part_from_stdin(&input);

    struct_queries queries;

    parse_second_part(&queries, input);

    EXPECT_EQ_INT(3, (int) queries.length);

    // check some results by hand
    free_struct_queries(&queries);
    free(input);
    input = NULL;
}

static void test_parse() {
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

int main() {
    test_parse();

    printf("%d/%d (%3.2f%%) passed\n", test_pass, test_count, test_pass * 100.0 / test_count);
    return 0;
}
