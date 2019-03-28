#include <stdio.h>
#include "litedb.h"

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

#define EXPECT_RELATION_COLUMN(rc, relation, column) \
do { \
    EXPECT_EQ_STRING((relation), (rc)->val[0], strlen((relation)));\
    EXPECT_EQ_STRING((column), (rc)->val[1], strlen((column)));\
    EXPECT_EQ_INT((int) (rc)->length_relation, (int) strlen((rc)->val[0]));\
    EXPECT_EQ_INT((int) (rc)->length_column, (int) strlen((rc)->val[1]));\
} while (0)

static void test_parse_firstline() {
    struct_first_line firstline;

    struct_parse_context c;
    init_struct_parse_context(&c, "SELECT SUM(D.c0), SUM(D.c4), SUM(C.c1)\n");

    int ret = parse_first_line(&c, &firstline);
    // should return 0
    EXPECT_EQ_INT(0, ret);

    // check content size
    EXPECT_EQ_INT(3, (int) firstline.length);
    // check content
    EXPECT_RELATION_COLUMN(&firstline.sums[0], "D", "c0");
    EXPECT_RELATION_COLUMN(&firstline.sums[1], "D", "c4");
    EXPECT_RELATION_COLUMN(&firstline.sums[2], "C", "c1");
}

void test_parse_relation_column() {
    struct_parse_context c;
    init_struct_parse_context(&c, "D.c0)");

    struct_relation_column *rc = (struct_relation_column *) malloc(sizeof(struct_relation_column));

    parse_relation_column(&c, rc);

    EXPECT_RELATION_COLUMN(rc, "D", "c0");

    free_struct_relation_column(rc);
    free_struct_parse_context(&c);
    free(rc);
}

void test_parse_sum() {
    struct_parse_context c;
    init_struct_parse_context(&c, "SUM(D.c0)\n");

    struct_relation_column *rc = (struct_relation_column *) malloc(sizeof(struct_relation_column));

    // run
    parse_first_line_sum(&c, rc);

    EXPECT_RELATION_COLUMN(rc, "D", "c0");

    free_struct_relation_column(rc);
    free_struct_parse_context(&c);
    free(rc);
}

void test_parse_sums() {
    struct_parse_context c;
    init_struct_parse_context(&c, "SUM(A.c1), SUM(C.c0), SUM(C.c3), SUM(C.c4)\n");

    struct_first_line firstline;

    // run
    parse_first_line_sums(&c, &firstline);

    EXPECT_EQ_INT(4, (int) firstline.length);

    EXPECT_RELATION_COLUMN(&firstline.sums[0], "A", "c1");
    EXPECT_RELATION_COLUMN(&firstline.sums[1], "C", "c0");
    EXPECT_RELATION_COLUMN(&firstline.sums[2], "C", "c3");
    EXPECT_RELATION_COLUMN(&firstline.sums[3], "C", "c4");

    free_struct_parse_context(&c);
    free_struct_first_line(&firstline);
}

void test_parse_query() {
    struct_query query;

    struct_parse_context c;
    init_struct_parse_context(&c,
                              "SELECT SUM(A.c1), SUM(C.c0), SUM(C.c3), SUM(C.c4)\nFROM A, C, D\nWHERE A.c2 = C.c0 AND A.c3 = D.c0 AND C.c2 = D.c2\nAND C.c1 < -4;");

    parse_query(&c, &query);

    // check first line
    struct_first_line first = query.first;

    EXPECT_EQ_INT(4, (int) first.length);

    EXPECT_RELATION_COLUMN(&first.sums[0], "A", "c1");
    EXPECT_RELATION_COLUMN(&first.sums[1], "C", "c0");
    EXPECT_RELATION_COLUMN(&first.sums[2], "C", "c3");
    EXPECT_RELATION_COLUMN(&first.sums[3], "C", "c4");

    // check second line
    struct_second_line second = query.second;

    EXPECT_EQ_INT(3, (int) second.length);
    EXPECT_EQ_CHAR('A', second.relations[0]);
    EXPECT_EQ_CHAR('C', second.relations[1]);
    EXPECT_EQ_CHAR('D', second.relations[2]);

    struct_third_line third = query.third;

//    EXPECT_EQ_INT(3, third.length);
//    EXPECT_EQ_STRING(third.joins[0].lhs)

    // free
    free_struct_query(&query);
}

static void test_parse() {
//    test_parse_paths();
//    test_parse_queries();
//    test_parse_query();
    test_parse_relation_column();
    test_parse_sum();
    test_parse_sums();
    test_parse_firstline();
    test_parse_query();
}

int main() {
    test_parse();

    printf("%d/%d (%3.2f%%) passed\n", test_pass, test_count, test_pass * 100.0 / test_count);
    return 0;
}