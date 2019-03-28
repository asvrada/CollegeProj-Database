//
// Created by ZiJie Wu on 2019-03-28.
//

#ifndef LITE_DB_TEST_LITEDB_H
#define LITE_DB_TEST_LITEDB_H

#include <stdlib.h>
#include <assert.h>
#include <string.h>

#ifndef PARSE_STACK_INIT_SIZE
#define PARSE_STACK_INIT_SIZE 256
#endif

#define EXPECT(c, ch) do { assert(c->input[0] == (ch)); } while(0)
#define EXPECT_ALPHABET(c) do { \
assert(('a' <= c->input[0] && c->input[0] <= 'z') || ('A' <= c->input[0] && c->input[0] <= 'Z')); } while(0)

//////////
// enum //
//////////

// this is the type of operations involved in the query predicates
typedef enum {
    EQUAL,
    LESS_THAN,
    GREATER_THAN
} enum_operator;

typedef enum {
    PARSE_OK = 0,
    PARSE_FAILED
} enum_return_value;

////////////
// struct //
////////////

// SUM(D.c0)
typedef struct {
    // 2D string
    // char[0] = string of the relation
    // char[1] = string of the column
    char **val;
    // length of the relation string (not including \0)
    size_t length_relation;
    // length of the column string (same)
    size_t length_column;
} struct_relation_column;

// SELECT SUM(D.c0), SUM(D.c4), SUM(C.c1)
typedef struct {
    struct_relation_column *sums;
    // size of above array
    // number of SUM queries
    size_t length;
} struct_first_line;

// FROM A, B, C, D
typedef struct {
    char *relations;
    size_t length;
} struct_second_line;

// A.c1 = B.c0
typedef struct {
    // length will be 2, lhs and rhs
    struct_relation_column lhs;
    struct_relation_column rhs;
} struct_join;

// WHERE A.c1 = B.c0 AND A.c3 = D.c0 AND C.c2 = D.c2
typedef struct {
    struct_join *joins;
    // number of join operations
    size_t length;
} struct_third_line;

// D.c3 = -9496
typedef struct {
    struct_relation_column lhs;
    enum_operator operator;
    int rhs;
} struct_predicate;

// AND C.c2 = 2247;
typedef struct {
    struct_predicate *predicates;
    // number of predicates
    size_t length;
} struct_fourth_line;

/*
SELECT SUM(D.c0), SUM(D.c4), SUM(C.c1)
FROM A, B, C, D
WHERE A.c1 = B.c0 AND A.c3 = D.c0 AND C.c2 = D.c2
AND D.c3 = -9496;
 */
typedef struct {
    struct_first_line first;
    struct_second_line second;
    struct_third_line third;
    struct_fourth_line fourth;
} struct_query;

// this struct stores information during parsing
typedef struct {
    const char *input;
    char *stack;
    size_t size, top;
} struct_parse_context;

static void *context_pop(struct_parse_context *c, size_t size) {
    assert(c->top >= size);
    c->top -= size;
    return c->stack + c->top;
}

static void *context_push(struct_parse_context *c, size_t size) {
    void *ret;
    assert(size > 0);

    // expand 1.5 times if smaller than whats required
    if (c->top + size >= c->size) {
        if (c->size == 0) {
            c->size = PARSE_STACK_INIT_SIZE;
        }

        while (c->top + size >= c->size) {
            // size = size * 1.5
            c->size += c->size >> 1;
        }

        c->stack = (char *) realloc(c->stack, c->size);
    }

    ret = c->stack + c->top;
    c->top += size;
    return ret;
}

/////////////////
// Init & Free //
/////////////////

static void init_struct_parse_context(struct_parse_context *c, const char *input) {
    c->input = input;
    c->stack = NULL;
    c->size = c->top = 0;
}

static void free_struct_parse_context(struct_parse_context *c) {
    assert(c->top == 0);

    free(c->stack);
    c->top = c->size = 0;
}

static void free_struct_relation_column(struct_relation_column *rc) {
    assert(rc->val != NULL);

    free(rc->val[0]);
    free(rc->val[1]);
    free(rc->val);

    rc->length_column = rc->length_relation = 0;
}

static void free_struct_first_line(struct_first_line *fl) {
    for (int i = 0; i < fl->length; i++) {
        free_struct_relation_column(&fl->sums[i]);
    }

    free(fl->sums);
    fl->length = 0;
}

static void free_struct_second_line(struct_second_line *sl) {

}

static void free_struct_third_line(struct_third_line *tl) {

}

static void free_struct_fourth_line(struct_fourth_line *fl) {

}

static void free_struct_query(struct_query *query) {
    free_struct_first_line(&query->first);
    free_struct_second_line(&query->second);
    free_struct_third_line(&query->third);
    free_struct_fourth_line(&query->fourth);
}

///////////
// parse //
///////////

// skip whitespaces as many as possible
void parse_whitespaces(struct_parse_context *c) {
    while (c->input[0] == ' ') {
        c->input++;
    }
}

// skip one whitespace if there is one
void parse_whitespace(struct_parse_context *c) {
    if (c->input[0] == ' ') {
        c->input++;
    }
}

// skip newlines as many as possible
void parse_newlines(struct_parse_context *c) {
    while (c->input[0] == '\n') {
        c->input++;
    }
}

// skip one newline if there is one
void parse_newline(struct_parse_context *c) {
    if (c->input[0] == '\n') {
        c->input++;
    }
}

void parse_first_line_select(struct_parse_context *c) {
    assert(0 == strncmp(c->input, "SELECT", strlen("SELECT")));
    c->input += strlen("SELECT");
}

int parse_relation_column(struct_parse_context *c, struct_relation_column *relation_column) {
    // Should start with alphabet
    EXPECT_ALPHABET(c);

    size_t head = c->top;
    const char *tmp_p = c->input;

    relation_column->val = (char **) malloc(2 * sizeof(char *));

    // parse relation
    while (1) {
        char ch = tmp_p[0];

        // end of relation
        if (ch == '.') {
            size_t len = c->top - head;
            const char *str = context_pop(c, len);
            // copy this str to struct
            relation_column->val[0] = (char *) malloc(len + 1);
            memcpy(relation_column->val[0], str, len);
            relation_column->val[0][len] = '\0';
            relation_column->length_relation = len;

            c->input = tmp_p;
            break;
        }

        *(char *) context_push(c, sizeof(char)) = ch;
        tmp_p++;
    }

    // skip '.'
    c->input++;

    // parse column
    head = c->top;
    tmp_p = c->input;

    while (1) {
        char ch = tmp_p[0];

        // end of column
        if (ch == ')') {
            size_t len = c->top - head;
            const char *str = context_pop(c, len);
            // copy str
            relation_column->val[1] = (char *) malloc(len + 1);
            memcpy(relation_column->val[1], str, len);
            relation_column->val[1][len] = '\0';
            relation_column->length_column = len;

            c->input = tmp_p;
            break;
        }

        *(char *) context_push(c, sizeof(char)) = ch;
        tmp_p++;
    }

    return PARSE_OK;
}

int parse_first_line_sum(struct_parse_context *c, struct_relation_column *relation_column) {
    EXPECT(c, 'S');

    // move pointer to relation
    c->input += strlen("SUM(");

    parse_relation_column(c, relation_column);

    // move pointer, skip )
    c->input++;

    return PARSE_OK;
}

/*
Sums -> Sum | Sum,WhitespaceSums
 */
// todo
int parse_first_line_sums(struct_parse_context *c, struct_first_line *v) {
    // Should start with SUM
    EXPECT(c, 'S');

    struct_relation_column *rc = NULL;

    size_t head = c->top;

    rc = (struct_relation_column *) malloc(sizeof(struct_relation_column));
    parse_first_line_sum(c, rc);

    // push into context
    *(struct_relation_column *) context_push(c, sizeof(struct_relation_column)) = *rc;
    free(rc);

    while (c->input[0] == ',') {
        c->input++;
        parse_whitespace(c);

        rc = (struct_relation_column *) malloc(sizeof(struct_relation_column));
        parse_first_line_sum(c, rc);

        // push into context
        *(struct_relation_column *) context_push(c, sizeof(struct_relation_column)) = *rc;
        free(rc);
    }

    // pop
    size_t len = c->top - head;
    const struct_relation_column *rcs = context_pop(c, len);

    v->sums = (struct_relation_column *) malloc(len);
    memcpy(v->sums, rcs, len);
    v->length = len / sizeof(struct_relation_column);

    return PARSE_OK;
}

/*
FirstLine -> SELECTWhitespaceSums
 */
int parse_first_line(struct_parse_context *c, struct_first_line *v) {
    // should start with SELECT
    EXPECT(c, 'S');

    parse_first_line_select(c);
    parse_whitespace(c);
    return parse_first_line_sums(c, v);
}

int parse_query(struct_parse_context *c, struct_query *v) {
    assert(c != NULL && v != NULL);

    parse_first_line(c, &v->first);
    parse_newline(c);
//    parse_second_line();
//    parse_newline(c);
//    parse_third_line();
//    parse_newline(c);
//    parse_fourth_line();

    return PARSE_OK;
}

/*
Parse second part: number + queries of that number
NumberNewlinesQueries
*/
int parse_second_part(struct_query *v, const char *input) {
    assert(v != NULL && input != NULL);

    struct_parse_context c;
    init_struct_parse_context(&c, input);

//    parse_number();
    parse_first_line(&c, &v->first);
    parse_newline(&c);
//    parse_second_line();

    // check stack empty
    free_struct_parse_context(&c);

    return PARSE_OK;
}

#endif //LITE_DB_TEST_LITEDB_H

