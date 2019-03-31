//
// Created by ZiJie Wu on 2019-03-28.
//
/*
 * The entire Database assignment in one GIANT source file, just to make compile easier
 */
#ifndef LITE_DB_LITEDB_C
#define LITE_DB_LITEDB_C

#include <stdlib.h>
#include <assert.h>
#include <string.h>

#ifndef PARSE_STACK_INIT_SIZE
#define PARSE_STACK_INIT_SIZE 256
#endif

#define EXPECT(c, ch) do { assert(c->input[0] == (ch)); } while(0)
#define EXPECT_ALPHABET(c) do { \
assert(('a' <= c->input[0] && c->input[0] <= 'z') || ('A' <= c->input[0] && c->input[0] <= 'Z')); } while(0)

#define IS_ALPHABET_OR_NUMERIC(ch) ((ch) >= '0' && (ch) <= '9' || (ch) >= 'a' && (ch) <= 'z' || (ch) >= 'A' && (ch) <= 'Z')

//////////
// enum //
//////////

// this is the type of operations involved in the query predicates
// =, <, >
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
    // single char, represents the name of relation
    char relation;

    // string, represents the name of column
    char *column;

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
    // length of string, not including \0
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

typedef struct {
    struct_query *queries;
    size_t length;
} struct_queries;

// this struct stores information during parsing
typedef struct {
    const char *input;
    char *stack;
    size_t size, top;
} struct_parse_context;

/*
 * pop the stack by moving the top index of the stack down
 * must memcpy the memory because they may be occupied by future stack push
 */
static void *context_pop(struct_parse_context *c, size_t size) {
    assert(c->top >= size);
    c->top -= size;
    return c->stack + c->top;
}

/*
 * push the stack by moving the top index of the stack up
 * return a pointer to the allocated memory where the pushed object should store
 */
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

////////////////////////////
// Init & Free of structs //
////////////////////////////

static void free_struct_relation_column(struct_relation_column *rc) {
    assert(rc->column != NULL);

    rc->relation = '\0';

    free(rc->column);
    rc->column = NULL;

    rc->length_column = 0;
}

static void free_struct_first_line(struct_first_line *fl) {
    for (int i = 0; i < fl->length; i++) {
        free_struct_relation_column(&fl->sums[i]);
    }

    free(fl->sums);
    fl->sums = NULL;
    fl->length = 0;
}

static void free_struct_second_line(struct_second_line *sl) {
    assert(sl->length != 0);

    free(sl->relations);
    sl->relations = NULL;
    sl->length = 0;
}

static void free_struct_join(struct_join *join) {
    free_struct_relation_column(&join->lhs);
    free_struct_relation_column(&join->rhs);
}

static void free_struct_third_line(struct_third_line *tl) {
    assert(tl->length != 0);

    for (int i = 0; i < tl->length; i++) {
        free_struct_join(&tl->joins[i]);
    }

    free(tl->joins);
    tl->joins = NULL;
    tl->length = 0;
}

static void free_struct_predicate(struct_predicate *p) {
    free_struct_relation_column(&p->lhs);
}

static void free_struct_fourth_line(struct_fourth_line *fl) {
    if (fl->length == 0) {
        return;
    }

    for (int i = 0; i < fl->length; i++) {
        free_struct_predicate(&fl->predicates[i]);
    }

    free(fl->predicates);
    fl->predicates = NULL;
    fl->length = 0;
}

static void free_struct_query(struct_query *query) {
    free_struct_first_line(&query->first);
    free_struct_second_line(&query->second);
    free_struct_third_line(&query->third);
    free_struct_fourth_line(&query->fourth);
}

static void free_struct_queries(struct_queries *queries) {
    assert(queries->length != 0);

    for (int i = 0; i < queries->length; i++) {
        free_struct_query(&queries->queries[i]);
    }

    free(queries->queries);
    queries->queries = NULL;
    queries->length = 0;
}

static void init_struct_parse_context(struct_parse_context *c, const char *input) {
    c->input = input;
    c->stack = NULL;
    c->size = c->top = 0;
}

static void free_struct_parse_context(struct_parse_context *c) {
    assert(c->top == 0);

    free(c->stack);
    c->stack = NULL;
    c->top = c->size = 0;
}


///////////
// parse //
///////////

// skip one whitespace if there is one
void parse_whitespace(struct_parse_context *c) {
    while (c->input[0] == '\n' || c->input[0] == ' ') {
        c->input++;
    }
}

/*
 * Literal SELECT
 * Match string SELECT
 */
void parse_first_line_select(struct_parse_context *c) {
    assert(0 == strncmp(c->input, "SELECT", strlen("SELECT")));
    c->input += strlen("SELECT");
}

/*
 * Match Relation and Column
 * CFG:
 * relation.column
 */
int parse_relation_column(struct_parse_context *c, struct_relation_column *relation_column) {
    // Should start with alphabet
    EXPECT_ALPHABET(c);

    size_t head = c->top;
    const char *tmp_p = c->input;

    // parse relation
    while (1) {
        char ch = tmp_p[0];

        // end of relation
        if (ch == '.') {
            size_t len = c->top - head;
            const char *relation = context_pop(c, len);

            // copy this relation
            relation_column->relation = *relation;

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
        if (!IS_ALPHABET_OR_NUMERIC(ch)) {
            size_t len = c->top - head;
            const char *str = context_pop(c, len);
            // copy string
            relation_column->column = (char *) malloc(len + 1);
            memcpy(relation_column->column, str, len);
            relation_column->column[len] = '\0';
            relation_column->length_column = len;

            c->input = tmp_p;
            break;
        }

        *(char *) context_push(c, sizeof(char)) = ch;
        tmp_p++;
    }

    return PARSE_OK;
}

/*
 * Match relation and column
 * CFG:
 * SUM(relation.column)
 */
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
 * List of sums
 * CFG:
 * Sums -> Sum | Sum,WhitespaceSums
 */
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
 * First line of SQL query
 * CFG:
 * FirstLine -> SELECTWhitespaceSums
 */
int parse_first_line(struct_parse_context *c, struct_first_line *v) {
    // should start with SELECT
    EXPECT(c, 'S');

    parse_first_line_select(c);
    parse_whitespace(c);
    return parse_first_line_sums(c, v);
}

/*
 * Match the beginning of second line of SQL query
 * literal FROM
 */
void parse_second_line_from(struct_parse_context *c) {
    assert(0 == strncmp(c->input, "FROM", strlen("FROM")));
    c->input += strlen("FROM");
}

/*
 * Match the relation given on second line of SQL query
 * single alphabet
 */
int parse_second_line_relation(struct_parse_context *c, char *ch) {
    EXPECT_ALPHABET(c);

    // match single alphabet
    *ch = c->input[0];
    c->input++;

    return PARSE_OK;
}

/*
 * Match a list of relations
 * CFG:
 * Relation | Relation,WhitespaceRelations
 */
int parse_second_line_relations(struct_parse_context *c, struct_second_line *v) {
    EXPECT_ALPHABET(c);

    size_t head = c->top;

    char ch = 0;

    // parse first relation
    parse_second_line_relation(c, &ch);
    // push into stack
    *(char *) context_push(c, sizeof(char)) = ch;

    while (c->input[0] == ',') {
        c->input++;
        parse_whitespace(c);

        // parse following relation
        parse_second_line_relation(c, &ch);
        *(char *) context_push(c, sizeof(char)) = ch;
    }

    size_t len = c->top - head;
    const char *relations = context_pop(c, len);

    // len = number of alphabets
    v->relations = (char *) malloc(len + 1);
    memcpy(v->relations, relations, len);

    // add ending \0 to the string
    v->relations[len] = 0;

    // size of actual elements
    v->length = len / sizeof(char);

    return PARSE_OK;
}

/*
 * Match the second line of SQL query
 * CFG:
 * SecondLine -> FROMWhitespaceRelations
 */
int parse_second_line(struct_parse_context *c, struct_second_line *v) {
    // FROM
    EXPECT(c, 'F');

    parse_second_line_from(c);
    parse_whitespace(c);

    return parse_second_line_relations(c, v);
}

/*
 * Match literal WHERE at the beginning of third line of SQL query
 */
void parse_third_line_where(struct_parse_context *c) {
    assert(0 == strncmp(c->input, "WHERE", strlen("WHERE")));
    c->input += strlen("WHERE");
}

/*
 * Math JOIN
 * CFG:
 * Relation.ColumnWhitespace=WhitespaceRelation.Column
 */
int parse_third_line_join(struct_parse_context *c, struct_join *join) {
    EXPECT_ALPHABET(c);

    parse_relation_column(c, &join->lhs);

    parse_whitespace(c);

    // skip = sign
    EXPECT(c, '=');
    c->input++;

    parse_whitespace(c);

    parse_relation_column(c, &join->rhs);

    return PARSE_OK;
}

/*
 * Match literal AND
 */
void parse_and(struct_parse_context *c) {
    assert(0 == strncmp(c->input, "AND", strlen("AND")));
    c->input += strlen("AND");
}

/*
 * Match list of JOINs
 * CFG:
 * Join | JoinWhitespaceANDWhitespaceJoins
 */
int parse_third_line_joins(struct_parse_context *c, struct_third_line *tl) {
    EXPECT_ALPHABET(c);

    struct_join *join = NULL;

    size_t head = c->top;

    // first join
    join = malloc(sizeof(struct_join));
    parse_third_line_join(c, join);

    // push into context
    *(struct_join *) context_push(c, sizeof(struct_join)) = *join;
    free(join);

    // will end because third line ends with \n
    while (c->input[0] == ' ') {
        // skip ws AND ws
        parse_whitespace(c);
        parse_and(c);
        parse_whitespace(c);

        // following joins
        join = malloc(sizeof(struct_join));
        parse_third_line_join(c, join);

        // push into context
        *(struct_join *) context_push(c, sizeof(struct_join)) = *join;
        free(join);
    }

    size_t len = c->top - head;
    const struct_join *joins = context_pop(c, len);

    tl->joins = malloc(len);
    memcpy(tl->joins, joins, len);
    tl->length = len / sizeof(struct_join);

    return PARSE_OK;
}

/*
 * Match third line of SQL query
 * CFG:
 * WHEREWhitespaceJoins
 */
int parse_third_line(struct_parse_context *c, struct_third_line *v) {
    // WHERE
    EXPECT(c, 'W');

    parse_third_line_where(c);
    parse_whitespace(c);

    return parse_third_line_joins(c, v);
}

/*
 * Match comparision operator
 * =, > or <
 */
int parse_operator(struct_parse_context *c, enum_operator *op) {
    switch (c->input[0]) {
        case '=':
            *op = EQUAL;
            break;
        case '>':
            *op = GREATER_THAN;
            break;
        case '<':
            *op = LESS_THAN;
            break;
        default:
            assert(0);
            return PARSE_FAILED;
    }

    c->input++;

    return PARSE_OK;
}

/*
 * Match an Integer
 */
int parse_number(struct_parse_context *c, int *num) {
    char *tail = NULL;
    *num = strtol(c->input, &tail, 0);

    c->input = tail;

    return PARSE_OK;
}

/*
 * Match single predicate
 * CFG:
 * Relation.ColumnWhitespaceOperatorWhitespaceNumber
 */
int parse_fourth_line_predicate(struct_parse_context *c, struct_predicate *p) {
    EXPECT_ALPHABET(c);

    parse_relation_column(c, &p->lhs);

    // skip ws Op ws
    parse_whitespace(c);
    parse_operator(c, &p->operator);
    parse_whitespace(c);

    parse_number(c, &p->rhs);

    return PARSE_OK;
}

/*
 * Match one or more predicates
 * CFG:
 * Predicate | PredicateWhitespaceANDWhitespacePredicates
 */
int parse_fourth_line_predicates(struct_parse_context *c, struct_fourth_line *fl) {
    EXPECT_ALPHABET(c);

    struct_predicate *p = NULL;

    size_t head = c->top;

    // first predicate
    p = malloc(sizeof(struct_predicate));
    parse_fourth_line_predicate(c, p);

    // push into context
    *(struct_predicate *) context_push(c, sizeof(struct_predicate)) = *p;
    free(p);

    while (c->input[0] == ' ') {
        parse_whitespace(c);
        parse_and(c);
        parse_whitespace(c);

        // following predicates
        p = malloc(sizeof(struct_predicate));
        parse_fourth_line_predicate(c, p);

        // push into context
        *(struct_predicate *) context_push(c, sizeof(struct_predicate)) = *p;
        free(p);
    }

    size_t len = c->top - head;
    const struct_predicate *predicates = context_pop(c, len);

    fl->predicates = malloc(len);
    memcpy(fl->predicates, predicates, len);
    fl->length = len / sizeof(struct_predicate);

    return PARSE_OK;
}

/*
 * Match fourth line of SQL query
 * CFG:
 * ANDWhitespacePredicates; | ANDWhitespaces;
 */
int parse_fourth_line(struct_parse_context *c, struct_fourth_line *v) {
    // AND
    EXPECT(c, 'A');

    // AND
    parse_and(c);

    // whitespace
    parse_whitespace(c);

    // no predicates
    if (c->input[0] == ';') {
        c->input++;
        v->length = 0;
        v->predicates = NULL;
        return PARSE_OK;
    }

    // Predicates
    int ret = parse_fourth_line_predicates(c, v);

    // parse ';'
    assert(c->input[0] == ';');
    c->input++;

    return ret;
}

/*
 * Match one SQL query
 * CFG:
 * FirstLine Newline SecondLine Newline ThirdLine Newline FourthLine
 */
int parse_query(struct_parse_context *c, struct_query *v) {
    assert(c != NULL && v != NULL);

    parse_first_line(c, &v->first);
    parse_whitespace(c);
    parse_second_line(c, &v->second);
    parse_whitespace(c);
    parse_third_line(c, &v->third);
    parse_whitespace(c);
    parse_fourth_line(c, &v->fourth);
    parse_whitespace(c);

    return PARSE_OK;
}

/*
 * Match one or many queries, separated by newline
 * CFG:
 * Query | QueryNewlinesQueries
 */
int parse_queries(struct_parse_context *c, struct_queries *queries) {
    // begin with SELECT
    EXPECT(c, 'S');

    struct_query *query = NULL;

    size_t head = c->top;

    query = malloc(sizeof(struct_query));
    parse_query(c, query);

    // push into context
    *(struct_query *) context_push(c, sizeof(struct_query)) = *query;
    free(query);

    // THere are more queries
    while (c->input[0] == 'S') {
        parse_whitespace(c);

        query = malloc(sizeof(struct_query));
        parse_query(c, query);

        // push into context
        *(struct_query *) context_push(c, sizeof(struct_query)) = *query;
        free(query);
    }

    size_t len = c->top - head;
    const struct_query *tmp_queries = context_pop(c, len);

    queries->queries = malloc(len);
    memcpy(queries->queries, tmp_queries, len);
    queries->length = len / sizeof(struct_query);

    return PARSE_OK;
}

/*
 * Match second part of the input given to the program
 * CFG:
 * Number Newlines Queries
 */
int parse_second_part(struct_queries *queries, const char *input) {
    assert(queries != NULL && input != NULL);

    struct_parse_context c;
    init_struct_parse_context(&c, input);

    // expect number
    char head = c.input[0];
    assert('0' <= head && head <= '9');

    int count_query = 0;
    // we don't really need this number
    parse_number(&c, &count_query);
    parse_whitespace(&c);

    parse_queries(&c, queries);

    free_struct_parse_context(&c);
    return PARSE_OK;
}

void read_second_part_from_stdin(char **input) {
    assert(input != NULL);

    struct_parse_context c;

    // we only need the stack, so no input given
    init_struct_parse_context(&c, NULL);

    char *line = NULL;
    // no idea what is this for
    size_t size = 0;

    while (getline(&line, &size, stdin) != -1) {
        strncpy((char *) context_push(&c, strlen(line)), line, strlen(line));
    }
    free(line);
    line = NULL;

    size_t len = c.top;
    const char *tmp_input = context_pop(&c, len);

    *input = (char *) malloc(len + 1);
    memcpy(*input, tmp_input, len);
    (*input)[len] = 0;

    free_struct_parse_context(&c);
}

#endif //LITE_DB_LITEDB_C

