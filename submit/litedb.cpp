/*
 * Created by ZiJie Wu on 2019-03-28.
 * The entire Database assignment in one GIANT source file, just to make compiling easier
 *
 * It comprises of several components: data loader, parser
 */
#ifndef LITE_DB_LITEDB_C
#define LITE_DB_LITEDB_C

//#define DEBUG_PROFILING 1
//#define DEBUG_STACK_SIZE 1

#ifdef DEBUG_PROFILING
#include <time.h>
static long time_query = 0;
static long time_total = 0;
// number of times buffer hit during a query
static long count_buffer_hit_query = 0;
static long count_buffer_total_query = 0;
// number of times buffer hit throughout entire program
static long count_buffer_hit_total = 0;
static long count_buffer_total = 0;
#endif

/////////////////
// C++ library //
/////////////////
#include <unordered_map>
#include <unordered_set>
#include <vector>
#include <string>
#include <sstream>
#include <iostream>

#include <stdio.h>
#include <stdlib.h>
#include <assert.h>
#include <string.h>
#include <inttypes.h>
#include <ctype.h>

// runtime assert
#define ASSERT(val) \
do {\
    if (!(val)) {\
        fprintf(stderr, "%s:%d: runtime assert failed\n", __FILE__, __LINE__);\
        exit(99);\
    }\
} while(0)


/*
 _______
/       \
$$$$$$$  | ______    ______    _______   ______    ______
$$ |__$$ |/      \  /      \  /       | /      \  /      \
$$    $$/ $$$$$$  |/$$$$$$  |/$$$$$$$/ /$$$$$$  |/$$$$$$  |
$$$$$$$/  /    $$ |$$ |  $$/ $$      \ $$    $$ |$$ |  $$/
$$ |     /$$$$$$$ |$$ |       $$$$$$  |$$$$$$$$/ $$ |
$$ |     $$    $$ |$$ |      /     $$/ $$       |$$ |
$$/       $$$$$$$/ $$/       $$$$$$$/   $$$$$$$/ $$/
 */

#ifndef PARSE_STACK_INIT_SIZE
#define PARSE_STACK_INIT_SIZE 256
#endif

#define EXPECT(c, ch) do { ASSERT(c->input[0] == (ch)); } while(0)
#define EXPECT_ALPHABET(c) do { \
ASSERT(('a' <= c->input[0] && c->input[0] <= 'z') || ('A' <= c->input[0] && c->input[0] <= 'Z')); } while(0)

#define IS_ALPHABET_OR_NUMERIC(ch) ((ch) >= '0' && (ch) <= '9' || (ch) >= 'a' && (ch) <= 'z' || (ch) >= 'A' && (ch) <= 'Z')
#define IS_NUMERIC(ch) ((ch) >= '0' && (ch) <= '9')

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

typedef struct {
    // always inited with 26 element
    // 2D array of path to each csv file
    char **files;
    // number of files that are actually there
    size_t length;
} struct_input_files;

// SUM(D.c0)
typedef struct {
    // single char, represents the name of relation
    char relation;

    // int, represents the name of column
    int column;
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

// D.c3 < -9496
typedef struct {
    struct_relation_column lhs;
    enum_operator op;
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
    ASSERT(c->top >= size && size != 0);

    c->top -= size;
    return c->stack + c->top;
}

/*
 * push the stack by moving the top index of the stack up
 * return a pointer to the allocated memory where the pushed object should store
 */
static void *context_push(struct_parse_context *c, size_t size) {
    ASSERT(size > 0);

    void *ret;

    // expand 1.5 times if smaller than whats required
    if (c->top + size >= c->size) {
        if (c->size == 0) {
            c->size = PARSE_STACK_INIT_SIZE;
        }

        while (c->top + size >= c->size) {
            // expand the size to 1.5 times the original size
            // size += size / 2
            c->size += c->size >> 1;
#if DEBUG_STACK_SIZE
            printf("stack size: %zu KB\n", c->size / 1024);
#endif
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

static void init_struct_input_files(struct_input_files *files) {
    files->length = 0;

    files->files = (char **) malloc(26 * sizeof(char *));
    for (int i = 0; i < 26; i++) {
        files->files[i] = NULL;
    }
}

static void free_struct_input_files(struct_input_files *files) {
    for (int i = 0; i < files->length; i++) {
        free(files->files[i]);
    }
    free(files->files);
    files->files = NULL;
    files->length = 0;
}

static void free_struct_first_line(struct_first_line *fl) {
    free(fl->sums);
    fl->sums = NULL;
    fl->length = 0;
}

static void free_struct_second_line(struct_second_line *sl) {
    free(sl->relations);
    sl->relations = NULL;
    sl->length = 0;
}

static void free_struct_third_line(struct_third_line *tl) {
    free(tl->joins);
    tl->joins = NULL;
    tl->length = 0;
}

static void free_struct_fourth_line(struct_fourth_line *fl) {
    // this line may be empty
    if (fl->length == 0) {
        return;
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
    ASSERT(0 == strncmp(c->input, "SELECT", strlen("SELECT")));
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
            const char *relation = (char *) context_pop(c, len);

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
    // parse 'C' at the beginning of column
    c->input++;

    head = c->top;
    tmp_p = c->input;

    while (1) {
        char ch = tmp_p[0];

        // if its not number, then its end of column
        if (!IS_NUMERIC(ch)) {
            size_t len = c->top - head;
            // string of number, no terminal
            const char *str = (char *) context_pop(c, len);

            char *tmp_str = (char *) malloc(len + 1);
            memcpy(tmp_str, str, len);
            tmp_str[len] = '\0';

            // parse int
            relation_column->column = strtol(tmp_str, NULL, 0);

            // clean up
            free(tmp_str);

            // move head to this non numeric char
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
    const struct_relation_column *rcs = (struct_relation_column *) context_pop(c, len);

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
    ASSERT(0 == strncmp(c->input, "FROM", strlen("FROM")));
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
    const char *relations = (char *) context_pop(c, len);

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
    ASSERT(0 == strncmp(c->input, "WHERE", strlen("WHERE")));
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
    ASSERT(0 == strncmp(c->input, "AND", strlen("AND")));
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
    join = (struct_join *) malloc(sizeof(struct_join));
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
        join = (struct_join *) malloc(sizeof(struct_join));
        parse_third_line_join(c, join);

        // push into context
        *(struct_join *) context_push(c, sizeof(struct_join)) = *join;
        free(join);
    }

    size_t len = c->top - head;
    const struct_join *joins = (struct_join *) context_pop(c, len);

    tl->joins = (struct_join *) malloc(len);
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
            ASSERT(0);
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
    parse_operator(c, &p->op);
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
    p = (struct_predicate *) malloc(sizeof(struct_predicate));
    parse_fourth_line_predicate(c, p);

    // push into context
    *(struct_predicate *) context_push(c, sizeof(struct_predicate)) = *p;
    free(p);

    while (c->input[0] == ' ') {
        parse_whitespace(c);
        parse_and(c);
        parse_whitespace(c);

        // following predicates
        p = (struct_predicate *) malloc(sizeof(struct_predicate));
        parse_fourth_line_predicate(c, p);

        // push into context
        *(struct_predicate *) context_push(c, sizeof(struct_predicate)) = *p;
        free(p);
    }

    size_t len = c->top - head;
    const struct_predicate *predicates = (struct_predicate *) context_pop(c, len);

    fl->predicates = (struct_predicate *) malloc(len);
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
    // AND or ; (empty)
    ASSERT(c->input[0] == 'A' || c->input[0] == ';');

    if (c->input[0] == ';') {
        c->input++;

        v->predicates = NULL;
        v->length = 0;
        return PARSE_OK;
    }

    // AND
    parse_and(c);

    // whitespace
    parse_whitespace(c);

    // no predicates after AND
    if (c->input[0] == ';') {
        c->input++;
        v->length = 0;
        v->predicates = NULL;
        return PARSE_OK;
    }

    // Predicates
    int ret = parse_fourth_line_predicates(c, v);

    // parse ';'
    ASSERT(c->input[0] == ';');
    c->input++;

    return ret;
}

/*
 * Match one SQL query
 * CFG:
 * FirstLine Newline SecondLine Newline ThirdLine Newline FourthLine
 */
int parse_query(struct_parse_context *c, struct_query *v) {
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

    query = (struct_query *) malloc(sizeof(struct_query));
    parse_query(c, query);

    // push into context
    *(struct_query *) context_push(c, sizeof(struct_query)) = *query;
    free(query);

    // THere are more queries
    while (c->input[0] == 'S') {
        parse_whitespace(c);

        query = (struct_query *) malloc(sizeof(struct_query));
        parse_query(c, query);

        // push into context
        *(struct_query *) context_push(c, sizeof(struct_query)) = *query;
        free(query);
    }

    size_t len = c->top - head;
    const struct_query *tmp_queries = (struct_query *) context_pop(c, len);

    queries->queries = (struct_query *) malloc(len);
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
    ASSERT(queries != NULL && input != NULL);

    struct_parse_context c;
    init_struct_parse_context(&c, input);

    // expect number
    char head = c.input[0];
    ASSERT('0' <= head && head <= '9');

    int count_query = 0;
    // we don't really need this number
    parse_number(&c, &count_query);
    parse_whitespace(&c);

    parse_queries(&c, queries);

    free_struct_parse_context(&c);
    return PARSE_OK;
}

/**
 * CFG:
 * Path
 */
int parse_path(struct_parse_context *c, struct_input_files *files) {
    const char *point = c->input;

    while (!(*point == 0 || *point == '\n') && *point != ',') {
        point++;
    }

    char relation = *(point - 5);

    // copy string into files
    int length = point - c->input + 1;
    files->files[relation - 'A'] = (char *) malloc(length * sizeof(char));
    strncpy((files->files[relation - 'A']), c->input, length - 1);
    files->files[relation - 'A'][length - 1] = '\0';

    files->length++;

    // move input forward
    c->input = point;

    return PARSE_OK;
}

/**
 * CFG:
 * Path | Path,WhitespacesPaths
 */
int parse_paths(struct_parse_context *c, struct_input_files *files) {
    ASSERT(c->input[0] != 0);

    parse_path(c, files);
    parse_whitespace(c);

    // there is more
    while (c->input[0] == ',') {
        // parse ,
        c->input++;

        parse_whitespace(c);
        parse_path(c, files);
        parse_whitespace(c);
    }

    return PARSE_OK;
}

/**
 * Parse name of relations from input string
 */
int parse_first_part(struct_input_files *const files, const char *const input) {
    ASSERT(input != NULL);

    struct_parse_context c;
    init_struct_parse_context(&c, input);

    // parse paths
    parse_paths(&c, files);

    free_struct_parse_context(&c);
    return PARSE_OK;
}

/**
 * Read one line from stdin and assign it to *input
 * @param input: the pointer to the pointer to char array
 */
void read_first_part_from_stdin(char **input) {
    ASSERT(input != NULL);

    size_t size = 0;
    getline(input, &size, stdin);
}

/**
 * Read string from stdin and assign the value to *input
 * @param input: pointer to a pointer to the char array. The value will be modified by this function to point to a pointer of char array created by malloc, so please free it after use.
 */
void read_second_part_from_stdin(char **input) {
    ASSERT(input != NULL);

    struct_parse_context c;

    // we only need the stack, so no input given
    init_struct_parse_context(&c, NULL);

    char *line = NULL;
    // no idea what is this for
    size_t size = 0;

    // read first number
    getline(&line, &size, stdin);
    strncpy((char *) context_push(&c, strlen(line)), line, strlen(line));

    // how many queries are there
    int count = strtol(line, NULL, 0);

    // how many lines should I read
    // each query consists of 4 lines, and blank line in the middle
    count = count * 4 + count - 1;

    // read line by line, push them into stack
    while (count > 0) {
        getline(&line, &size, stdin);

        if (strlen(line) != 0) {
            strncpy((char *) context_push(&c, strlen(line)), line, strlen(line));
        }

        count--;
    }

    // free line
    free(line);
    line = NULL;

    size_t len = c.top;
    const char *tmp_input = (char *) context_pop(&c, len);

    // malloc memory for *input and copy the input to it
    *input = (char *) malloc(len + 1);
    memcpy(*input, tmp_input, len);
    (*input)[len] = 0;

    // convert to upper case
    for (int i = 0; i < len; i++) {
        (*input)[i] = toupper((*input)[i]);
    }

    free_struct_parse_context(&c);
}

/*
  ______               __                __
 /      \             /  |              /  |
/$$$$$$  |  ______   _$$ |_     ______  $$ |  ______    ______
$$ |  $$/  /      \ / $$   |   /      \ $$ | /      \  /      \
$$ |       $$$$$$  |$$$$$$/    $$$$$$  |$$ |/$$$$$$  |/$$$$$$  |
$$ |   __  /    $$ |  $$ | __  /    $$ |$$ |$$ |  $$ |$$ |  $$ |
$$ \__/  |/$$$$$$$ |  $$ |/  |/$$$$$$$ |$$ |$$ \__$$ |$$ \__$$ |
$$    $$/ $$    $$ |  $$  $$/ $$    $$ |$$ |$$    $$/ $$    $$ |
 $$$$$$/   $$$$$$$/    $$$$/   $$$$$$$/ $$/  $$$$$$/   $$$$$$$ |
                                                      /  \__$$ |
                                                      $$    $$/
                                                       $$$$$$/
 */

typedef struct {
    int min;
    int max;
    int unique;
} struct_meta_column;

/*
 _______               __                      __                                  __
/       \             /  |                    /  |                                /  |
$$$$$$$  |  ______   _$$ |_     ______        $$ |        ______    ______    ____$$ |  ______    ______
$$ |  $$ | /      \ / $$   |   /      \       $$ |       /      \  /      \  /    $$ | /      \  /      \
$$ |  $$ | $$$$$$  |$$$$$$/    $$$$$$  |      $$ |      /$$$$$$  | $$$$$$  |/$$$$$$$ |/$$$$$$  |/$$$$$$  |
$$ |  $$ | /    $$ |  $$ | __  /    $$ |      $$ |      $$ |  $$ | /    $$ |$$ |  $$ |$$    $$ |$$ |  $$/
$$ |__$$ |/$$$$$$$ |  $$ |/  |/$$$$$$$ |      $$ |_____ $$ \__$$ |/$$$$$$$ |$$ \__$$ |$$$$$$$$/ $$ |
$$    $$/ $$    $$ |  $$  $$/ $$    $$ |      $$       |$$    $$/ $$    $$ |$$    $$ |$$       |$$ |
$$$$$$$/   $$$$$$$/    $$$$/   $$$$$$$/       $$$$$$$$/  $$$$$$/   $$$$$$$/  $$$$$$$/  $$$$$$$/ $$/

Created @ http://patorjk.com/software/taag/#p=display&f=Big%20Money-sw&t=Data%20Loader
 */

#define EMPTY -1
#define LENGTH_FILE_NAME 16
#define SIZE_PAGE 4096
#define SIZE_BUFFER (2 * SIZE_PAGE)

/**
 * struct that stores intermediate table (after join, after predicates)
 *
 * It stores the name of relations that it represents
 * And list of index to rows in these relations
 */
typedef struct {
    /**
     * A 1D array of chars, they are relations that this struct represents
     */
    char *relations;

    /**
     * A 1D array of int, they are index to the rows of above relations
     *
     * The layout is something like:
     * 0 1 0 2 1 1
     *
     * AB
     * [0 1] [0 2] [1 1]
     *
     * Which means:
     *     the first row of this df is comprised of row 0 from A and row 1 from B.
     *     the second row of this df is comprised of row 0 from A and row 2 from B.
     *     and so on
     */
    int *index;

    /**
     * Number of rows in index
     *
     * num_row = len(index) / len(relations)
     */
    int num_row;
} struct_data_frame;

/**
 * Represents each binary file
 */
typedef struct {
    // ID of column buffered
    int column;

    // the entire column
    int *columns;
} struct_column;

/**
 * A struct that describe a relation
 */
typedef struct {
    // name of this file / relation
    char relation;

    /**
     * buffer of one column
     */
    struct_column column;

    /**
     * The struct to the .meta file
     */
    struct_meta_column *meta;

    // number of column and rows in the relation
    int num_col;
    int num_row;

    /**
     * This struct stores index of valid rows (for example, after select/filter) of the original relation
     * @nullable: if there is no predicate on this relation, or after filter, the relation is empty, then df will be NULL
     */
    struct_data_frame *df;
} struct_file;

typedef struct {
    // a dict of input files, represented in 2D array
    // key(index): name of relation, calculated by (relation - 'A')
    // value: pointer to struct_file
    struct_file *files;
    // length / number of relations
    size_t length;
} struct_files;

/*
 * This struct serves as a buffer when writing to disk
 */
typedef struct {
    char *buffer;
    size_t max_size;
    size_t cur_size;
} struct_fwrite_buffer;

/////////////////
// Init & Free //
/////////////////

void init_struct_data_frame_for_file(struct_file *file) {
    file->df = (struct_data_frame *) malloc(sizeof(struct_data_frame));

    file->df->relations = (char *) malloc(2 * sizeof(char));
    file->df->relations[0] = file->relation;
    file->df->relations[1] = '\0';

    // init index with index[i] = i where i = 0...number of rows
    // size = number of rows
    file->df->index = (int *) malloc(file->num_row * sizeof(int));
    file->df->num_row = file->num_row;

    for (int i = 0; i < file->df->num_row; i++) {
        file->df->index[i] = i;
    }
}

void copy_struct_data_frame(struct_data_frame *src, struct_data_frame *dst) {
    ASSERT(src != NULL && dst != NULL);

    size_t size_relation = (strlen(src->relations) + 1) * sizeof(*(src->relations));
    size_t size_index = (strlen(src->relations) * src->num_row) * sizeof(*(src->index));

    dst->relations = (char *) malloc(size_relation);
    memcpy(dst->relations, src->relations, size_relation);

    dst->index = (int *) malloc(size_index);
    memcpy(dst->index, src->index, size_index);

    dst->num_row = src->num_row;
}

void free_struct_data_frame(struct_data_frame *df) {
    if (df == NULL) {
        return;
    }

    if (df->relations != NULL) {
        free(df->relations);
    }

    if (df->relations != NULL) {
        free(df->index);
    }

    df->relations = NULL;
    df->index = NULL;
    df->num_row = 0;
}

void free_only_struct_data_frames(struct_files *files) {
    for (int i = 0; i < files->length; i++) {
        free_struct_data_frame(files->files[i].df);
        free(files->files[i].df);
        files->files[i].df = NULL;
    }
}

void init_struct_column(struct_column *file) {
    file->column = EMPTY;
    file->columns = NULL;
}

void free_struct_column(struct_column *file) {
    if (file->columns != NULL) {
        free(file->columns);
        file->columns = NULL;
    }
    file->column = EMPTY;
}

void init_struct_file(struct_file *file) {
    file->relation = '\0';
    file->num_col = 0;
    file->num_row = 0;
    file->df = NULL;

    init_struct_column(&file->column);
    file->meta = NULL;
}

void free_struct_file(struct_file *file) {
    file->relation = '\0';

    file->num_col = 0;
    file->num_row = 0;

    if (file->df != NULL) {
        free_struct_data_frame(file->df);
        free(file->df);
        file->df = NULL;
    }

    free_struct_column(&file->column);
    free(file->meta);
}

void init_struct_files(struct_files *files, int length) {
    files->files = (struct_file *) malloc(length * sizeof(struct_file));
    files->length = length;

    for (int i = 0; i < length; i++) {
        init_struct_file(&files->files[i]);
    }
}

void free_struct_files(struct_files *files) {
    for (int i = 0; i < files->length; i++) {
        free_struct_file(&files->files[i]);
    }

    free(files->files);
    files->length = 0;
}

/**
 * Initilize the buffer, allocate memory
 *
 * @param buffer
 * @param max_size: maximum size of the buffer, in bytes
 */
void init_struct_fwrite_buffer(struct_fwrite_buffer *buffer, size_t max_size) {
    buffer->buffer = (char *) malloc(max_size);
    memset(buffer->buffer, 0, max_size);
    buffer->max_size = max_size;
    buffer->cur_size = 0;
}

/**
 * Free the buffer's memory
 */
void free_struct_fwrite_buffer(struct_fwrite_buffer *buffer) {
    free(buffer->buffer);
    buffer->buffer = NULL;
    buffer->max_size = 0;
    buffer->cur_size = 0;
}

/////////////////////
// Dataloader Core //
/////////////////////
void get_name_file_column(char relation, int column, char *file_name) {
    sprintf(file_name, "%c%d.binary", relation, column);
}

/**
 * write content to output buffer
 * The actual file will be write if buffer is almost full
 */
void fwrite_buffered(void *buffer, size_t size, size_t count, FILE *stream, struct_fwrite_buffer *manual_buffer) {
    size_t size_buffer = size * count;

    // manual buffer is full
    if (manual_buffer->cur_size == manual_buffer->max_size) {
        // we write the entire block into disk, so it's page aligned
        fwrite(manual_buffer->buffer, sizeof(*manual_buffer->buffer), manual_buffer->max_size, stream);

        // manual buffer is now empty
        manual_buffer->cur_size = 0;
    }

    // write into manual buffer
    memcpy(manual_buffer->buffer + manual_buffer->cur_size, buffer, size_buffer);
    manual_buffer->cur_size += size_buffer;
}

/**
 * Write whats in the buffer and empty the buffer (but not freeing the memory)
 */
void fwrite_buffered_flush(struct_fwrite_buffer *manual_buffer, FILE *stream) {
    if (manual_buffer->cur_size == 0) {
        return;
    }

    // write entire buffer into disk, so it's page aligned
    fwrite(manual_buffer->buffer, sizeof(*manual_buffer->buffer), manual_buffer->cur_size, stream);

    // buffer is now empty
    manual_buffer->cur_size = 0;
}

const int *const select_column_from_file(struct_file *const file, const int column) {
    // buffer hit
    if (file->column.column == column) {
        return file->column.columns;
    }

    assert(file->column.columns != NULL);

    file->column.column = column;

    char path_file[LENGTH_FILE_NAME] = {'\0'};
    get_name_file_column(file->relation, column, path_file);

    FILE *file_column = fopen(path_file, "rb");
    fseek(file_column, 0, SEEK_END);
    size_t file_size = ftell(file_column);
    fseek(file_column, 0, SEEK_SET);

    size_t size_read = fread(file->column.columns, 1, file_size, file_column);
    assert(size_read == file_size);

    fclose(file_column);
    return file->column.columns;
}

/**
 * Find out how many columns are there by counting number of , in the first line
 * @param buffer: buffer of the file from disk
 * @param size: size of this buffer (in bytes)
 * @return number of column
 */
int get_num_col(const char *const buffer, size_t size) {
    int count = 0;
    int cursor = 0;

    while (cursor < size && buffer[cursor] != '\n') {
        if (buffer[cursor] == ',') {
            count++;
        }

        cursor++;
    }

    return count + 1;
}

/**
 * Read csv file from disk and convert them into a more efficient format, then write back to disk
 *
 * What does it do:
 * Read contents from csv file, convert string of number into signed 32 bit representation and write (x.binary) back to disk
 *
 * Also, gather metadata about each file:
 * For each column: min value, max value, number of unique value
 *
 * @param relation: name of the relation
 * @param file: path to the file on the disk
 * @param loaded_file: struct describing the loaded file
 */
void load_csv_file(char relation, char *path_file_csv, struct_file *loaded_file) {
    FILE *file_csv = fopen(path_file_csv, "r");
    // array of file*
    FILE **files_column = NULL;

    // meta data for each column
    struct_meta_column *meta = NULL;

    /////////////
    // Buffers //
    /////////////
    char file_name[LENGTH_FILE_NAME] = {'\0'};

    // main buffer to read char from file
    char *buffer = (char *) malloc(SIZE_BUFFER);
    memset(buffer, 0, SIZE_BUFFER);

    char secondary_buffer[64] = {'\0'};
    // length of content stored in the buffer
    int size_secondary_buffer = 0;

    // one fwrite_buffer for each column
    struct_fwrite_buffer *fwrite_buffers = NULL;

    int num_col = EMPTY;
    int num_count = 0;

    while (1) {
        int size_buffer = fread(buffer, sizeof(buffer[0]), SIZE_BUFFER, file_csv);

        if (size_buffer == 0) {
            break;
        }

        // only execute once at the beginning
        if (num_col == EMPTY) {
            num_col = get_num_col(buffer, size_buffer);

            // then init output files
            files_column = (FILE **) malloc(num_col * sizeof(FILE *));
            // init meta data
            meta = (struct_meta_column *) malloc(num_col * sizeof(struct_meta_column));
            fwrite_buffers = (struct_fwrite_buffer *) malloc(num_col * sizeof(struct_fwrite_buffer));

            for (int i = 0; i < num_col; i++) {
                files_column[i] = NULL;
                // open file
                get_name_file_column(relation, i, file_name);

                files_column[i] = fopen(file_name, "wb");
                assert(files_column[i] != NULL);

                // init meta data
                meta[i].max = INT32_MIN;
                meta[i].min = INT32_MAX;
                meta[i].unique = -1;

                // init buffer for each column
                init_struct_fwrite_buffer(&fwrite_buffers[i], SIZE_BUFFER);
            }
        }

        int cursor = 0;
        int cursor_prev = 0;

        while (cursor < size_buffer) {
            char current = buffer[cursor];

            // we find a new number
            if (current == ',' || current == '\n') {
                num_count++;
                int number = 0;

                if (size_secondary_buffer == 0) {
                    // simply strtol from buffer
                    // the number to be read will always be valid because it will end with non-numeric char
                    number = strtol(&buffer[cursor_prev], NULL, 0);
                } else {
                    // this will always occur at the beginning of processing a new buffer
                    assert(cursor_prev == 0);

                    // copy the beginning of buffer (until cursor) to the end of secondary buffer
                    memcpy(secondary_buffer + size_secondary_buffer, buffer, cursor);
                    size_secondary_buffer += cursor;
                    secondary_buffer[size_secondary_buffer] = '\0';

                    number = strtol(secondary_buffer, NULL, 0);

                    size_secondary_buffer = 0;
                }

                // move cursor to the beginning of next number
                cursor += 1;
                cursor_prev = cursor;

                // store this number to column buffer
                // which column is this number in?
                int col = (num_count - 1) % num_col;

                // write number to buffer column
                fwrite_buffered(&number, sizeof(number), 1, files_column[col], &fwrite_buffers[col]);

                // update meta data
                if (meta[col].max < number) {
                    meta[col].max = number;
                }

                if (meta[col].min > number) {
                    meta[col].min = number;
                }
            } else {
                // this current char is part of the number, skip it
                cursor++;
            }
        }

        // we have read the entire buffer, now copy whats left into secondary buffer
        // only if there is anything left in the main buffer
        if (cursor_prev < size_buffer) {
            // always happen at the end of a buffer
            int length = size_buffer - cursor_prev;

            // copy whats left of buffer into secondary buffer
            memcpy(secondary_buffer, buffer + cursor_prev, length);
            size_secondary_buffer = length;
        }
    }

    assert(size_secondary_buffer == 0);

    int num_row = num_count / num_col;

    for (int i = 0; i < num_col; i++) {
        // write whats left inside output buffer to file
        fwrite_buffered_flush(&fwrite_buffers[i], files_column[i]);

        // calculate unique number for each column
        meta[i].unique = num_row < meta[i].max - meta[i].min ? num_row : meta[i].max - meta[i].min;
    }

    loaded_file->relation = relation;
    loaded_file->num_col = num_col;
    loaded_file->num_row = num_row;
    loaded_file->column.columns = (int *) malloc(loaded_file->num_row * sizeof(int));
    loaded_file->meta = meta;

    /////////////
    // cleanup //
    /////////////
    fclose(file_csv);

    for (int i = 0; i < num_col; i++) {
        fclose(files_column[i]);
        free_struct_fwrite_buffer(&fwrite_buffers[i]);
    }
    free(files_column);
    free(fwrite_buffers);

    free(buffer);
}

/**
 * Given the input files, load them into memory
 * @param files
 */
void load_csv_files(struct_input_files *path_files, struct_files *loaded_files) {
    // init loaded_files
    init_struct_files(loaded_files, path_files->length);

    // read each file
    for (int i = 0; i < path_files->length; i++) {
        load_csv_file((char) (i + 'A'), path_files->files[i], &loaded_files->files[i]);
    }

    // don't free struct_files
}

/*
  ______               __      __                __
 /      \             /  |    /  |              /  |
/$$$$$$  |  ______   _$$ |_   $$/  _____  ____  $$/  ________   ______    ______
$$ |  $$ | /      \ / $$   |  /  |/     \/    \ /  |/        | /      \  /      \
$$ |  $$ |/$$$$$$  |$$$$$$/   $$ |$$$$$$ $$$$  |$$ |$$$$$$$$/ /$$$$$$  |/$$$$$$  |
$$ |  $$ |$$ |  $$ |  $$ | __ $$ |$$ | $$ | $$ |$$ |  /  $$/  $$    $$ |$$ |  $$/
$$ \__$$ |$$ |__$$ |  $$ |/  |$$ |$$ | $$ | $$ |$$ | /$$$$/__ $$$$$$$$/ $$ |
$$    $$/ $$    $$/   $$  $$/ $$ |$$ | $$ | $$ |$$ |/$$      |$$       |$$ |
 $$$$$$/  $$$$$$$/     $$$$/  $$/ $$/  $$/  $$/ $$/ $$$$$$$$/  $$$$$$$/ $$/
          $$ |
          $$ |
          $$/
 */

const std::string vector_to_string(const std::vector<int> &v) {
    std::stringstream ss;
    for (auto it = v.begin(); it != v.end(); it++) {
        ss << *it << '-';
    }

    return ss.str();
}

/**
 * Calculate the cost of join r + order and order + r, return the one with smaller cost
 *
 * @param prev_order: order of previsou join clauses
 * @param join: index to the join clause
 * @param plan: where we put the order after this join
 * @return cost of this join order, INT32_MAX if impossible to join
 */
int cost(const std::vector<int> &prev_order,
         const int join,
         std::vector<int> &plan,
         const std::vector<struct_join *> &joins) {

    // todo: if first join, it's true

    ///////////////////////////////
    // check if join is possible //
    ///////////////////////////////
    // the join clause we want to evaluate
    const struct_join *new_join = joins[join];

    //////////////
    // join left?
    /////////////
    // first join in joins should be able to join with join
    bool can_join_left = false;
    std::unordered_set<char> sets;
    sets.insert(new_join->lhs.relation);
    sets.insert(new_join->rhs.relation);

    // check if the first join in the previous join order is inside the set
    const struct_join *first = joins[prev_order[0]];

    if (sets.find(first->lhs.relation) != sets.end()
        || sets.find(first->rhs.relation) != sets.end()) {
        can_join_left = true;
    }

    ////////////////
    // join right?
    ///////////////
    // either lhs or rhs of join should be in the joining tree
    bool can_join_right = false;

    // add all joined relations to the set
    sets.clear();
    for (auto i:prev_order) {
        sets.insert(joins[i]->lhs.relation);
        sets.insert(joins[i]->rhs.relation);
    }

    // check if right join is possible
    if (sets.find(new_join->lhs.relation) != sets.end()
        || sets.find(new_join->rhs.relation) != sets.end()) {
        can_join_right = true;
    }

    // this order of join is impossible
    if (!(can_join_left || can_join_right)) {
        return INT32_MAX;
    }

    /////////////////////////////////////////////////////////
    // calculate new cost for join orders that are possible
    /////////////////////////////////////////////////////////
    int cost = INT32_MAX;
    // can join left, calculate cost
    if (can_join_left) {

    }

    // can join right, calculate cost
    if (can_join_right) {

    }

    // todo: update plan
    plan.clear();
    if (can_join_left) {
        plan.push_back(join);
        plan.insert(plan.begin() + 1, prev_order.begin(), prev_order.end());
    } else if (can_join_right) {
        plan.insert(plan.begin(), prev_order.begin(), prev_order.end());
        plan.push_back(join);
    }

    return cost;
}

/**
 * Compute the best join order, given join clauses as rels
 *
 * @param rels: index of join clauses to join, should be sorted, ascending
 * @param best: map from joins to the best order of joining them
 * @param joins: join clauses
 * @return best join order of rels
 */
std::vector<int> compute_best(const std::vector<int> &rels,
                              std::unordered_map<std::string, std::vector<int>> &best,
                              const std::vector<struct_join *> &joins) {
    auto key = vector_to_string(rels);
    if (best.find(key) != best.end()) {
        return best[key];
    }

    // current join plan
    std::vector<int> curr_plan;
    int curr_cost = INT32_MAX;

    // tmp vector
    std::vector<int> plan;
    for (int i = 0; i < rels.size(); i++) {
        // make a copy so we can modify
        std::vector<int> tmp(rels);
        // delete tmp[i]
        tmp.erase(tmp.begin() + i);

        // recursivly get the sub join order
        auto internal_order = compute_best(tmp, best, joins);

        if (internal_order.empty()) {
            // no join possible
            continue;
        }

        // check [r] + order || order + [r]
        // todo: join itself
        auto plan_cost = cost(internal_order, rels[i], plan, joins);

        if (plan_cost <= curr_cost) {
            curr_plan = plan;
            curr_cost = plan_cost;
        }
    }

    // todo: what if there is no join possible
    best[key] = curr_plan;

    return curr_plan;
}

/**
 * Re-order the joins
 */
// todo
void optimize_joins(struct_files *const files, struct_query *const query) {
    // init joins and rels
    std::vector<int> rels;
    std::vector<struct_join *> joins;
    for (int i = 0; i < query->third.length; i++) {
        rels.push_back(i);
        joins.push_back(&query->third.joins[i]);
    }

    // init best
    std::vector<int> order;
    std::unordered_map<std::string, std::vector<int>> best;
    for (int i = 0; i < joins.size(); i++) {
        order.clear();
        order.push_back(i);
        auto tmp = vector_to_string(order);

        best[tmp] = order;
    }
    
    auto best_join_order = compute_best(rels, best, joins);
    
    // apply this order to query->third
    auto third = query->third.joins;

    // make a copy of joins
    std::vector<struct_join> tmp_joins;
    for (auto each: joins) {
        tmp_joins.push_back(*each);
    }

    for (int i = 0; i < best_join_order.size(); i++) {
        third[i] = tmp_joins[best_join_order[i]];
    }
}

/*
 ________                                            __      __                            ________                      __
/        |                                          /  |    /  |                          /        |                    /  |
$$$$$$$$/  __    __   ______    _______  __    __  _$$ |_   $$/   ______   _______        $$$$$$$$/  _______    ______  $$/  _______    ______
$$ |__    /  \  /  | /      \  /       |/  |  /  |/ $$   |  /  | /      \ /       \       $$ |__    /       \  /      \ /  |/       \  /      \
$$    |   $$  \/$$/ /$$$$$$  |/$$$$$$$/ $$ |  $$ |$$$$$$/   $$ |/$$$$$$  |$$$$$$$  |      $$    |   $$$$$$$  |/$$$$$$  |$$ |$$$$$$$  |/$$$$$$  |
$$$$$/     $$  $$<  $$    $$ |$$ |      $$ |  $$ |  $$ | __ $$ |$$ |  $$ |$$ |  $$ |      $$$$$/    $$ |  $$ |$$ |  $$ |$$ |$$ |  $$ |$$    $$ |
$$ |_____  /$$$$  \ $$$$$$$$/ $$ \_____ $$ \__$$ |  $$ |/  |$$ |$$ \__$$ |$$ |  $$ |      $$ |_____ $$ |  $$ |$$ \__$$ |$$ |$$ |  $$ |$$$$$$$$/
$$       |/$$/ $$  |$$       |$$       |$$    $$/   $$  $$/ $$ |$$    $$/ $$ |  $$ |      $$       |$$ |  $$ |$$    $$ |$$ |$$ |  $$ |$$       |
$$$$$$$$/ $$/   $$/  $$$$$$$/  $$$$$$$/  $$$$$$/     $$$$/  $$/  $$$$$$/  $$/   $$/       $$$$$$$$/ $$/   $$/  $$$$$$$ |$$/ $$/   $$/  $$$$$$$/
                                                                                                              /  \__$$ |
                                                                                                              $$    $$/
                                                                                                               $$$$$$/
 */

/**
 * This struct is used to store number and the index of row they are in
 */
typedef struct {
    // the number at column
    int number;
    // the index or row in dataframe.index
    int row;
} struct_number_row;

int cmp_struct_number_row_qsort(const void *p1, const void *p2) {
    const struct_number_row *a = (const struct_number_row *) p1;
    const struct_number_row *b = (const struct_number_row *) p2;

    if (a->number != b->number) {
        if (a->number < b->number) {
            return -1;
        } else {
            return 1;
        }
    }

    if (a->row < b->row) {
        return -1;
    } else if (a->row > b->row) {
        return 1;
    } else {
        return 0;
    }
}

int cmp_struct_number_row_bsearch(const void *p1, const void *p2) {
    const struct_number_row *a = (const struct_number_row *) p1;
    const struct_number_row *b = (const struct_number_row *) p2;

    if (a->number < b->number) {
        return -1;
    } else if (a->number > b->number) {
        return 1;
    } else {
        return 0;
    }
}

int findIndexOf(const char *const input, int length, char val) {
    for (int i = 0; i < length; i++) {
        if (input[i] == val) {
            return i;
        }
    }

    return -1;
}

/**
 * Filter data in the relation, given predicate like A.c3 < 7666
 * And create filered index for input file
 *
 * @param file
 * @param predicate
 */
void filter_data_given_predicate(struct_file *file, const struct_predicate *const predicate) {
    ASSERT(file->relation == predicate->lhs.relation);

    // empty file, do nothing
    if (file->num_row == 0) {
        return;
    }

    // if dataframe is NULL, create dataframe
    if (file->df == NULL) {
        init_struct_data_frame_for_file(file);
    }

    // empty data frame, do nothing
    if (file->df->num_row == 0) {
        return;
    }

    struct_data_frame *const df = file->df;
    const int row = df->num_row;

    // pointer to file->df->index
    size_t slow = 0;
    // pointer to file->df->index
    size_t fast = 0;

    // index of column
    int column = predicate->lhs.column;
    // the number @ given column
    int number = 0;
    int shouldKeep = 0;

    const int *const columns = select_column_from_file(file, column);

    // for each row
    for (; fast < row; fast++) {
        shouldKeep = 0;
        // get the number to be compared
        number = columns[df->index[fast]];

        switch (predicate->op) {
            case EQUAL:
                if (number == predicate->rhs) {
                    shouldKeep = 1;
                }
                break;
            case LESS_THAN:
                if (number < predicate->rhs) {
                    shouldKeep = 1;
                }
                break;
            case GREATER_THAN:
                if (number > predicate->rhs) {
                    shouldKeep = 1;
                }
                break;
            default:
                fprintf(stderr, "Invalid operator");
                break;
        }

        // if met, copy row @ fast to row @ slow, slow++
        if (shouldKeep == 0) {
            continue;
        }

        // copy index of row
        df->index[slow] = df->index[fast];
        slow++;
    }

    // update row of df
    df->num_row = slow;

    // if no rows selected, empty the index
    if (df->num_row == 0) {
        free(df->index);
        df->index = NULL;
    } else {
        // realloc memory for df->index
        df->index = (int *) realloc(df->index, df->num_row * sizeof(int));
    }
}

/**
 * (Left deep) join two columns(represented by data frame) from two relation
 *
 * @param loaded_files
 * @param intermediate
 * @param relation
 * @param join
 */
//todo: buffer outer loop
void sorted_nested_loop_join(const struct_files *const loaded_files,
                             struct_data_frame *const intermediate,
                             struct_file *const relation,
                             const struct_join *join) {
    ASSERT(loaded_files != NULL && intermediate != NULL && relation != NULL && join != NULL);

    ///////////////////////////////////
    // The new relations after join //
    //////////////////////////////////
    // first 1: size of relation
    // number of relations, or number of index per row, of intermediate dataframe
    int num_relations_before = strlen(intermediate->relations);

    // second 1: make room for ending \0
    size_t length_joined_relations = num_relations_before + 1 + 1;
    // the name of the new relations, remeber to free the one from inter and assign this to it
    char *relations_joined = (char *) malloc(length_joined_relations * sizeof(char));
    // assign value
    memcpy(relations_joined, intermediate->relations, num_relations_before);
    relations_joined[length_joined_relations - 2] = relation->relation;
    relations_joined[length_joined_relations - 1] = '\0';

    // check if one of the df is empty, and don't init if isn't empty
    // Init df for right hand side file
    int is_right_df_null = 0;
    if (relation->df == NULL) {
        is_right_df_null = 1;
    }

    /////////////////////////////////////////
    // Stack for temp storing join results //
    ////////////////////////////////////////
    // we use the stack again, to store join results (index of rows)
    struct_parse_context c;
    init_struct_parse_context(&c, NULL);

    int offset_column_left = findIndexOf(intermediate->relations,
                                         num_relations_before,
                                         join->lhs.relation);

    //////////////////
    // read columns //
    //////////////////
    struct_file *const file_left = loaded_files->files + (join->lhs.relation - 'A');
    struct_file *const file_right = loaded_files->files + (join->rhs.relation - 'A');
    const int *const column_left = select_column_from_file(file_left, join->lhs.column);
    const int *const column_right = select_column_from_file(file_right, join->rhs.column);

    ///////////////////////////
    // Buffer for outer loop //
    ///////////////////////////
    // todo: mem full: since a entire column can always fit in memeory, we read them in and sort
    // buffer numbers in a column as a pair (number, row), sort buffer by number
    int length_buffer_outer_loop = intermediate->num_row;
    struct_number_row *buffer_outer_loop = (struct_number_row *) malloc(
            length_buffer_outer_loop * sizeof(struct_number_row));

    // fill buffer with (number, index in df.index)
    for (int i = 0; i < length_buffer_outer_loop; i++) {
        buffer_outer_loop[i].row = i;
        buffer_outer_loop[i].number = column_left[
                intermediate->index[i * num_relations_before + offset_column_left]
        ];
    }

    // qsort
    qsort(buffer_outer_loop, length_buffer_outer_loop, sizeof(struct_number_row), cmp_struct_number_row_qsort);

    // inner loop
    // binary search the each number from the right relation in the left relation
    for (int row_relation = 0;
         row_relation < (is_right_df_null ? relation->num_row : relation->df->num_row);
         row_relation++) {
        const int number_right = is_right_df_null ? column_right[row_relation]
                                                  : column_right[relation->df->index[row_relation]];

        struct_number_row key;
        key.number = number_right;

        struct_number_row *res = (struct_number_row *) bsearch(
                &key, buffer_outer_loop,
                length_buffer_outer_loop,
                sizeof(struct_number_row),
                cmp_struct_number_row_bsearch);

        if (res == NULL) {
            continue;
        }

        int i = res - buffer_outer_loop;

        // todo: make this efficient
        // move i to the first duplicate element
        while (i > 0 && buffer_outer_loop[i].number == buffer_outer_loop[i - 1].number) {
            i--;
        }

        // loop through buffer
        // i = index of elements that number == number_right
        for (; (i < length_buffer_outer_loop && buffer_outer_loop[i].number == number_right);
               i++) {
            // push this row (based on original file) into stack
            // copy index[row_inter] from inter, and concat it with index[row_relation]
            size_t size_to_copy = num_relations_before * sizeof(int);

            memcpy(context_push(&c, size_to_copy),
                   &(intermediate->index[buffer_outer_loop[i].row * num_relations_before]),
                   size_to_copy);

            *(int *) context_push(&c, sizeof(int)) = is_right_df_null ? row_relation
                                                                      : relation->df->index[row_relation];
        }
    }

    /////////////
    // cleanup //
    ////////////
    free(intermediate->relations);
    free(intermediate->index);

    intermediate->relations = relations_joined;

    // copy index
    // size of index, in bytes
    size_t top = c.top;

    if (top == 0) {
        intermediate->index = NULL;
        intermediate->num_row = 0;
        free_struct_parse_context(&c);
    } else {
        // directly use the stack's memory, without creating new space and copying
        int *tmp_index = (int *) context_pop(&c, top);
        // manually empty c
        c.top = 0;
        c.size = 0;
        c.stack = NULL;

        intermediate->index = (int *) realloc(tmp_index, top);

        // bytes / sizeof int / number per row = num of row
        intermediate->num_row = top / strlen(intermediate->relations) / sizeof(int);
    }

    // don't free struct_parser
    free(buffer_outer_loop);
}

void sorted_nested_loop_join_both_joined_before(const struct_files *const loaded_files,
                                                struct_data_frame *const intermediate,
                                                const struct_join *join) {
    ASSERT(loaded_files != NULL && intermediate != NULL && join != NULL);

    // we use the stack again
    struct_parse_context c;
    init_struct_parse_context(&c, NULL);

    int num_relations = strlen(intermediate->relations);

    int offset_lhs = findIndexOf(intermediate->relations,
                                 num_relations,
                                 join->lhs.relation);
    int offset_rhs = findIndexOf(intermediate->relations,
                                 num_relations,
                                 join->rhs.relation);

    struct_file *const file_left = loaded_files->files + (join->lhs.relation - 'A');
    struct_file *const file_right = loaded_files->files + (join->rhs.relation - 'A');
    const int *const column_left = select_column_from_file(file_left, join->lhs.column);
    const int *const column_right = select_column_from_file(file_right, join->rhs.column);


    // for each row in inter
    for (int i = 0; i < intermediate->num_row; i++) {
        const int *const row_index = &intermediate->index[i * num_relations];

        int number_left = column_left[row_index[offset_lhs]];
        int number_right = column_right[row_index[offset_rhs]];

        if (number_left == number_right) {
            size_t size_row = num_relations * sizeof(int);

            memcpy(context_push(&c, size_row),
                   row_index,
                   size_row);
        }
    }

    // re-assign index
    free(intermediate->index);

    // copy index
    // size of index, in bytes
    size_t top = c.top;
    if (top == 0) {
        intermediate->index = NULL;
        intermediate->num_row = 0;
    } else {
        // memcpy
        int *tmp_index = (int *) context_pop(&c, top);
        intermediate->index = (int *) malloc(top);
        memcpy(intermediate->index, tmp_index, top);

        // bytes / sizeof int / number per row = num of row
        intermediate->num_row = top / num_relations / sizeof(int);
    }

    free_struct_parse_context(&c);
}

/**
 * Execute the fourth line of SQL query
 *
 * @param loaded_file
 * @param fl
 */
void execute_selects(struct_files *const loaded_file, const struct_fourth_line *const fl) {
    for (int i = 0; i < fl->length; i++) {
        const struct_predicate *const predicate = &fl->predicates[i];
        char relation = predicate->lhs.relation;

        filter_data_given_predicate(&loaded_file->files[relation - 'A'], predicate);
    }
}


/**
 * Execute all the joins, and assign the result to *result
 *
 * @param loaded_file
 * @param tl
 * @param result
 */
void execute_joins(struct_files *const loaded_file, const struct_third_line *const tl, struct_data_frame *inter) {
    if (tl->length == 0) {
        ASSERT(0);
        return;
    }

    char first_relation = tl->joins[0].lhs.relation;
    struct_file *first_file = &loaded_file->files[first_relation - 'A'];

    if (first_file->df == NULL) {
        init_struct_data_frame_for_file(first_file);
    }

    copy_struct_data_frame(first_file->df, inter);

    // check if join valid
    for (int i = 0; i < tl->length; i++) {
        struct_join *join = &tl->joins[i];

        const int index_left = findIndexOf(inter->relations, strlen(inter->relations), join->lhs.relation);
        const int index_right = findIndexOf(inter->relations, strlen(inter->relations), join->rhs.relation);

        // if both joined before
        if (-1 != index_left && -1 != index_right) {

            sorted_nested_loop_join_both_joined_before(loaded_file, inter, join);
            continue;
        }

        // else if one of them is not joined before
        // we swap the order of join to make sure the lhs one is joined before
        if (-1 == index_left) {
            // swap join lhs and rhs
            struct_relation_column tmp = join->lhs;
            join->lhs = join->rhs;
            join->rhs = tmp;
        }

        struct_file *rhs_file = &loaded_file->files[join->rhs.relation - 'A'];

        sorted_nested_loop_join(loaded_file, inter, rhs_file, join);
    }
}

/**
 * Execute sums for each column
 *
 * @param loaded_file
 * @param fl
 * @param result
 * @param ans
 */
void execute_sums(struct_files *const loaded_file,
                  const struct_first_line *const fl,
                  struct_data_frame *result,
                  int64_t *ans) {
    int num_relations = strlen(result->relations);

    for (int col = 0; col < fl->length; col++) {
        // the sum query
        struct_relation_column *rc = &fl->sums[col];
        char relation = rc->relation;

        // find the index of this relation in the df
        int offset = findIndexOf(result->relations, num_relations, relation);

        // get the relation where this column in
        struct_file *file = &loaded_file->files[relation - 'A'];

        const int *const columns = select_column_from_file(file, rc->column);

        for (int i = 0; i < result->num_row; i++) {
            ans[col] += columns[result->index[i * num_relations + offset]];
        }
    }
}

/**
 * Execute the query
 *
 * 1. select/filter first
 * 2. then join
 * 3. then SUM
 *
 * @param query
 */
void execute(struct_files *const loaded_file, struct_query *const query) {
#ifdef DEBUG_PROFILING
    time_query = 0;
    count_buffer_hit_query = 0;
    count_buffer_total_query = 0;
#endif
    // select
    execute_selects(loaded_file, &query->fourth);

    struct_data_frame result;

    // optimize join order
    optimize_joins(loaded_file, query);

    // join
    execute_joins(loaded_file, &query->third, &result);

    size_t size_ans = query->first.length * sizeof(int64_t);
    int64_t *ans = (int64_t *) malloc(size_ans);
    memset(ans, 0, size_ans);

    // sum
    execute_sums(loaded_file, &query->first, &result, ans);

    // output result
    for (int i = 0; i < query->first.length; i++) {
        if (result.num_row != 0) {
            printf("%" PRId64, ans[i]);
        }
        if (i != query->first.length - 1) {
            putc(',', stdout);
        }
    }
    puts("");

    // clean up
    free_struct_data_frame(&result);
    free(ans);

#ifdef DEBUG_PROFILING
    fprintf(stderr,
            "                 Query     Total\nTime        %10ld%10ld\nBuffer miss %10ld%10ld\nBuffer      %10ld%10ld\n\n",
            time_query,
            time_total,
            count_buffer_total_query - count_buffer_hit_query,
            count_buffer_total - count_buffer_hit_total,
            count_buffer_total_query,
            count_buffer_total);
#endif
}

#endif //LITE_DB_LITEDB_C

