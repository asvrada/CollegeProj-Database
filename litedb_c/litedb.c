/*
 * Created by ZiJie Wu on 2019-03-28.
 * The entire Database assignment in one GIANT source file, just to make compiling easier
 *
 * It comprises of several components: data loader, parser
 */
#ifndef LITE_DB_LITEDB_C
#define LITE_DB_LITEDB_C

#include <stdlib.h>
#include <assert.h>
#include <string.h>
#include <inttypes.h>

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
#if 0
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

    files->files = malloc(26 * sizeof(char *));
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

static void free_struct_relation_column(struct_relation_column *rc) {
    // we do not malloc here, so nothing to free
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
    free(sl->relations);
    sl->relations = NULL;
    sl->length = 0;
}

static void free_struct_join(struct_join *join) {
    free_struct_relation_column(&join->lhs);
    free_struct_relation_column(&join->rhs);
}

static void free_struct_third_line(struct_third_line *tl) {
    for (int i = 0; i < tl->length; i++) {
        free_struct_join(&tl->joins[i]);
    }

    free(tl->joins);
    tl->joins = NULL;
    tl->length = 0;
}

static void free_struct_predicate(struct_predicate *p) {
    // nothing to free
}

static void free_struct_fourth_line(struct_fourth_line *fl) {
    // this line may be empty
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
    // parse 'c' at the beginning of column
    c->input++;

    head = c->top;
    tmp_p = c->input;

    while (1) {
        char ch = tmp_p[0];

        // if its not number, then its end of column
        if (!IS_NUMERIC(ch)) {
            size_t len = c->top - head;
            // string of number, no terminal
            const char *str = context_pop(c, len);

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
    const char *tmp_input = context_pop(&c, len);

    // malloc memory for *input and copy the input to it
    *input = (char *) malloc(len + 1);
    memcpy(*input, tmp_input, len);
    (*input)[len] = 0;

    free_struct_parse_context(&c);
}

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

//const int SIZE_PAGE = 4096;
//const int SIZE_BUFFER = 2 * SIZE_PAGE;
//const int NUM_BUFFER_PER_RELATION = 8192;

#define SIZE_PAGE 4096
#define SIZE_BUFFER (2 * SIZE_PAGE)
#define NUM_BUFFER_PER_RELATION 4096

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
     * [01] [02] [11]
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
    FILE *file_binary;

    // size (in bytes): NUM_BUFFER_PER_RELATION * SIZE_BUFFER
    int *pages;

    // the index of first row the pages stores (inclusive)
    int row_start;
    // the index of last row the pages stores (exclusive)
    int row_end;
} struct_file_binary;

/**
 * A struct that describe a relation
 */
typedef struct {
    // name of this file / relation
    char relation;

    /**
     * The struct to the .binary file
     */
    struct_file_binary file_binary;

    /**
     * The struct to the .meta file
     */
//    FILE* file_meta;

    // todo: move them into metadata
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

void init_struct_file_binary(struct_file_binary *file) {
    file->file_binary = NULL;
    file->pages = (int *) malloc(NUM_BUFFER_PER_RELATION * SIZE_BUFFER);
    memset(file->pages, 0, NUM_BUFFER_PER_RELATION * SIZE_BUFFER);
    file->row_end = 0;
    file->row_start = 0;
}

void free_struct_file_binary(struct_file_binary *file) {
    if (file->file_binary != NULL) {
        fclose(file->file_binary);
        file->file_binary = NULL;
    }

    if (file->pages != NULL) {
        free(file->pages);
    }
    file->pages = NULL;
    file->row_end = 0;
    file->row_start = 0;
}

void init_struct_file(struct_file *file) {
    file->relation = '\0';
    file->num_col = 0;
    file->num_row = 0;
    file->df = NULL;

    init_struct_file_binary(&file->file_binary);
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

    free_struct_file_binary(&file->file_binary);
}

void init_struct_files(struct_files *files, int length) {
    files->files = malloc(length * sizeof(struct_file));
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

/**
 * write content to output buffer
 * The actual file will be write if buffer is almost full
 */
void fwrite_buffered(void *buffer, size_t size, size_t count, FILE *stream, struct_fwrite_buffer *manual_buffer) {
    size_t size_buffer = size * count;

    // manual buffer is full
    if (manual_buffer->cur_size + size_buffer >= manual_buffer->max_size) {
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
    fwrite(manual_buffer->buffer, sizeof(*manual_buffer->buffer), manual_buffer->max_size, stream);

    // buffer is now empty
    manual_buffer->cur_size = 0;
}

/**
 * Select the index of row from file
 * @param file
 * @param row: 0 indexed
 */
const int *const select_row_from_file(struct_file *const file, const int row) {
    ASSERT(file->num_row > row);

    // 1. calculate offset in bytes to read from disk
    const int byte_per_row = file->num_col * sizeof(int);
    const int row_per_buffer = SIZE_BUFFER / byte_per_row;

    // if buffered
    if (file->file_binary.row_start <= row && row < file->file_binary.row_end) {
        // offset (in number of buffers) from the beginning of pages
        int offset_num_buffer = (row - file->file_binary.row_start) / row_per_buffer;

        return (int *) ((char *) file->file_binary.pages + (offset_num_buffer * SIZE_BUFFER)) +
               (row - file->file_binary.row_start - offset_num_buffer * row_per_buffer) * file->num_col;
    }

    // offset (in number of buffers) from the beginning of file
    int offset_num_buffer = row / row_per_buffer;

    // not buffered, read pages into buffer
    // offset in bytes
    size_t offset = offset_num_buffer * SIZE_BUFFER;
    // move pointer to that bytes
    fseek(file->file_binary.file_binary, offset, SEEK_SET);

    // read in pages
    size_t size_read = fread(file->file_binary.pages, 1, NUM_BUFFER_PER_RELATION * SIZE_BUFFER,
                             file->file_binary.file_binary);

    if (size_read == 0) {
        // this should never happen
        ASSERT(0);
    }

    file->file_binary.row_start = offset_num_buffer * row_per_buffer;
    file->file_binary.row_end = file->file_binary.row_start + row_per_buffer * NUM_BUFFER_PER_RELATION;

    // if size_read is smaller than expected, update row end
    if (size_read < NUM_BUFFER_PER_RELATION * SIZE_BUFFER) {
        file->file_binary.row_end = file->file_binary.row_start + size_read / SIZE_BUFFER * row_per_buffer;
    }

    return file->file_binary.pages +
           (row - file->file_binary.row_start) * file->num_col;
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
void load_csv_file(char relation, char *file, struct_file *loaded_file) {
    // set name for files to write to disk
    char path_file_binary[] = "?.binary";
    char path_file_meta[] = "?.meta";
    path_file_binary[0] = relation;
    path_file_meta[0] = relation;

    // freed at the end of this function
    FILE *file_input = fopen(file, "r");
    FILE *file_binary = fopen(path_file_binary, "wb+");
    // todo: file_meta
    // FILE *file_meta

    // main buffer to read char from file
    char *buffer = (char *) malloc(SIZE_BUFFER);
    char *secondary_buffer = (char *) malloc(64);
    // length of content stored in the buffer
    int size_secondary_buffer = 0;

    struct_fwrite_buffer fwrite_buffer;
    // init buffer
    init_struct_fwrite_buffer(&fwrite_buffer, SIZE_BUFFER);

    int num_row = 0;
    int num_col = -1;
    int tmp_num_col = 0;

    // we use the stack to store the first line of csv file, because we don't know the number of column
    // freed after processing the first line of csv file
    struct_parse_context c;
    init_struct_parse_context(&c, NULL);

    // buffer for storing numbers in the same row
    int *buffer_row = NULL;
    // number of int currently stored, if == num_col, write the buffer_row to output buffer and empty buffer
    int size_buffer_row = 0;

    while (1) {
        int size_buffer = fread(buffer, sizeof(*buffer), SIZE_BUFFER, file_input);

        if (size_buffer == 0) {
            break;
        }

        int cursor = 0;
        int cursor_prev = 0;

        while (cursor < size_buffer) {
            char current = buffer[cursor];

            // if its end of number
            if (current == ',' || current == '\n') {
                if (num_col == -1) {
                    tmp_num_col++;

                    // This is the end of first line
                    // We do:
                    // 1. assign num_col
                    // 2. create row buffer
                    // 3. move content inside stack into row buffer
                    if (current == '\n') {
                        // 1. assign num_col
                        num_col = tmp_num_col;

                        // 2. create row buffer
                        // find out size of stack, plus one because the last number is not pushed into the stack
                        size_t top = c.top;
                        size_t num_bytes_buffer_row = num_col * sizeof(int);
                        // freed at the end of this function
                        buffer_row = (int *) malloc(num_bytes_buffer_row);

                        // 3. move content inside stack into row buffer
                        // copy content from stack into buffer row
                        memcpy(buffer_row, context_pop(&c, top), top);
                        size_buffer_row = top / sizeof(int);
                        // free stack
                        free_struct_parse_context(&c);
                    }
                }

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

                // if still processing first line, output number to stack
                if (num_col == -1) {
                    *(int *) context_push(&c, sizeof(number)) = number;
                } else {
                    // write number to buffer row
                    buffer_row[size_buffer_row] = number;
                    size_buffer_row++;
                }

                // if buffer row is full, write to output buffer, and empty it
                // this also means row++
                if (size_buffer_row == num_col) {
                    fwrite_buffered(buffer_row, sizeof(buffer_row[0]), size_buffer_row, file_binary, &fwrite_buffer);
                    size_buffer_row = 0;
                    num_row++;
                }
                continue;
            }

            // this current char is part of the number, skip it
            cursor++;
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

    // todo: handle when csv has no newline at the end

    assert(size_buffer_row == 0);


    // write whats left inside output buffer to file
    if (fwrite_buffer.cur_size != 0) {
        fwrite_buffered_flush(&fwrite_buffer, file_binary);
    }

    // assign value to loaded_file
    loaded_file->relation = relation;
    loaded_file->file_binary.file_binary = file_binary;
    loaded_file->num_col = num_col;
    loaded_file->num_row = num_row;


    free(buffer);
    free(secondary_buffer);
    free(buffer_row);
    free_struct_fwrite_buffer(&fwrite_buffer);

    fclose(file_input);
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

    struct_data_frame *const df = file->df;
    const int row = file->df->num_row;

    // pointer to file->df->index
    size_t slow = 0;
    // pointer to file->df->index
    size_t fast = 0;

    // index of column
    int column = predicate->lhs.column;
    // the number @ given column
    int number = 0;
    int shouldKeep = 0;

    // for each row
    for (; fast < row; fast++) {
        shouldKeep = 0;
        // check predicate
        const int *const tmp_row = select_row_from_file(file, df->index[fast]);
        number = tmp_row[column];

        switch (predicate->operator) {
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
        df->index[slow] = fast;
        slow++;
    }

    // update row of df
    df->num_row = slow;

    // if no rows selected, empty the index
    if (df->num_row == 0) {
        free(df->index);
        df->index = 0;
    } else {
        // realloc memory for df->index
        df->index = (int *) realloc(df->index, df->num_row * sizeof(int));
    }
}

/**
 * (Left Deep) Join two data frame that representing some relations
 * We assume the input is always valid
 *
 * The result of join will be stored back into intermediate
 *
 * @param loaded_files: info about relations
 * @param intermediate: the intermediate result of joining two relation
 * @param relation: some of the original relation
 */
// todo: ACDD
void nested_loop_join(const struct_files *const loaded_files,
                      struct_data_frame *const intermediate,
                      struct_file *const relation,
                      const struct_join *join) {
    ASSERT(loaded_files != NULL && intermediate != NULL && relation != NULL && join != NULL);

    // first 1: size of relation
    // second 1: make room for ending \0
    size_t length_joined_relations = strlen(intermediate->relations) + 1 + 1;
    // the name of the new relations, remeber to free the one from inter and assign this to it
    char *joined_relations = (char *) malloc(length_joined_relations * sizeof(char));

    // number of relations, or number of index per row, of intermediate dataframe
    int inter_num_relations = strlen(intermediate->relations);

    // assign value
    memcpy(joined_relations, intermediate->relations, inter_num_relations);
    joined_relations[length_joined_relations - 2] = relation->relation;
    joined_relations[length_joined_relations - 1] = '\0';

    // todo: check if one of the df is empty, and don't init if isn't empty
    // Init df for files without one
    if (relation->df == NULL) {
        init_struct_data_frame_for_file(relation);
    }

    // we use the stack again
    struct_parse_context c;
    init_struct_parse_context(&c, NULL);


    int inter_offset_relation = findIndexOf(intermediate->relations,
                                            inter_num_relations,
                                            join->lhs.relation);

    // pointer to loaded file on the left side of join
    // join->lhs.relation - 'A' = index of the file
    struct_file *const file_left = loaded_files->files + (join->lhs.relation - 'A');

    // outer loop
    // todo: cache rows from outer loop?
    for (int row_inter = 0; row_inter < intermediate->num_row; row_inter++) {
        const int *const row_left = select_row_from_file(file_left,
                                                         intermediate->index[
                                                                 row_inter * inter_num_relations +
                                                                 inter_offset_relation]);

        int number_left = row_left[join->lhs.column];

        // inner loop
        for (int row_relation = 0; row_relation < relation->df->num_row; row_relation++) {
            const int *const row_right = select_row_from_file(relation,
                                                              relation->df->index[row_relation]);

            // select number from given columns

            int number_right = row_right[join->rhs.column];

            if (number_left == number_right) {
                // push this row (based on original file) into stack
                // copy index[row_inter] from inter, and concat it with index[row_relation]
                size_t size_to_copy = inter_num_relations * sizeof(int);

                memcpy(context_push(&c, size_to_copy),
                       &(intermediate->index[row_inter * inter_num_relations]),
                       size_to_copy);

                *(int *) context_push(&c, sizeof(int)) = relation->df->index[row_relation];
            }
        }
    }

    // clean up, swap index and relations
    // free original index and relation
    free(intermediate->relations);
    free(intermediate->index);

    // assign new value
    intermediate->relations = joined_relations;

    // copy index
    // size of index, in bytes
    size_t top = c.top;

    if (top == 0) {
        intermediate->index = NULL;
        intermediate->num_row = 0;
    } else {
        // memcpy
        int *tmp_index = context_pop(&c, top);
        intermediate->index = (int *) malloc(top);
        memcpy(intermediate->index, tmp_index, top);

        // bytes / sizeof int / number per row = num of row
        intermediate->num_row = top / strlen(intermediate->relations) / sizeof(int);
    }

    free_struct_parse_context(&c);
}


void nested_loop_join_both_joined_before(const struct_files *const loaded_files,
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

    // for each row in inter
    for (int i = 0; i < intermediate->num_row; i++) {
        const int *const row_index = &intermediate->index[i * num_relations];

        const int *const row_left = select_row_from_file(file_left, row_index[offset_lhs]);
        const int *const row_right = select_row_from_file(file_right, row_index[offset_rhs]);

        int number_left = row_left[join->lhs.column];
        int number_right = row_right[join->rhs.column];

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
        int *tmp_index = context_pop(&c, top);
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
 // did I miss relations without joins...
void execute_joins(struct_files *const loaded_file, const struct_third_line *const tl, struct_data_frame *inter) {
    if (tl->length == 0) {
        ASSERT(0);
        return;
    }

    // todo: simplify this!
    char first_relation = tl->joins[0].lhs.relation;
    struct_file *first_file = &loaded_file->files[first_relation - 'A'];

    if (first_file->df == NULL) {
        init_struct_data_frame_for_file(first_file);
    }

    copy_struct_data_frame(first_file->df, inter);

    // check if join valid
    for (int i = 0; i < tl->length; i++) {
        struct_join *join = &tl->joins[i];

        // if both joined before
        if (-1 != findIndexOf(inter->relations, strlen(inter->relations), join->lhs.relation)
            && -1 != findIndexOf(inter->relations, strlen(inter->relations), join->rhs.relation)) {

            nested_loop_join_both_joined_before(loaded_file, inter, join);
            continue;
        }

        // else if one of them is not joined before
        // we swap the order of join to make sure the lhs one is joined before
        if (-1 == findIndexOf(inter->relations, strlen(inter->relations), join->lhs.relation)) {
            ASSERT(-1 != findIndexOf(inter->relations, strlen(inter->relations), join->rhs.relation));

            // swap join lhs and rhs
            struct_relation_column tmp = join->lhs;
            join->lhs = join->rhs;
            join->rhs = tmp;
        }

        struct_file *rhs_file = &loaded_file->files[join->rhs.relation - 'A'];

        nested_loop_join(loaded_file, inter, rhs_file, join);
    }
}

/**
 * This will output the final result
 *
 * @param loaded_file
 * @param fl
 * @param result
 */
void execute_sums(struct_files *const loaded_file,
                  const struct_first_line *const fl,
                  struct_data_frame *result,
                  int64_t *ans) {
    int num_relations = strlen(result->relations);

    // for each row in result
    for (int i = 0; i < result->num_row; i++) {
        // ABDC
        int *rows = &result->index[num_relations * i];

        // for each sum
        for (int col = 0; col < fl->length; col++) {
            struct_relation_column *rc = &fl->sums[col];
            // find relation
            char relation = rc->relation;
            struct_file *file = &loaded_file->files[relation - 'A'];

            // find the index of this relation in the result
            int index = findIndexOf(result->relations, strlen(result->relations), relation);
            int row = rows[index];

            // find value
            const int *tmp_row = select_row_from_file(file, row);

            // accumulate result
            ans[col] += tmp_row[rc->column];
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
#define DEBUG 1

void execute(struct_files *const loaded_file, const struct_query *const query) {
    execute_selects(loaded_file, &query->fourth);

    struct_data_frame result;

    execute_joins(loaded_file, &query->third, &result);

    size_t size_ans = query->first.length * sizeof(int64_t);
    int64_t *ans = (int64_t *) malloc(size_ans);
    memset(ans, 0, size_ans);

    execute_sums(loaded_file, &query->first, &result, ans);

    for (int i = 0; i < query->first.length; i++) {
        if (result.num_row != 0) {
            printf("%" PRId64, ans[i]);
        }
        if (i != query->first.length - 1) {
            putc(',', stdout);
        }
    }
    puts("");

    free_struct_data_frame(&result);
    free(ans);
}

#endif //LITE_DB_LITEDB_C

