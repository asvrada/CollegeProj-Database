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

// runtime assert
#define ASSERT(val) \
do {\
    if (!(val)) {\
        fprintf(stderr, "%s:%d: runtime assert failed\n", __FILE__, __LINE__);\
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
    ASSERT(c->top >= size);

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
    // parse letter c at the beginning of column
    c->input++;

    head = c->top;
    tmp_p = c->input;

    while (1) {
        char ch = tmp_p[0];

        // todo: end of column
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
int parse_first_part(struct_input_files *files, char *input) {
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

    // read line by line, push them into stack
    while (getline(&line, &size, stdin) != -1) {
        strncpy((char *) context_push(&c, strlen(line)), line, strlen(line));
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

#define PAGE_SIZE 4096
#define SIZE_BUFFER 2 * PAGE_SIZE

/**
 * struct that stores intermediate table (after join, after predicates)
 */
typedef struct {
    char *relations;
    int *index;
    int num_row;
} struct_data_frame;

/**
 * A struct that describe a csv file
 */
typedef struct {
    // name of this file / relation
    char relation;

    // 1D int array, to keep things compact
    int *data;

    // number of column in this relation
    int num_col;
    int num_row;

    // if there is predicates on this relation, then df will not be null
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

void free_struct_data_frame(struct_data_frame *df) {
    if (df == NULL) {
        return;
    }

    free(df->relations);
    free(df->index);

    df->relations = NULL;
    df->index = NULL;
    df->num_row = 0;
}

void init_struct_file(struct_file *file) {
    file->relation = '\0';
    file->data = NULL;
    file->num_col = -1;
    file->num_row = -1;
    file->df = NULL;
}

void free_struct_file(struct_file *file) {
    if (file->data != NULL) {
        free(file->data);
    }
    file->data = NULL;
    file->relation = '\0';
    file->num_col = -1;
    file->num_row = -1;

    if (file->df != NULL) {
        free_struct_data_frame(file->df);
        file->df = NULL;
    }
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
    buffer->buffer = malloc(max_size);
    buffer->max_size = max_size;
    buffer->cur_size = 0;
}

/**
 * Free the buffer's memory
 */
void free_struct_fwrite_buffer(struct_fwrite_buffer *buffer) {
    free(buffer->buffer);
    buffer->max_size = 0;
    buffer->cur_size = 0;
}

/**
 * write content to output buffer
 * The actual file will be write if buffer is almost full
 */
void fwrite_buffered(void *buffer, size_t size, size_t count, FILE *stream, struct_fwrite_buffer *manual_buffer) {
    // todo: page align
    size_t size_buffer = size * count;

    // manual buffer is full
    if (manual_buffer->cur_size + size_buffer >= manual_buffer->max_size) {
        fwrite(manual_buffer->buffer, sizeof(*manual_buffer->buffer), manual_buffer->cur_size, stream);

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
    fwrite(manual_buffer->buffer, sizeof(*(manual_buffer->buffer)), manual_buffer->cur_size, stream);

    // buffer is now empty
    manual_buffer->cur_size = 0;
}

/**
 * Filter data in the relation, given predicate like A.c3 < 7666
 * And create filered index for input file
 *
 * @param file
 * @param predicate
 */
void filter_data_given_predicate(struct_file *file, struct_predicate *predicate) {
    ASSERT(file->relation == predicate->lhs.relation);

    // empty file, do nothing
    if (file->num_row == 0) {
        return;
    }

    // if dataframe is NULL, create dataframe
    if (file->df == NULL) {
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

    struct_data_frame *const df = file->df;
    const int row = file->df->num_row;
    const int col = file->num_col;

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
        number = file->data[df->index[fast] * col + column];

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

    // if no rows selected, empty the relation
    if (df->num_row == 0) {
        free(file->data);
        file->data = NULL;
        file->num_row = 0;

        // also empty the data frame
        free_struct_data_frame(file->df);
        file->df = NULL;
    } else {
        // realloc memory for df->index
        df->index = (int *) realloc(df->index, df->num_row * sizeof(int));
    }
}

/**
 * Read csv file, given the path, from disk into memory. Convert string to int then write them to disk
 *
 * @param relation: name of the relation
 * @param file: path to the file on the disk
 * @param loaded_file: struct describing the loaded file
 */
void load_csv_file(char relation, char *file, struct_file *loaded_file) {
    // setup stack to store data in memory
    struct_parse_context c;
    // again, we only need the stack
    init_struct_parse_context(&c, NULL);

    // set name for files to write to disk
    char path_file_binary[] = "?.binary";
    char path_file_meta[] = "?.meta";
    path_file_binary[0] = relation;
    path_file_meta[0] = relation;

    FILE *file_input = fopen(file, "r");
    FILE *file_binary = fopen(path_file_binary, "wb");
    // todo: file_meta

    char *buffer = malloc(SIZE_BUFFER);

    char *secondary_buffer = malloc(64);
    // length of content stored in the buffer
    int size_secondary_buffer = 0;

    struct_fwrite_buffer fwrite_buffer;
    // init buffer
    init_struct_fwrite_buffer(&fwrite_buffer, SIZE_BUFFER);

    int num_col = -1;
    int tmp_num_col = 0;

    while (1) {
        size_t size_buffer = fread(buffer, sizeof(*buffer), SIZE_BUFFER, file_input);

        if (size_buffer == 0) {
            break;
        }

        int cursor = 0;
        int cursor_prev = 0;

        while (cursor < size_buffer) {
            char current = buffer[cursor];

            if (current == '\n' && num_col == -1) {
                num_col = tmp_num_col + 1;
            }

            if (current == ',' || current == '\n') {
                if (num_col == -1) {
                    tmp_num_col++;
                }

                int number = 0;

                if (size_secondary_buffer == 0) {
                    // simply strtol from buffer
                    // the number to be read will always be valid because it will end with non-numeric char
                    number = strtol(&buffer[cursor_prev], NULL, 0);
                } else {
                    // this will always occur at the beginning of processing a new buffer
                    assert(cursor_prev == 0);

                    int size_tmp = size_secondary_buffer + cursor;
                    char *tmp = malloc(size_tmp + 1);

                    // copy secondary buffer to tmp buffer
                    memcpy(tmp, secondary_buffer, size_secondary_buffer);
                    // copy buffer to tmp buffer
                    memcpy(tmp + size_secondary_buffer, buffer, cursor);
                    tmp[size_tmp] = 0;

                    number = strtol(tmp, NULL, 0);

                    // free tmp buffer
                    free(tmp);

                    // clear secondary buffer
                    size_secondary_buffer = 0;
                }

                // move cursor to the beginning of next number
                cursor += 1;
                cursor_prev = cursor;

                // write number to file
                fwrite_buffered(&number, sizeof(number), 1, file_binary, &fwrite_buffer);
                // also write to buffer
                *(int *) context_push(&c, sizeof(number)) = number;
                continue;
            }

            // this current char is part of the number
            cursor++;
        }

        // we have read the entire buffer, now copy whats left into secondary buffer
        if (cursor_prev < size_buffer) {
            // always happen at the end of a buffer
            int length = size_buffer - cursor_prev;

            // copy whats left of buffer into secondary buffer
            memcpy(secondary_buffer, buffer + cursor_prev, length);
            size_secondary_buffer = length;
        }
    }

    // write whats left inside output buffer to file
    fwrite_buffered_flush(&fwrite_buffer, file_binary);

    // copy content in the stack to file in-memory buffer
    loaded_file->relation = relation;
    loaded_file->num_col = num_col;
    size_t len = c.top;
    loaded_file->data = malloc(len);
    memcpy(loaded_file->data, context_pop(&c, len), len);
    loaded_file->num_row = len / sizeof(int) / num_col;

    free(buffer);
    free(secondary_buffer);
    free_struct_fwrite_buffer(&fwrite_buffer);
    free_struct_parse_context(&c);

    fclose(file_input);
    fclose(file_binary);
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

#endif //LITE_DB_LITEDB_C

