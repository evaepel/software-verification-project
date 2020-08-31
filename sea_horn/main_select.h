//
// Created by Nataf on 31/08/2020.
//

#ifndef SH_CHECK_INSERT_MAIN_SELECT_H
#define SH_CHECK_INSERT_MAIN_SELECT_H
#include <stdio.h>
#include <string.h>
#include <stdlib.h>
#include <stdbool.h>
#include <stdint.h>
#include<stddef.h>
#include <sys/fcntl.h>
#include <unistd.h>
#include <errno.h>

#define MAX_SIZE 100

#include "seahorn/seahorn.h"

#define CELL_SIZE           256
#define MAX_COLS            50
#define TABLE_MAX_PAGES     100
#define ROW_SIZE            512
#define NUM_COLS            2
#define MAX_TABLES          100
#define PAGE_SIZE           4096

extern int nd(void);

extern void *nd_ptr(void);

static void *g_bgn;
static void *g_end;
static bool g_active;
const uint32_t LEAF_NODE_HEADER_SIZE = sizeof(uint8_t) + sizeof(uint8_t) + sizeof(uint32_t) +
                                       +sizeof(uint32_t) +
                                       +sizeof(uint32_t);
const uint32_t NODE_TYPE_OFFSET = 0;
const uint32_t IS_ROOT_OFFSET = sizeof(uint8_t);
const uint32_t LEAF_NODE_NUM_CELLS_OFFSET = (sizeof(uint8_t) + sizeof(uint8_t) + sizeof(uint32_t));
const uint32_t LEAF_NODE_NEXT_LEAF_OFFSET =
        +sizeof(uint8_t) + sizeof(uint8_t) + sizeof(uint32_t) + sizeof(uint32_t);



typedef enum {
    NODE_INTERNAL, NODE_LEAF
} NodeType;
typedef enum {
    PREPARE_SUCCESS, PREPARE_UNRECOGNIZED_STATEMENT, PREPARE_SYNTAX_ERROR, PREPARE_TOO_MANY_KEYS, PREPARE_MISS_KEY
} PrepareResult;
typedef enum {
    STATEMENT_INSERT, STATEMENT_SELECT, STATEMENT_UPDATE, STATEMENT_CREATE, STATEMENT_DELETE, STATEMENT_JOIN
} StatementType;
typedef enum {
    EXECUTE_SUCCESS, EXECUTE_TABLE_FULL, EXECUTE_MISS_VALUES, EXECUTE_TABLE_ALREADY_EXISTS,
    EXECUTE_DATABASE_FULL, EXECUTE_TABLE_DOESNT_EXIST, EXECUTE_COL_DOESNT_EXIST,
    EXECUTE_DUPLICATE_KEY, EXECUTE_MISMATCH_NUM_VALUES
} ExecuteResult;
typedef struct {
    uint32_t leafNodeKeySize;
    uint32_t leafNodeKeyOffset;
    uint32_t leafNodeValueSize; // = row_size
    uint32_t leafNodeValueOffset; // = LEAF_NODE_KEY_OFFSET + LEAF_NODE_KEY_SIZE;
    uint32_t leafNodeCellSize; // = LEAF_NODE_KEY_SIZE + LEAF_NODE_VALUE_SIZE;
    uint32_t leafNodeSpaceForCells; // = PAGE_SIZE - LEAF_NODE_HEADER_SIZE;
    uint32_t leafNodeMaxCells; // LEAF_NODE_SPACE_FOR_CELLS / LEAF_NODE_CELL_SIZE;
    uint32_t leafNodeRightSplitCount;
    uint32_t leafNodeLeftSplitCount;
} NodeBodyLayout;
typedef struct {
    char colName[MAX_COLS];
    bool key;
} Column;

typedef struct {
    char *name[MAX_COLS];
} ColNames;
typedef struct {
    char *name[MAX_COLS];
    int size;
} Value;

typedef struct {
    int file_descriptor;
    FILE *file;
    uint32_t file_length;
    uint32_t num_pages;
    void *pages[TABLE_MAX_PAGES];
} Pager;

typedef struct {
    char name[CELL_SIZE];
    int num_of_keys;
    uint32_t root_page_num;
    uint32_t num_cols;
    int row_size;
    int rows_per_page;
    int table_max_rows;
    Column *columns[MAX_COLS];
    Pager *pager;
    NodeBodyLayout *nbl;
    bool saved;
} Table;

typedef struct {
    Table *Tables[MAX_TABLES];
    int num_table;
} DataBase;

typedef struct {
    StatementType type;
    Table *table_to_insert;
    Table *table_from_select;
    Table *table_to_update;
    Table *table_to_create;
    Value values_to_insert;
    Value values_to_select;
    Value values_for_update;
    Value values_to_compare;
    ColNames columns_to_update;
    ColNames columns_to_compare;

} Statement;
typedef struct {
    Table *table;
    uint32_t page_num;
    uint32_t cell_num;
    bool end_of_table;  // Indicates a position one past the last element
} Cursor;

NodeBodyLayout *new_nbl(int row_size);

int strlen2(char *s);

char *strcpy2(char *destination, char *source);

void *xmalloc(size_t size);

void *leaf_node_cell(void *node, uint32_t cell_num, NodeBodyLayout *nodeBodyLayout);

void *get_page(Pager *pager, uint32_t page_num);

void *leaf_node_value(void *node, uint32_t cell_num, NodeBodyLayout *nodeBodyLayout);

void *cursor_value(Cursor *cursor);

NodeType get_node_type(void *node);

uint32_t *leaf_node_num_cells(void *node);

uint32_t *leaf_node_key(void *node, uint32_t cell_num, NodeBodyLayout *nodeBodyLayout);

Cursor *leaf_node_find(Table *table, uint32_t page_num, uint32_t key);

Cursor *table_find(Table *table, uint32_t key);

Cursor *table_start(Table *table);

Pager *pager_open(const char *filename);

void set_node_type(void *node, NodeType type);

void set_node_root(void *node, bool is_root);

uint32_t *leaf_node_next_leaf(void *node);

void cursor_advance(Cursor *cursor);

void initialize_leaf_node(void *node);

Table *
new_table(const char *filename, char *name, int row_size, int num_of_keys, int num_cols, Column *columns[MAX_COLS]);

Statement *new_statement();

int indexCol(Table table, char *name);

PrepareResult checkSelect(char command[MAX_SIZE][MAX_SIZE], Statement *statement);

void deserialize_row(void *source, Statement *destination, Table *table);

ExecuteResult execute_select(Statement *statement, Table *table);

#endif //SH_CHECK_INSERT_MAIN_SELECT_H
