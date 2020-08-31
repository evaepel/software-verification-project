//
// Created by Nataf on 30/08/2020.
//

#ifndef SH_CHECK_INSERT_MAIN_UPDATE_H
#define SH_CHECK_INSERT_MAIN_UPDATE_H
//
// Created by Nataf on 27/08/2020.
//
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
#define MAX_TABLES          100
#define PAGE_SIZE           4096
#define INTERNAL_NODE_CELL_SIZE  (sizeof(uint32_t) + sizeof(uint32_t))
#define INTERNAL_NODE_MAX_CELLS  15
extern int nd(void);
extern int8_t * nd_ptr(void);

static int8_t * g_bgn;
static int8_t * g_end;
static bool g_active;

typedef enum { PREPARE_SUCCESS, PREPARE_UNRECOGNIZED_STATEMENT, PREPARE_SYNTAX_ERROR,PREPARE_TABLE_DOESNT_EXIST } PrepareResult;
typedef enum { STATEMENT_INSERT, STATEMENT_SELECT, STATEMENT_UPDATE, STATEMENT_CREATE, STATEMENT_DELETE, STATEMENT_JOIN } StatementType;
typedef enum { NODE_INTERNAL, NODE_LEAF } NodeType;
typedef enum { EXECUTE_SUCCESS, EXECUTE_TABLE_FULL, EXECUTE_MISS_VALUES, EXECUTE_TABLE_ALREADY_EXISTS,
    EXECUTE_DATABASE_FULL, EXECUTE_TABLE_DOESNT_EXIST, EXECUTE_COL_DOESNT_EXIST,
    EXECUTE_DUPLICATE_KEY,EXECUTE_MISMATCH_NUM_VALUES} ExecuteResult;

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
typedef struct{
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
    FILE* file;
    uint32_t file_length;
    uint32_t num_pages;
    void* pages[TABLE_MAX_PAGES];
} Pager;
const uint32_t NODE_TYPE_SIZE = sizeof(uint8_t);
const uint32_t NODE_TYPE_OFFSET = 0;
const uint32_t IS_ROOT_SIZE = sizeof(uint8_t);
const uint32_t IS_ROOT_OFFSET = sizeof(uint8_t);
const uint32_t PARENT_POINTER_SIZE = sizeof(uint32_t);
const uint32_t PARENT_POINTER_OFFSET = sizeof(uint8_t) + sizeof(uint8_t);
const uint32_t COMMON_NODE_HEADER_SIZE = (sizeof(uint8_t) + sizeof(uint8_t) + sizeof(uint32_t));
/*
 * Leaf Node Header Layout
 */
const uint32_t LEAF_NODE_NUM_CELLS_SIZE = sizeof(uint32_t);
const uint32_t LEAF_NODE_NUM_CELLS_OFFSET = (sizeof(uint8_t) + sizeof(uint8_t) + sizeof(uint32_t));
const uint32_t LEAF_NODE_NEXT_LEAF_SIZE = sizeof(uint32_t);
const uint32_t LEAF_NODE_NEXT_LEAF_OFFSET =
        +    sizeof(uint8_t) + sizeof(uint8_t) + sizeof(uint32_t) + sizeof(uint32_t);
//const uint32_t LEAF_NODE_HEADER_SIZE = COMMON_NODE_HEADER_SIZE +
//                                      +                                       LEAF_NODE_NUM_CELLS_SIZE +
//                                    +                                       LEAF_NODE_NEXT_LEAF_SIZE;
const uint32_t LEAF_NODE_HEADER_SIZE = sizeof(uint8_t) + sizeof(uint8_t) + sizeof(uint32_t) +
                                       +                                       sizeof(uint32_t) +
                                       +                                       sizeof(uint32_t);

/*
Internal Node Header Layout
*/
const uint32_t INTERNAL_NODE_NUM_KEYS_SIZE = sizeof(uint32_t);
//const uint32_t INTERNAL_NODE_NUM_KEYS_OFFSET = COMMON_NODE_HEADER_SIZE;
const uint32_t INTERNAL_NODE_NUM_KEYS_OFFSET = (sizeof(uint8_t) + sizeof(uint8_t) + sizeof(uint32_t));
const uint32_t INTERNAL_NODE_RIGHT_CHILD_SIZE = sizeof(uint32_t);
//const uint32_t INTERNAL_NODE_RIGHT_CHILD_OFFSET =
//            INTERNAL_NODE_NUM_KEYS_OFFSET + INTERNAL_NODE_NUM_KEYS_SIZE;
const uint32_t INTERNAL_NODE_RIGHT_CHILD_OFFSET =
        sizeof(uint8_t) + sizeof(uint8_t) + sizeof(uint32_t) + sizeof(uint32_t);
//const uint32_t INTERNAL_NODE_HEADER_SIZE = COMMON_NODE_HEADER_SIZE + INTERNAL_NODE_NUM_KEYS_SIZE + INTERNAL_NODE_RIGHT_CHILD_SIZE;
const uint32_t INTERNAL_NODE_HEADER_SIZE = sizeof(uint8_t) + sizeof(uint8_t) + sizeof(uint32_t) + sizeof(uint32_t)
                                           + sizeof(uint32_t);

/*
 * Internal Node Body Layout
*/
const uint32_t INTERNAL_NODE_KEY_SIZE = sizeof(uint32_t);
const uint32_t INTERNAL_NODE_CHILD_SIZE = sizeof(uint32_t);
//const uint32_t INTERNAL_NODE_CELL_SIZE = sizeof(uint32_t) + sizeof(uint32_t);
//   INTERNAL_NODE_CHILD_SIZE + INTERNAL_NODE_KEY_SIZE;
typedef struct {
    char name[CELL_SIZE];
    int num_of_keys;
    uint32_t root_page_num;
    uint32_t  num_cols;
    int row_size;
    int rows_per_page;
    int table_max_rows;
    Column* columns[MAX_COLS];
    Pager* pager;
    NodeBodyLayout* nbl;
    int num_rows;
} Table;

typedef struct {
    Table* table;
    uint32_t page_num;
    uint32_t cell_num;
    bool end_of_table;  // Indicates a position one past the last element
} Cursor;

typedef struct {
    StatementType type;
    Table* table_to_insert;
    Table *table_from_select;
    Table *table_to_update;
    Table *table_to_create;
    Table *table_to_delete;
    Table *table_left_join;
    Table *table_right_join;
    Value values_to_insert;
    Value values_to_select;
    Value values_for_update;
    Value values_to_compare;
    ColNames columns_to_update;
    ColNames columns_to_compare;
    bool saved;
    void* dest;

} Statement;

typedef struct {
    Table* Tables[MAX_TABLES];
    int num_table;
} DataBase;

void* xmalloc(size_t size);

int hash_pow(int num, int e);

uint32_t compute_hash(const char *s);

int strlen2(char *s);

char *strcpy2(char *destination, char *source);

DataBase* new_data_base();

Statement* new_statement();

void serialize_row_insert(Statement* statement, void* destination, int row_size);

void* get_page(Pager* pager, uint32_t page_num);

NodeType get_node_type(void *node);

uint32_t* leaf_node_num_cells(void* node);

void* leaf_node_cell(void* node, uint32_t cell_num, NodeBodyLayout* nodeBodyLayout);

uint32_t * leaf_node_key(void* node, uint32_t cell_num, NodeBodyLayout* nodeBodyLayout);

Cursor *leaf_node_find(Table *table, uint32_t page_num, uint32_t key);

uint32_t *internal_node_num_keys(void *node);

uint32_t *internal_node_cell(void *node, uint32_t cell_num);

uint32_t *internal_node_key(void *node, uint32_t key_num);

uint32_t *internal_node_right_child(void *node);

uint32_t *leaf_node_next_leaf(void *node);

uint32_t internal_node_find_child(void* node, uint32_t key);

uint32_t* node_parent(void* node);

uint32_t *internal_node_child(void *node, uint32_t child_num);

Cursor* internal_node_find(Table* table, uint32_t page_num, uint32_t key);

Cursor* table_find(Table* table, uint32_t key);

uint32_t  get_node_max_key(void *node, NodeBodyLayout* nbl);

uint32_t get_unused_page_num(Pager* pager);

void set_node_type(void *node, NodeType type);

void set_node_root(void *node, bool is_root);

void initialize_leaf_node(void* node);

void* leaf_node_value(void* node, uint32_t cell_num, NodeBodyLayout* nodeBodyLayout);

bool is_node_root(void *node);

void initialize_internal_node(void *node);

void create_new_root(Table* table, uint32_t right_child_page_num);

void update_internal_node_key(void* node, uint32_t old_key, uint32_t new_key);

void internal_node_insert(Table *table, uint32_t parent_page_num, uint32_t child_page_num);

void *cursor_value(Cursor *cursor);

void leaf_node_split_and_insert(Cursor* cursor, uint32_t  key, Statement* statement);

void leaf_node_insert(Cursor* cursor, uint32_t key, Statement* statement);

Pager* pager_open(const char* filename);

NodeBodyLayout* new_nbl(int row_size);

Table* new_table(const char* filename, char* name, int row_size, int num_of_keys, int num_cols, Column* columns[MAX_COLS]);

void cursor_advance(Cursor* cursor);

int indexCol(Table table, char* name);

Table* getTable(char* name, DataBase* dataBase);

PrepareResult checkUpdate(char command[MAX_SIZE][MAX_SIZE], Statement *statement, DataBase *db);

ExecuteResult execute_update(Statement* statement, Table* table);

#endif //SH_CHECK_INSERT_MAIN_UPDATE_H
