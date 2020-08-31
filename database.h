//
// Created by Nataf on 05/05/2020.
//

#ifndef SOFTWARE_VERIF_PROJECT_MASTER_DATABASE_H
#define SOFTWARE_VERIF_PROJECT_MASTER_DATABASE_H

#include "parser.h"
#include "b_tree.h"
typedef enum { EXECUTE_SUCCESS, EXECUTE_TABLE_ALREADY_EXISTS,
    EXECUTE_DATABASE_FULL, EXECUTE_TABLE_DOESNT_EXIST, EXECUTE_COL_DOESNT_EXIST,
    EXECUTE_DUPLICATE_KEY,EXECUTE_MISMATCH_NUM_VALUES} ExecuteResult;

typedef struct {

    Table* table;
    uint32_t page_num;
    uint32_t cell_num;
    bool end_of_table;  // Indicates a position one past the last element
} Cursor;

int hash_pow(int num, int e);

uint32_t compute_hash(const char *s);

Table* getTable(char* name, DataBase* dataBase);

void serialize_row(Statement *statement, void *destination);

void deserialize_row(void *source, Statement *destination, int *cols, Table *table);

void create_new_root(Table* table, uint32_t right_child_page_num);

uint32_t get_unused_page_num(Pager* pager);

void leaf_node_split_and_insert(Cursor* cursor, uint32_t key, Statement* statement);

void leaf_node_insert(Cursor* cursor, uint32_t key, Statement* statement);

void internal_node_insert(Table* table, uint32_t parent_page_num, uint32_t child_page_num);

void* cursor_value(Cursor* cursor);

void cursor_advance(Cursor* cursor);

int indexCol(Table table, char* name);
ExecuteResult executeCreate(Statement* statement, DataBase* dataBase);

ExecuteResult execute_insert(Statement* statement, Table* table);

ExecuteResult execute_select(Statement* statement, Table* table);

ExecuteResult execute_statement(Statement* statement, DataBase* dataBase);

void* get_page(Pager* pager, uint32_t page_num);

void db_close(DataBase* db);

void pager_flush(Pager* pager, uint32_t page_num);

Cursor* table_start(Table* table);

Cursor *internal_node_find(Table *table, uint32_t page_num, uint32_t key);

Cursor* table_find(Table* table, uint32_t key);

Cursor* leaf_node_find(Table* table, uint32_t page_num, uint32_t key);

#endif //SOFTWARE_VERIF_PROJECT_MASTER_DATABASE_H