#ifndef PROJECT_PARSER_H
#define PROJECT_PARSER_H
#include <stdio.h>
#include <string.h>
#include <stdlib.h>
#include <stdbool.h>
#include <stdint.h>
#include "alloc_manager.h"
#include "b_tree.h"

#define CELL_SIZE           256
#define MAX_COLS            50
#define MAX_NUM_KEYS        10
#define TABLE_MAX_PAGES     500
#define MAX_TABLES          100
#define PAGE_SIZE           4096

typedef enum {
    META_COMMAND_SUCCESS,
    META_COMMAND_UNRECOGNIZED_COMMAND
} MetaCommandResult;

typedef enum { PREPARE_SUCCESS, PREPARE_UNRECOGNIZED_STATEMENT, PREPARE_SYNTAX_ERROR, PREPARE_TABLE_DOESNT_EXIST, PREPARE_TOO_MANY_KEYS, PREPARE_MISS_KEY } PrepareResult;

typedef enum { STATEMENT_INSERT, STATEMENT_SELECT, STATEMENT_UPDATE, STATEMENT_CREATE} StatementType;


typedef struct {
    char* buffer;
    size_t buffer_length;
    size_t input_length;
} InputBuffer;

typedef struct{
    char colName[CELL_SIZE];
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
} Table;

typedef struct {
    StatementType type;
    Table* table_to_insert;
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
    Table* Tables[MAX_TABLES];
    int num_table;
} DataBase;

InputBuffer* new_input_buffer();

void freeBuffer(InputBuffer* bf);

size_t my_getline(char **lineptr, size_t *n, FILE *stream);

Pager* pager_open(const char* filename);

Table* new_table(const char* filename, char* name, int row_size,int num_of_keys, int num_cols,  Column* columns[MAX_COLS]);

void freeTable(Table** tb);

void freeValue(Value* val);

Statement* new_statement();

void freeStatement(Statement** st);

DataBase* new_data_base();

PrepareResult checkSelect(char* command[], Statement* statement);

PrepareResult checkInsert(char* command[], Statement* statement);

PrepareResult checkUpdate(char* command[], Statement* statement, DataBase* db);

PrepareResult checkCreate(char* command[], Statement* statement);

void saveDataToFile(DataBase* db, FILE* file);

void extractDataFromFile(DataBase* db, FILE* file);

void split_line(InputBuffer* input_buffer, DataBase* db, int num_table);

#endif