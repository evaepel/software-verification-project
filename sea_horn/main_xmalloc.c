//
// Created by Nataf on 24/08/2020.
//
#include <stdio.h>
#include <string.h>
#include <stdlib.h>
#include <stdbool.h>
#include <stdint.h>
#include<stddef.h>
#define MAX_SIZE 100
#include "seahorn/seahorn.h"
#define CELL_SIZE           256
#define MAX_COLS            50
#define TABLE_MAX_PAGES     100
#define MAX_TABLES          100
#define PAGE_SIZE           4096
extern int nd(void);
typedef enum { STATEMENT_INSERT, STATEMENT_SELECT, STATEMENT_UPDATE, STATEMENT_CREATE, STATEMENT_DELETE, STATEMENT_JOIN } StatementType;

typedef struct {
    char* buffer;
    size_t buffer_length;
    size_t input_length;
} InputBuffer;

typedef struct {
    int file_descriptor;
    FILE* file;
    uint32_t file_length;
    uint32_t num_pages;
    void* pages[TABLE_MAX_PAGES];
} Pager;
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
    Table* Tables[MAX_TABLES];
    int num_table;
} DataBase;


typedef struct {
    char *name[MAX_COLS];
    int size;
} Value;

typedef struct {
    Table* table;
    uint32_t page_num;
    uint32_t cell_num;
    bool end_of_table;  // Indicates a position one past the last element
} Cursor;

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
void* xmalloc(size_t size){
    sassert(size <= 4096);
    void* p = NULL;
    if (size <= 512)
        p = (malloc(512));
    else if (size <= 1024)
        p = (malloc(1024));
    else if (size <= 2048)
        p = (malloc(2048));
    else if (size <= 4096)
        p = (malloc(4096));
    assume(((ptrdiff_t)p) > 0);
    return p;
}

int main(int argc, char* argv[]){
    xmalloc(128);
    //int i = nd();
    //assume(i==1);
    //Pager* p = (Pager*)xmalloc(sizeof(Pager)*i);
    Pager* p = (Pager*)xmalloc(sizeof(Pager));
    Table* table = (Table*)xmalloc(sizeof(Table));
    NodeBodyLayout* nbl = (NodeBodyLayout*)xmalloc(sizeof(NodeBodyLayout));
    Cursor* cursor = xmalloc(sizeof(Cursor));
    InputBuffer* input_buffer = (InputBuffer*)xmalloc(sizeof(InputBuffer));
    DataBase* dataBase = (DataBase*)xmalloc(sizeof(DataBase));
    Statement* statement = (Statement*)xmalloc(sizeof(Statement));
    Column* column = (Column*)xmalloc(sizeof(Column));
}