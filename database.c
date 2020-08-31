//
// Created by Nataf on 05/05/2020.
//
#include <string.h>
#include <stdint.h>
#include <assert.h>
#include <unistd.h>
#include <errno.h>
#include "database.h"
#include "parser.h"
#include "b_tree.h"

int hash_pow(int num, int e){
    int a = 1;
    for(int i = 0; i < e; i++){
        a = num*a;
    }
    return a;
}

uint32_t compute_hash(const char *s) {
    const int p = 31;
    const int m = hash_pow(10, 9) + 9;
    uint32_t hash_value = 0;
    uint32_t p_pow = 1;
    for (int i = 0; s[i] != '\0'; i++) {
        hash_value = (hash_value + (s[i] - 'a' + 1) * p_pow) % m;
        p_pow = (p_pow * p) % m;
    }
    return hash_value;
}

Table* getTable(char* name, DataBase* dataBase){
    for(int i= 0; i<dataBase->num_table; i++){
        if(strcmp(dataBase->Tables[i]->name, name)==0){
            return dataBase->Tables[i];
        }
    }
    return NULL;
}

void serialize_row(Statement *statement, void *destination){
    // destintion is a pointer to the line which we insert in
    for (int i=0; i<statement->values_to_insert.size; i++) {
        assert(statement->values_to_insert.name[i]);
        memmove(destination + i*CELL_SIZE, statement->values_to_insert.name[i], strlen(statement->values_to_insert.name[i])+1);
        }
}

void deserialize_row(void *source, Statement *destination, int *cols, Table *table) {
    void* end = source+table->row_size;
    char results[1][255];
    int i = 0;
    while(i < MAX_COLS && i < destination->values_to_select.size && cols[i] != -1 ){
        assert(source + cols[i] * CELL_SIZE <= end);
        memmove(&(results[0]), source + cols[i] * CELL_SIZE, CELL_SIZE);
        printf("%s \t", results[0]);
        i++;
    }
}

void create_new_root(Table* table, uint32_t right_child_page_num) {
    /*
Handle splitting the root.
Old root copied to new page, becomes left child.
Address of right child passed in.
Re-initialize root page to contain the new root node.
New root node points to two children.
*/

    void *root = get_page(table->pager, table->root_page_num);
    void *right_child = get_page(table->pager, right_child_page_num);
    uint32_t left_child_page_num = get_unused_page_num(table->pager);
    void *left_child = get_page(table->pager, left_child_page_num);
    /* Left child has data copied from old root */
    memcpy(left_child, root, PAGE_SIZE);
    set_node_root(left_child, false);
    /* Root node is a new internal node with one key and two children */
    initialize_internal_node(root);
    set_node_root(root, true);
    *internal_node_num_keys(root) = 1;
    *internal_node_child(root, 0) = left_child_page_num;
    uint32_t left_child_max_key = get_node_max_key(left_child, table->nbl);
    *internal_node_key(root, 0) = left_child_max_key;
    *internal_node_right_child(root) = (right_child_page_num);
    *node_parent(left_child) = table->root_page_num;
    *node_parent(right_child) = table->root_page_num;

}
/*
+Until we start recycling free pages, new pages will always
+go onto the end of the database file
*/
uint32_t get_unused_page_num(Pager* pager) { return pager->num_pages; }

void leaf_node_split_and_insert(Cursor* cursor, uint32_t  key, Statement* statement) {
/*
  Create a new node and move half the cells over.
  Insert the new value in one of the two nodes.
  Update parent or create a new parent.
  */
    void *old_node = get_page(cursor->table->pager, cursor->page_num);
    uint32_t old_max = get_node_max_key(old_node, cursor->table->nbl);
    uint32_t new_page_num = get_unused_page_num(cursor->table->pager);

    void *new_node = get_page(cursor->table->pager, new_page_num);
    initialize_leaf_node(new_node);
    *node_parent(new_node) = *node_parent(old_node);//new node is a right brother of old node
    *leaf_node_next_leaf(new_node) = *leaf_node_next_leaf(old_node);
    *leaf_node_next_leaf(old_node) = new_page_num;
    /*
  All existing keys plus new key should be divided
  evenly between old (left) and new (right) nodes.
  Starting from the right, move each key to correct position.
  */
    uint32_t leaf_node_left_split_count = cursor->table->nbl->leafNodeLeftSplitCount;
    uint32_t leaf_node_right_split_count = cursor->table->nbl->leafNodeRightSplitCount;

    for (int32_t i = cursor->table->nbl->leafNodeMaxCells; i >= 0; i--) {//division du old entre droite et gauche
        void *destination_node;
        if (i >= leaf_node_left_split_count) {
            destination_node = new_node;
        } else {
            destination_node = old_node;
        }
        uint32_t index_within_node = i % leaf_node_left_split_count;
        void *destination = leaf_node_cell(destination_node, index_within_node, cursor->table->nbl);

        if (i == cursor->cell_num) {
            //serialize_row(statement, destination);
            serialize_row(statement, leaf_node_value(destination_node, index_within_node, cursor->table->nbl));
            *leaf_node_key(destination_node, index_within_node,cursor->table->nbl) = key;
        } else if (i > cursor->cell_num) {
            memcpy(destination, leaf_node_cell(old_node, i - 1,cursor->table->nbl ), cursor->table->nbl->leafNodeCellSize);
        } else {
            memcpy(destination, leaf_node_cell(old_node, i, cursor->table->nbl), cursor->table->nbl->leafNodeCellSize);
        }
    }
    /* Update cell count on both leaf nodes */
    *(leaf_node_num_cells(old_node)) = leaf_node_left_split_count;
    *(leaf_node_num_cells(new_node)) = leaf_node_right_split_count;

    if (is_node_root(old_node)) {
        return create_new_root(cursor->table, new_page_num);
    } else {
        uint32_t parent_page_num = *node_parent(old_node);
        uint32_t new_max = get_node_max_key(old_node,cursor->table->nbl);
        void *parent = get_page(cursor->table->pager, parent_page_num);

        update_internal_node_key(parent, old_max, new_max);
        internal_node_insert(cursor->table, parent_page_num, new_page_num);
        return;
    }
}

void leaf_node_insert(Cursor* cursor, uint32_t key, Statement* statement) {
    void* node = get_page(cursor->table->pager, cursor->page_num);

    uint32_t num_cells = *leaf_node_num_cells(node);
    if (num_cells >= cursor->table->nbl->leafNodeMaxCells) {
        // Node full
        leaf_node_split_and_insert(cursor, key, statement);
        return;
    }

    if (cursor->cell_num < num_cells) {
        //cursor->cell_num points on the location where we have to insert.
        // If there is already something at this location, we move what is next in right direction
        // Make room for new cell
        for (uint32_t i = num_cells; i > cursor->cell_num; i--) {
            memcpy(leaf_node_cell(node, i, cursor->table->nbl), leaf_node_cell(node, i - 1,cursor->table->nbl),
                   cursor->table->nbl->leafNodeCellSize);
        }
    }
    *(leaf_node_num_cells(node)) += 1;
    *leaf_node_key(node, cursor->cell_num,cursor->table->nbl) = key;
    serialize_row(statement, leaf_node_value(node, cursor->cell_num, cursor->table->nbl));
}

void internal_node_insert(Table *table, uint32_t parent_page_num, uint32_t child_page_num) {
    /*
    Add a new child/key pair to parent that corresponds to child
    */

    void *parent = get_page(table->pager, parent_page_num);
    void *child = get_page(table->pager, child_page_num);
    uint32_t child_max_key = get_node_max_key(child, table->nbl);
    uint32_t index = internal_node_find_child(parent, child_max_key);

    uint32_t original_num_keys = *internal_node_num_keys(parent);
    *internal_node_num_keys(parent) = original_num_keys + 1;

    if (original_num_keys >= INTERNAL_NODE_MAX_CELLS) {
        printf("Need to implement splitting internal node\n");
        exit(EXIT_FAILURE);
    }
    uint32_t right_child_page_num = *internal_node_right_child(parent);
    void *right_child = get_page(table->pager, right_child_page_num);
    if (child_max_key > get_node_max_key(right_child, table->nbl)) {
        /* Replace right child */
        *internal_node_child(parent, original_num_keys) = right_child_page_num;
        *internal_node_key(parent, original_num_keys) = get_node_max_key(right_child, table->nbl);
        *internal_node_right_child(parent) = child_page_num;
    } else {
        /* Make room for the new cell */
        for (uint32_t i = original_num_keys; i > index; i--) {
            void *destination = internal_node_cell(parent, i);
            void *source = internal_node_cell(parent, i - 1);
            memcpy(destination, source, INTERNAL_NODE_CELL_SIZE);
        }
        *internal_node_child(parent, index) = child_page_num;
        *internal_node_key(parent, index) = child_max_key;
    }
}

void* cursor_value(Cursor* cursor) {
    uint32_t page_num = cursor->page_num;
    void* page = get_page(cursor->table->pager, page_num);
    return leaf_node_value(page, cursor->cell_num, cursor->table->nbl);
}

void cursor_advance(Cursor* cursor) {
    uint32_t page_num = cursor->page_num;
    void* node = get_page(cursor->table->pager, page_num);
    cursor->cell_num += 1;
    if (cursor->cell_num >= (*leaf_node_num_cells(node))) {
        /* Advance to next leaf node */
        uint32_t next_page_num = *leaf_node_next_leaf(node);
        if (next_page_num == 0) {
            /* This was rightmost leaf */
            cursor->end_of_table = true;
        } else {
            cursor->page_num = next_page_num;
            cursor->cell_num = 0;
        }
    }
}

int indexCol(Table table, char* name){
    //returns index of the column whose name is name
    for(int i = 0; i<table.num_cols; i++){
        if (strcmp(table.columns[i]->colName, name)==0){
            return i;
        }
    }
    return -1;
}

ExecuteResult executeCreate(Statement* statement, DataBase* dataBase){
    for(int i = 0; i<dataBase->num_table; i++){
        if(strcmp(dataBase->Tables[i]->name, statement->table_to_create->name) == 0){
            return EXECUTE_TABLE_ALREADY_EXISTS;
        }
    }
    if(dataBase->num_table == MAX_TABLES){
        return EXECUTE_DATABASE_FULL;
    }
    int row_size = CELL_SIZE*statement->table_to_create->num_cols;
    dataBase->Tables[dataBase->num_table] = new_table(statement->table_to_create->name,statement->table_to_create->name, row_size, statement->table_to_create->num_of_keys,
                                                      statement->table_to_create->num_cols, statement->table_to_create->columns);
    dataBase->num_table++;
    return EXECUTE_SUCCESS;
}

ExecuteResult execute_insert(Statement* statement, Table* table) {
    if (table == NULL){
        return EXECUTE_TABLE_DOESNT_EXIST;
    }
    if(statement->values_to_insert.size != table->num_cols){
        return EXECUTE_MISMATCH_NUM_VALUES;
    }
    char *key_to_hash = (char *) xmalloc(table->num_of_keys * CELL_SIZE);
    strcpy(key_to_hash,"");
    int i = 0;
    while (strcmp(table->columns[i]->colName,"")!=0) {
        if (table->columns[i]->key) {
            strcat(key_to_hash, statement->values_to_insert.name[i]);
        }
        i++;
    }
    printf("key to hash = %s \t", key_to_hash);
    uint32_t key_to_insert = compute_hash(key_to_hash);
    printf("key to insert = %d \n", key_to_insert);

    Cursor* cursor = table_find(table, key_to_insert);

    void* node = get_page(table->pager, cursor->page_num);
    uint32_t num_cells = *leaf_node_num_cells(node);

    if (cursor->cell_num < num_cells) {
        uint32_t key_at_index = *leaf_node_key(node, cursor->cell_num,cursor->table->nbl);
        if (key_at_index == key_to_insert) {
            printf("DUPLICATE_KEY\n");
            free(cursor);
            return EXECUTE_DUPLICATE_KEY;
        }
    }
    leaf_node_insert(cursor, key_to_insert,statement);
    free(cursor);
    return EXECUTE_SUCCESS;
}

ExecuteResult execute_select(Statement* statement, Table* table) {
    if (table == NULL){
        return EXECUTE_TABLE_DOESNT_EXIST;
    }
    int cols[MAX_COLS];
    int last = 0;

    if (strcmp(statement->values_to_select.name[0], "*") == 0){
        statement->values_to_select.size = table->num_cols;
        for (int i = 0; i<MAX_COLS; i++) cols[i]=i;
    }

    Cursor* cursor = table_start(table);

    if (strcmp(statement->values_to_select.name[0], "*") == 0){
        while (!(cursor->end_of_table)) {
            deserialize_row(cursor_value(cursor), statement, cols, table);
            printf("\n");
            cursor_advance(cursor);
        }
        free(cursor);
        return EXECUTE_SUCCESS;
    }
    for (int i = 0; statement->values_to_select.name[i]; i++)
    {
        cols[i]=indexCol(*table,statement->values_to_select.name[i]);
        last = i;
        if(cols[i]==-1) {
            free(cursor);
            return EXECUTE_COL_DOESNT_EXIST;
        }
    }
    while (!(cursor->end_of_table)) {
        cols[last+1] = -1;
        deserialize_row(cursor_value(cursor), statement, cols, table);
        cursor_advance(cursor);
        printf("\n");
    }
    free(cursor);
    return EXECUTE_SUCCESS;
}

ExecuteResult execute_update(Statement* statement, Table* table) {
//if  the column we want the update is a key - we do  not update
    if (table == NULL) {
        printf("Error table doesnt exist\n");
        return EXECUTE_TABLE_DOESNT_EXIST;
    }
    int cols_to_update[MAX_COLS];
    int cols_to_compare[MAX_COLS];
    int last_update = 0;
    int last_compare = 0;
    Cursor *cursor = table_start(table);
    for (int i = 0; statement->columns_to_update.name[i]; i++) {
        cols_to_update[i] = indexCol(*table, statement->columns_to_update.name[i]);
        last_update = i;
        if (cols_to_update[i] == -1) {
            free(cursor);
            printf("Error col doesnt exist\n");
            return EXECUTE_COL_DOESNT_EXIST;
        }
    }
    for (int i = 0; statement->columns_to_compare.name[i]; i++) {
        cols_to_compare[i] = indexCol(*table, statement->columns_to_compare.name[i]);
        last_compare = i;
        if (cols_to_compare[i] == -1) {
            free(cursor);
            printf("Error col doesnt exist\n");
            return EXECUTE_COL_DOESNT_EXIST;
        }
    }
    cols_to_update[last_update + 1] = -1;
    cols_to_compare[last_compare + 1] = -1;
    int j = 0;
    int i = 0;
    int to_update = 1;
    int updated = 0;
    while (!(cursor->end_of_table)) {
        j = 0;
        to_update = 1;
        while (cols_to_compare[j] != -1) {
            if (strcmp(cursor_value(cursor) + cols_to_compare[j] * CELL_SIZE, statement->values_to_compare.name[j]) != 0) {
                to_update = 0;
                break;
            }
            else {
                j++;
            }
        }
        i = 0;
        if (to_update) {
            updated++;
            while (i < MAX_COLS && cols_to_update[i] != -1) {
                memcpy(cursor_value(cursor) + cols_to_update[i] * CELL_SIZE, statement->values_for_update.name[i],
                       CELL_SIZE);
                i++;
            }
        }
        cursor_advance(cursor);
    }
    if(updated == 0){
        printf("Condition never verified\n");
    }
    free(cursor);
    return EXECUTE_SUCCESS;
}

ExecuteResult execute_statement(Statement* statement, DataBase* dataBase){
    Table* table;
    switch (statement->type) {
        case (STATEMENT_INSERT):
            table = getTable(statement->table_to_insert->name, dataBase);
            return execute_insert(statement, table);
            break;
        case (STATEMENT_SELECT):
            table = getTable(statement->table_from_select->name, dataBase);
            return execute_select(statement, table);
            break;
        case(STATEMENT_CREATE):
            return executeCreate(statement, dataBase);
            break;
        case(STATEMENT_UPDATE):
            table = getTable(statement->table_to_update->name, dataBase);
            return execute_update(statement, table);
        default:
            break;
    }
}

void* get_page(Pager* pager, uint32_t page_num) {
    // Get some page of the table according to page_num. In the case where the page doesn't exist, allocates a new page and updates
    // page num of the pager
    if (page_num > TABLE_MAX_PAGES) {
        printf("Tried to fetch page number out of bounds. %d > %d\n", page_num,
               TABLE_MAX_PAGES);
        exit(EXIT_FAILURE);
    }

    if (pager->pages[page_num] == NULL) {
        // Cache miss. Allocate memory and load from file.
        void* page = calloc(1,PAGE_SIZE);
        memset(page,0,0);
        uint32_t num_pages = pager->file_length / PAGE_SIZE;

        // We might save a partial page at the end of the file
        if (pager->file_length % PAGE_SIZE) {
            num_pages += 1;
        }
        if (page_num <= num_pages) {
            lseek(pager->file_descriptor, page_num * PAGE_SIZE, SEEK_SET);
            ssize_t bytes_read = read(pager->file_descriptor, page, PAGE_SIZE);
            if (bytes_read == -1) {
                printf("Error reading file: %d\n", errno);
                exit(EXIT_FAILURE);
            }
        }

        pager->pages[page_num] = page;
        if (page_num >= pager->num_pages) {
                  pager->num_pages = page_num + 1;
            }
    }

    return pager->pages[page_num];
}

void db_close(DataBase* db) {
    for (int j = 0; j < db->num_table; j++) {
        Pager *pager = db->Tables[j]->pager;

        for (uint32_t i = 0; i < pager->num_pages; i++) {
            if (pager->pages[i] == NULL) {
                continue;
            }
            pager_flush(pager, i);
            free(pager->pages[i]);
            pager->pages[i] = NULL;
        }
        int result = close(pager->file_descriptor);
        if (result == -1) {
            printf("Error closing db file.\n");
            exit(EXIT_FAILURE);
        }
        freeTable(&db->Tables[j]);
    }
    free(db);
}

void pager_flush(Pager* pager, uint32_t page_num) {
    //writes in the file
    if (pager->pages[page_num] == NULL) {
        printf("Tried to flush null page\n");
        exit(EXIT_FAILURE);
    }

    off_t offset = lseek(pager->file_descriptor, page_num * PAGE_SIZE, SEEK_SET);

    if (offset == -1) {
        printf("Error seeking: %d\n", errno);
        exit(EXIT_FAILURE);
    }

    ssize_t bytes_written =
            write(pager->file_descriptor, pager->pages[page_num], PAGE_SIZE);

    if (bytes_written == -1) {
        printf("Error writing: %d\n", errno);
        exit(EXIT_FAILURE);
    }

}

Cursor* table_start(Table* table) {
    Cursor* cursor =  table_find(table, 0);
    void *node = get_page(table->pager, cursor->page_num);
    uint32_t num_cells = *leaf_node_num_cells(node);
    cursor->end_of_table = (num_cells == 0);
    return cursor;
}

Cursor* internal_node_find(Table* table, uint32_t page_num, uint32_t key) {
    void *node = get_page(table->pager, page_num);
    uint32_t child_index = internal_node_find_child(node, key);
    uint32_t child_num = *internal_node_child(node, child_index);
    void *child = get_page(table->pager, child_num);
    switch (get_node_type(child)) {
        case NODE_LEAF:
            return leaf_node_find(table, child_num, key);
        case NODE_INTERNAL:
            return internal_node_find(table, child_num, key);
    }
}

/*
Return the position of the given key.
If the key is not present, return the position
where it should be inserted
*/
Cursor* table_find(Table* table, uint32_t key) {
      uint32_t root_page_num = table->root_page_num;
      void* root_node = get_page(table->pager, root_page_num);
            if (get_node_type(root_node) == NODE_LEAF) {
                return leaf_node_find(table, root_page_num, key);
          } else {
                return internal_node_find(table, root_page_num, key);
            }
}

Cursor *leaf_node_find(Table *table, uint32_t page_num, uint32_t key) {
    void *node = get_page(table->pager, page_num);
    uint32_t num_cells = *leaf_node_num_cells(node);
    Cursor *cursor = xmalloc(sizeof(Cursor));
    cursor->table = table;
    cursor->page_num = page_num;

    // Binary search
    uint32_t min_index = 0;
    uint32_t one_past_max_index = num_cells;
    while (one_past_max_index != min_index) {
        uint32_t index = (min_index + one_past_max_index) / 2;
        uint32_t key_at_index = *leaf_node_key(node, index, cursor->table->nbl);
        if (key == key_at_index) {
            cursor->cell_num = index;
            //cursor->page_num = page_num;
            return cursor;
        }
        if (key < key_at_index) {
            one_past_max_index = index;
        } else {
            min_index = index + 1;
        }
    }
    cursor->cell_num = min_index;
    return cursor;
}

