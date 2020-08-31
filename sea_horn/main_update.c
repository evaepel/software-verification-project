//
// Created by Nataf on 23/08/2020.
//

#include "main_update.h"

void *xmalloc(size_t size) {
    sassert(size <= 4096);
    void *p = NULL;
    //assume(size<=4096);
    if (size <= 512)
        p = (malloc(512));
    else if (size <= 1024)
        p = (malloc(1024));
    else if (size <= 2048)
        p = (malloc(2048));
    else if (size <= 8192)
        p = (malloc(8192));
    assume(p != NULL);
    return p;
}

int hash_pow(int num, int e) {
    int a = 1;
    for (int i = 0; i < e; i++) {
        a = num * a;
    }
    return a;
}

uint32_t compute_hash(const char *s) {
    printf("%s\t", s);
    const int p = 31;
    const int m = hash_pow(10, 9) + 9;
    uint32_t hash_value = 0;
    uint32_t p_pow = 1;
    for (int i = 0; s[i] != '\0'; i++) {
        hash_value = (hash_value + (s[i] - 'a' + 1) * p_pow) % m;
        p_pow = (p_pow * p) % m;
    }
    printf("%u\n", hash_value);
    return hash_value;
}

int strlen2(char *s) {
    int i;
    for (i = 0; *s; ++i, ++s) {}
    return i;
}

char *strcpy2(char *destination, char *source) {
    char *start = destination;

    while (*source != '\0') {
        *destination = *source;
        destination++;
        source++;
    }

    *destination = '\0'; // add '\0' at the end
    return start;
}

DataBase *new_data_base() {
    DataBase *dataBase = (DataBase *) xmalloc(sizeof(DataBase));
    dataBase->num_table = 0;
    return dataBase;
}

Statement *new_statement() {
    Statement *statement = (Statement *) xmalloc(sizeof(Statement));
    statement->table_to_create = new_table("dummy", " ", 1, 0, 0, NULL);
    statement->table_to_insert = new_table("dummy", " ", 1, 0, 0, NULL);
    statement->table_from_select = new_table("dummy", " ", 1, 0, 0, NULL);
    statement->table_to_update = new_table("dummy", " ", 1, 0, 0, NULL);
    for (int i = 0; i < MAX_COLS; i++) {
        statement->values_to_insert.name[i] = NULL;
        statement->values_to_select.name[i] = NULL;
        statement->values_for_update.name[i] = NULL;
    }
    for (int i = 0; i < MAX_COLS; i++) {
        statement->columns_to_update.name[i] = "";
        strcpy(statement->table_to_create->columns[i]->colName, "");
        strcpy(statement->table_to_insert->columns[i]->colName, "");
        strcpy(statement->table_from_select->columns[i]->colName, "");
        strcpy(statement->table_to_update->columns[i]->colName, "");
        statement->columns_to_update.name[i] = NULL;
    }
    statement->values_to_insert.size = 0;
    statement->values_to_select.size = 0;
    statement->values_for_update.size = 0;
    return statement;
}

void serialize_row_insert(Statement *statement, void *destination, int row_size) {
    statement->dest = destination;
    statement->saved = false;
    if (!g_active) {// && nd()) {//g_active == 0 and nd()!=0
        g_active = !g_active;   //g_active = 1
        statement->saved = true;
        assume((int8_t *) destination == g_bgn);
        assume((int8_t *) destination + row_size == g_end);
    }
        //(int8_t *)
    else {
        assume((int8_t *) destination > g_end);
    }
    for (int i = 0; i < statement->values_to_insert.size; i++) {
        memmove(destination + i * CELL_SIZE, statement->values_to_insert.name[i],
                strlen(statement->values_to_insert.name[i]) + 1);
    }
}

void *get_page(Pager *pager, uint32_t page_num) {
    if (page_num > TABLE_MAX_PAGES) {
        printf("Tried to fetch page number out of bounds. %d > %d\n", page_num,
               TABLE_MAX_PAGES);
        exit(EXIT_FAILURE);
    }

    if (pager->pages[page_num] == NULL) {
        // Cache miss. Allocate memory and load from file.
        void *page = xmalloc(PAGE_SIZE);
        //memset(page,0,0);
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

NodeType get_node_type(void *node) {
    uint8_t value = *((uint8_t *) (node + NODE_TYPE_OFFSET));
    return (NodeType) value;
}

uint32_t *leaf_node_num_cells(void *node) {
    return node + LEAF_NODE_NUM_CELLS_OFFSET;
}

void *leaf_node_cell(void *node, uint32_t cell_num, NodeBodyLayout *nodeBodyLayout) {
    return node + LEAF_NODE_HEADER_SIZE + cell_num * nodeBodyLayout->leafNodeCellSize;
}

uint32_t *leaf_node_key(void *node, uint32_t cell_num, NodeBodyLayout *nodeBodyLayout) {
    return (leaf_node_cell(node, cell_num, nodeBodyLayout));
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

Cursor *table_start(Table *table) {
    sassert(sizeof(Cursor) <= 4096);
    Cursor *cursor = table_find(table, 0);
    void *node = get_page(table->pager, cursor->page_num);
    uint32_t num_cells = *leaf_node_num_cells(node);
    cursor->end_of_table = (num_cells == 0);

    return cursor;
}

uint32_t *internal_node_num_keys(void *node) {
    return node + INTERNAL_NODE_NUM_KEYS_OFFSET;
}

uint32_t *internal_node_cell(void *node, uint32_t cell_num) {

    return (node + INTERNAL_NODE_HEADER_SIZE + cell_num * INTERNAL_NODE_CELL_SIZE);
}

uint32_t *internal_node_key(void *node, uint32_t key_num) {
    return (void *) internal_node_cell(node, key_num) + INTERNAL_NODE_CHILD_SIZE;
}

uint32_t *internal_node_right_child(void *node) {
    return (node + INTERNAL_NODE_RIGHT_CHILD_OFFSET);
}

uint32_t *leaf_node_next_leaf(void *node) {
    return node + LEAF_NODE_NEXT_LEAF_OFFSET;
}

uint32_t internal_node_find_child(void *node, uint32_t key) {
/*
    Return the index of the child which should contain
    the given key.
*/
    uint32_t num_keys = *internal_node_num_keys(node);

    /* Binary search*/
    uint32_t min_index = 0;
    uint32_t max_index = num_keys; /* there is one more child than key */

    while (min_index != max_index) {
        uint32_t index = (min_index + max_index) / 2;
        uint32_t key_to_right = *internal_node_key(node, index);
        if (key_to_right >= key) {
            max_index = index;
        } else {
            min_index = index + 1;
        }
    }
    return min_index;
}

uint32_t *node_parent(void *node) { return node + PARENT_POINTER_OFFSET; }

uint32_t *internal_node_child(void *node, uint32_t child_num) {
    uint32_t num_keys = *internal_node_num_keys(node);
    if (child_num > num_keys) {
        printf("Tried to access child_num %d > num_keys %d\n", child_num, num_keys);
        exit(EXIT_FAILURE);
    } else if (child_num == num_keys) {
        return internal_node_right_child(node);
    } else {
        return internal_node_cell(node, child_num);
    }
}

Cursor *internal_node_find(Table *table, uint32_t page_num, uint32_t key) {
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

Cursor *table_find(Table *table, uint32_t key) {
    uint32_t root_page_num = table->root_page_num;
    void *root_node = get_page(table->pager, root_page_num);
    if (get_node_type(root_node) == NODE_LEAF) {
        return leaf_node_find(table, root_page_num, key);
    } else {
        return NULL;
    }
}

uint32_t get_node_max_key(void *node, NodeBodyLayout *nbl) {
    switch (get_node_type(node)) {
        case NODE_INTERNAL:
            return *internal_node_key(node, *internal_node_num_keys(node) - 1);
        case NODE_LEAF:
            return *leaf_node_key(node, *leaf_node_num_cells(node) - 1, nbl);
    }
}

uint32_t get_unused_page_num(Pager *pager) { return pager->num_pages; }

void set_node_type(void *node, NodeType type) {
    uint8_t value = type;
    *((uint8_t *) (node + NODE_TYPE_OFFSET)) = value;
}

void set_node_root(void *node, bool is_root) {
    uint8_t value = is_root;
    *((uint8_t *) (node + IS_ROOT_OFFSET)) = value;
}

void initialize_leaf_node(void *node) {
    set_node_type(node, NODE_LEAF);
    set_node_root(node, false);
    *leaf_node_num_cells(node) = 0;
    *leaf_node_next_leaf(node) = 0;  // 0 represents no sibling
}

void *leaf_node_value(void *node, uint32_t cell_num, NodeBodyLayout *nodeBodyLayout) {
    return leaf_node_cell(node, cell_num, nodeBodyLayout) + nodeBodyLayout->leafNodeKeySize;
}

bool is_node_root(void *node) {
    uint8_t value = *((uint8_t *) (node + IS_ROOT_OFFSET));
    return (bool) value;
}

void initialize_internal_node(void *node) {
    set_node_type(node, NODE_INTERNAL);
    set_node_root(node, false);
    *internal_node_num_keys(node) = 0;
}

void create_new_root(Table *table, uint32_t right_child_page_num) {
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

void update_internal_node_key(void *node, uint32_t old_key, uint32_t new_key) {
    uint32_t old_child_index = internal_node_find_child(node, old_key);
    *internal_node_key(node, old_child_index) = new_key;
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

void *cursor_value(Cursor *cursor) {
    uint32_t page_num = cursor->page_num;
    void *page = get_page(cursor->table->pager, page_num);
    return leaf_node_value(page, cursor->cell_num, cursor->table->nbl);
}

void leaf_node_split_and_insert(Cursor *cursor, uint32_t key, Statement *statement) {
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
    *node_parent(new_node) = *node_parent(old_node);//new node is a droite du old node
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
            //serialize_row_insert(statement, destination);
            serialize_row_insert(statement, leaf_node_value(destination_node, index_within_node, cursor->table->nbl),
                                 cursor->table->row_size);
            cursor->table->num_rows++;
            *leaf_node_key(destination_node, index_within_node, cursor->table->nbl) = key;
        } else if (i > cursor->cell_num) {
            memcpy(destination, leaf_node_cell(old_node, i - 1, cursor->table->nbl),
                   cursor->table->nbl->leafNodeCellSize);
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
        uint32_t new_max = get_node_max_key(old_node, cursor->table->nbl);
        void *parent = get_page(cursor->table->pager, parent_page_num);

        update_internal_node_key(parent, old_max, new_max);
        internal_node_insert(cursor->table, parent_page_num, new_page_num);
        return;
    }
}

void leaf_node_insert(Cursor *cursor, uint32_t key, Statement *statement) {
    void *node = get_page(cursor->table->pager, cursor->page_num);

    uint32_t num_cells = *leaf_node_num_cells(node);
    if (num_cells >= cursor->table->nbl->leafNodeMaxCells) {
        // Node full
        leaf_node_split_and_insert(cursor, key, statement);
        return;
    }

    if (cursor->cell_num < num_cells) {
        // Make room for new cell
        for (uint32_t i = num_cells; i > cursor->cell_num; i--) {
            memcpy(leaf_node_cell(node, i, cursor->table->nbl), leaf_node_cell(node, i - 1, cursor->table->nbl),
                   cursor->table->nbl->leafNodeCellSize);
        }
    }

    *(leaf_node_num_cells(node)) += 1;
    *leaf_node_key(node, cursor->cell_num, cursor->table->nbl) = key;
    serialize_row_insert(statement, leaf_node_value(node, cursor->cell_num, cursor->table->nbl),
                         cursor->table->row_size);
    cursor->table->num_rows++;
}

Pager *pager_open(const char *filename) {
    int fd = open(filename,
                  O_RDWR |    // Read/Write mode
                  O_CREAT,    // Create file if it does not exist
                  S_IWUSR |    // User write permission
                  S_IRUSR    // User read permission
    );
    if (fd == -1) {
        printf("Unable to open file\n");
        exit(EXIT_FAILURE);
    }

    off_t file_length = lseek(fd, 0, SEEK_END);
    Pager *pager = xmalloc(sizeof(Pager));
    pager->file_descriptor = fd;
    pager->file_length = file_length;
    pager->num_pages = (file_length / PAGE_SIZE);
    if (file_length % PAGE_SIZE != 0) {
        printf("Db file is not a whole number of pages. Corrupt file.\n");
        exit(EXIT_FAILURE);
    }
    for (uint32_t i = 0; i < TABLE_MAX_PAGES; i++) {
        pager->pages[i] = NULL;
    }
    return pager;
}

NodeBodyLayout *new_nbl(int row_size) {
    NodeBodyLayout *nbl = (NodeBodyLayout *) xmalloc(sizeof(NodeBodyLayout));
    nbl->leafNodeKeySize = sizeof(uint32_t);
    nbl->leafNodeKeyOffset = 0;
    nbl->leafNodeValueSize = row_size;
    nbl->leafNodeValueOffset = nbl->leafNodeKeyOffset + nbl->leafNodeKeySize;
    nbl->leafNodeCellSize = nbl->leafNodeKeySize + nbl->leafNodeValueSize;
    nbl->leafNodeSpaceForCells = PAGE_SIZE - LEAF_NODE_HEADER_SIZE;
    nbl->leafNodeMaxCells = nbl->leafNodeSpaceForCells / nbl->leafNodeCellSize;
    nbl->leafNodeRightSplitCount = (nbl->leafNodeMaxCells + 1) / 2;
    nbl->leafNodeLeftSplitCount = nbl->leafNodeMaxCells + 1 - nbl->leafNodeRightSplitCount;
    return nbl;
}

Table *
new_table(const char *filename, char *name, int row_size, int num_of_keys, int num_cols, Column *columns[MAX_COLS]) {
    Pager *pager = pager_open(filename);
    Table *table = (Table *) xmalloc(sizeof(Table));

    table->pager = pager;
    strcpy(table->name, name);
    for (int i = 0; i < MAX_COLS; i++) {
        table->columns[i] = (Column *) xmalloc(sizeof(Column));
        table->columns[i]->key = false;
        strcpy(table->columns[i]->colName, "");
    }
    table->num_of_keys = num_of_keys;
    table->num_cols = num_cols;

    table->row_size = row_size;
    table->nbl = new_nbl(row_size);
    table->rows_per_page = table->nbl->leafNodeMaxCells;
    table->table_max_rows = table->rows_per_page * TABLE_MAX_PAGES;
    int i = 0;
    while (i < num_cols && strcmp(columns[i]->colName, "") != 0) {
        strcpy(table->columns[i]->colName, columns[i]->colName);
        if (columns[i]->key) {
            table->columns[i]->key = true;
        } else {
            table->columns[i]->key = false;
        }
        i++;
    }
    table->root_page_num = 0;
    if (pager->num_pages == 0) {
        // New database file. Initialize page 0 as leaf node.
        void *root_node = get_page(pager, 0);
        initialize_leaf_node(root_node);
        set_node_root(root_node, true);
    }

    return table;
}

void cursor_advance(Cursor *cursor) {
    uint32_t page_num = cursor->page_num;
    void *node = get_page(cursor->table->pager, page_num);
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

int indexCol(Table table, char *name) {
    for (int i = 0; i < table.num_cols; i++) {
        if (strcmp(table.columns[i]->colName, name) == 0) {
            return i;
        }
    }
    return -1;
}

Table *getTable(char *name, DataBase *dataBase) {
    for (int i = 0; i < dataBase->num_table; i++) {
        if (strcmp(dataBase->Tables[i]->name, name) == 0) {
            return dataBase->Tables[i];
        }
    }
    return NULL;
}

PrepareResult checkUpdate(char command[MAX_SIZE][MAX_SIZE], Statement *statement, DataBase *db) {

    statement->type = STATEMENT_UPDATE;
    strcpy(statement->table_to_update->name, command[1]);
    Table *table_to_compare_key = getTable(statement->table_to_update->name, db);
    if (table_to_compare_key == NULL) {
        return PREPARE_TABLE_DOESNT_EXIST;
    }
    int i = 3;
    int k = 0;
    int len;
    len = strlen2(command[i]);
    assume(strlen2(command[i]) > 0);
    statement->columns_to_update.name[k] = xmalloc(len + 1);
    strcpy2(statement->columns_to_update.name[k], command[i]);
    sassert(strlen2(statement->columns_to_update.name[k]) > 0);
    if (!command[i + 1] || !command[i + 2]) { // checks there are 2 next strings
        return PREPARE_SYNTAX_ERROR;
    }
    if (strcmp(command[i + 1], "=") != 0 ||
        strcmp(command[i + 2], "where") == 0) { // checks this is of the form col1 = val1
        return PREPARE_SYNTAX_ERROR;
    }
    assume(strlen2(command[i + 2]) > 0);
    len = strlen2(command[i + 2]);
    statement->values_for_update.name[k] = xmalloc(len + 1); //puts col1
    strcpy2(statement->values_for_update.name[k], command[i + 2]); //puts val1
    sassert(strlen2(statement->values_for_update.name[k]) > 0);
    i += 3;
    k++;
    if (i <= 3) {                           // nothing after set
        return PREPARE_SYNTAX_ERROR;
    }
    statement->values_for_update.name[k] = NULL;
    statement->values_for_update.size = k;
    statement->columns_to_update.name[k] = NULL;
    //todo: add loop
    int m = 0;
    if (command[i] && strcmp(command[i], "where") == 0) {
        while (command[i]) {
            if (!command[i + 1] || !command[i + 2] || !command[i + 3] ||
                strcmp(command[i + 2], "=") != 0) { return PREPARE_SYNTAX_ERROR; }
            len = strlen(command[i + 1]);
            statement->columns_to_compare.name[m] = xmalloc(len + 1);
            strcpy(statement->columns_to_compare.name[m], command[i + 1]);
            len = strlen(command[i + 3]);
            statement->values_to_compare.name[m] = xmalloc(len + 1);
            strcpy(statement->values_to_compare.name[m], command[i + 3]);
            i += 4;
            m++;
            if (command[i] && strcmp(command[i], "and") != 0) { return PREPARE_SYNTAX_ERROR; }

        }
    }
    for (int n = 0; n < k; n++) { //run on all the columns we want to update
        for (int j = 0; j < MAX_COLS; j++) { //run on all the columns in the table_to_compare_key
            if (strcmp(table_to_compare_key->columns[j]->colName, statement->columns_to_update.name[n]) == 0 &&
                table_to_compare_key->columns[j]->key) {//todo compare to 1st column name
                printf("TRIED TO EDIT KEY COLUMN\n");
                return PREPARE_SYNTAX_ERROR;
            }
        }
    }
    return PREPARE_SUCCESS;

}

ExecuteResult execute_update(Statement *statement, Table *table) {
//if  the column we want the update is a key - we do  not update
    //update second set c2 = v22, c3 = v33  where c1 = v1
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
        //cols_to_update[i] = indexCol(*table, statement->columns_to_update.name[i]);
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
            if (strcmp(cursor_value(cursor) + cols_to_compare[j] * CELL_SIZE, statement->values_to_compare.name[j]) !=
                0) {
                to_update = 0;
                break;
            } else {
                j++;
            }
        }
        i = 0;
        if (to_update) {
            updated++;
            while (i < MAX_COLS && cols_to_update[i] != -1) {
                assume(g_end == cursor_value(cursor) + table->row_size);
                sassert(cursor_value(cursor) + i * CELL_SIZE <= g_end - CELL_SIZE);
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

int main(int argc, char *argv[]) {

    //Statement *statement = new_statement();
    char command[MAX_SIZE][MAX_SIZE];

    char word0[MAX_SIZE] = "update";
    sassert(strlen2(word0) > 0);
    strcpy2(command[0], word0);
    sassert(strlen2(command[0]) > 0);

    char word1[MAX_SIZE] = "first";
    sassert(strlen2(word1) > 0);
    strcpy2(command[1], word1);
    sassert(strlen2(command[1]) > 0);

    char word2[MAX_SIZE] = "set";
    sassert(strlen2(word2) > 0);
    strcpy2(command[2], word2);
    sassert(strlen2(command[2]) > 0);

    char word3[MAX_SIZE] = "col1";
    sassert(strlen2(word3) > 0);
    strcpy2(command[3], word3);
    sassert(strlen2(command[3]) > 0);

    char word4[MAX_SIZE] = "=";
    //sassert(strlen2(word4) > 0);
    strcpy2(command[4], word4);
    //sassert(strlen2(command[4]) > 0);

    char word5[MAX_SIZE] = "newcol";
    sassert(strlen2(word5) > 0);
    strcpy2(command[5], word5);
    sassert(strlen2(command[5]) > 0);

    char word6[MAX_SIZE] = "where";
    sassert(strlen2(word6) > 0);
    strcpy2(command[6], word6);
    sassert(strlen2(command[6]) > 0);

    char word7[MAX_SIZE] = "key";
    sassert(strlen2(word7) > 0);
    strcpy2(command[7], word7);
    sassert(strlen2(command[7]) > 0);

    char word8[MAX_SIZE] = "=";
    sassert(strlen2(word8) > 0);
    strcpy2(command[8], word8);
    sassert(strlen2(command[8]) > 0);

    char word9[MAX_SIZE] = "key1";
    sassert(strlen2(word9) > 0);
    strcpy2(command[9], word9);
    sassert(strlen2(command[9]) > 0);

    Statement *statement = new_statement();
    DataBase *dataBase = new_data_base();
    checkUpdate(command, statement, dataBase);

    //----------------------------------------

    Table *table = new_table("first", "first", 2 * CELL_SIZE, 1, 2, NULL);

    g_bgn = nd_ptr();
    g_end = nd_ptr();
    assume(g_bgn > 0);
    assume(g_end > g_bgn);
    execute_update(statement, table);
    return 0;
}