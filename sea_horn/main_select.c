//
// Created by Nataf on 23/08/2020.
//
#include "main_select.h"

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

void *xmalloc(size_t size) {
    void *p = NULL;
    assume(size <= 4096);
    if (size <= 512)
        p = (malloc(512));
    else if (size <= 1024)
        p = (malloc(1024));
    else if (size <= 2048)
        p = (malloc(2048));
    else if (size <= 4096)
        p = (malloc(4096));
    assume(((ptrdiff_t) p) > 0);
    return p;
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

void *leaf_node_cell(void *node, uint32_t cell_num, NodeBodyLayout *nodeBodyLayout) {
    return node + LEAF_NODE_HEADER_SIZE + cell_num * nodeBodyLayout->leafNodeCellSize;
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

void *leaf_node_value(void *node, uint32_t cell_num, NodeBodyLayout *nodeBodyLayout) {
    return leaf_node_cell(node, cell_num, nodeBodyLayout) + nodeBodyLayout->leafNodeKeySize;
}

void *cursor_value(Cursor *cursor) {
    uint32_t page_num = cursor->page_num;
    void *page = get_page(cursor->table->pager, page_num);
    return leaf_node_value(page, cursor->cell_num, cursor->table->nbl);
}

NodeType get_node_type(void *node) {
    uint8_t value = *((uint8_t *) (node + NODE_TYPE_OFFSET));
    return (NodeType) value;
}

uint32_t *leaf_node_num_cells(void *node) {
    return node + LEAF_NODE_NUM_CELLS_OFFSET;
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

Cursor *table_find(Table *table, uint32_t key) {
    uint32_t root_page_num = table->root_page_num;
    void *root_node = get_page(table->pager, root_page_num);
    if (get_node_type(root_node) == NODE_LEAF) {
        return leaf_node_find(table, root_page_num, key);
    } else {
        return NULL;
    }
}

Cursor *table_start(Table *table) {
    sassert(sizeof(Cursor) <= 4096);
    Cursor *cursor = table_find(table, 0);
    void *node = get_page(table->pager, cursor->page_num);
    uint32_t num_cells = *leaf_node_num_cells(node);
    cursor->end_of_table = (num_cells == 0);

    return cursor;
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

void set_node_type(void *node, NodeType type) {
    uint8_t value = type;
    *((uint8_t *) (node + NODE_TYPE_OFFSET)) = value;
}

void set_node_root(void *node, bool is_root) {
    uint8_t value = is_root;
    *((uint8_t *) (node + IS_ROOT_OFFSET)) = value;
}

uint32_t *leaf_node_next_leaf(void *node) {
    return node + LEAF_NODE_NEXT_LEAF_OFFSET;
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

void initialize_leaf_node(void *node) {
    set_node_type(node, NODE_LEAF);
    set_node_root(node, false);
    *leaf_node_num_cells(node) = 0;
    *leaf_node_next_leaf(node) = 0;  // 0 represents no sibling
}

Table *
new_table(const char *filename, char *name, int row_size, int num_of_keys, int num_cols, Column *columns[MAX_COLS]) {
    Pager *pager = pager_open(filename);
    Table *table = (Table *) xmalloc(sizeof(Table));

    //table->pager = pager;
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
    strcpy(table->columns[num_cols]->colName, "");
    table->root_page_num = 0;
    if (pager->num_pages == 0) {
        // New database file. Initialize page 0 as leaf node.
        void *root_node = get_page(pager, 0);
        initialize_leaf_node(root_node);
        set_node_root(root_node, true);
    }
    table->saved = false;
    return table;
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

int indexCol(Table table, char *name) {
    for (int i = 0; i < table.num_cols; i++) {
        if (strcmp(table.columns[i]->colName, name) == 0) {
            return i;
        }
    }
    return -1;
}

PrepareResult checkSelect(char command[MAX_SIZE][MAX_SIZE], Statement *statement) {
    int i = 1;
    int k = 0;
    bool star = false;
    if (strcmp("*", command[i]) == 0) {
        star = true;
    }
    if (star && k > 0)
        return PREPARE_SYNTAX_ERROR;
    int len = strlen(command[i]);
    statement->values_to_select.name[k] = (char *) xmalloc(len * sizeof(char) + 1);
    assume(strlen2(command[i]) > 0);
    strcpy2(statement->values_to_select.name[k++], command[i++]);
    sassert(strlen2(statement->values_to_select.name[k - 1]) > 0);
    statement->values_to_select.size = k;

    statement->values_to_select.name[k] = NULL;
    //unsigned int j = nd();
    if (!command[i] || k == 0) { // there is no from
        return PREPARE_SYNTAX_ERROR;
    } else if (command[i + 1]) //there is a name of table after the word "from"
    {
        i++;
        assume(strlen2(command[i]) > 0);
        strcpy2(statement->table_from_select->name, command[i++]);
        sassert(strlen2(statement->table_from_select->name) > 0);
        statement->type = STATEMENT_SELECT;
    }
    return PREPARE_SUCCESS;
}

void deserialize_row_select(void *source, Statement *destination, Table *table) {
    assume(g_end == source + table->row_size);
    char results[1][255];
    int i = 0;
    assume(destination->values_to_select.size >= 0);
    assume(destination->values_to_select.size <= table->num_cols);

    while (i < MAX_COLS && i < destination->values_to_select.size) {
        sassert(source + i * CELL_SIZE <= g_end - CELL_SIZE);
        memmove(&(results[0]), source + i * CELL_SIZE, CELL_SIZE);
        printf("%s \t", results[0]);
        i++;
    }
}

ExecuteResult execute_select(Statement *statement, Table *table) {
    if (table == NULL) {
        return EXECUTE_TABLE_DOESNT_EXIST;
    }
    int cols[MAX_COLS];
    int last = 0;

    if (strcmp(statement->values_to_select.name[0], "*") == 0) {
        statement->values_to_select.size = table->num_cols;
        for (int i = 0; i < MAX_COLS; i++) cols[i] = i;
    }

    Cursor *cursor = table_start(table);

    if (strcmp(statement->values_to_select.name[0], "*") == 0) {
        while (!(cursor->end_of_table)) {
            deserialize_row_select(cursor_value(cursor), statement, table);
            printf("\n");
            cursor_advance(cursor);
        }
        free(cursor);
        return EXECUTE_SUCCESS;
    }
    for (int i = 0; statement->values_to_select.name[i]; i++) {
        cols[i] = indexCol(*table, statement->values_to_select.name[i]);
        last = i;
        if (cols[i] == -1) {
            free(cursor);
            return EXECUTE_COL_DOESNT_EXIST;
        }
    }
    while (!(cursor->end_of_table)) {
        cols[last + 1] = -1;
        deserialize_row_select(cursor_value(cursor), statement, table);
        cursor_advance(cursor);
        printf("\n");
    }
    free(cursor);
    return EXECUTE_SUCCESS;
}

int main(int argc, char *argv[]) {
    char command[MAX_SIZE][MAX_SIZE];

    char word0[MAX_SIZE] = "select";
    sassert(strlen2(word0) > 0);
    strcpy2(command[0], word0);
    sassert(strlen2(command[0]) > 0);

    char word1[MAX_SIZE] = "*";
    sassert(strlen2(word1) > 0);
    strcpy2(command[1], word1);
    sassert(strlen2(command[1]) > 0);

    char word2[MAX_SIZE] = "from";
    sassert(strlen2(word2) > 0);
    strcpy2(command[2], word2);
    sassert(strlen2(command[2]) > 0);

    char word3[MAX_SIZE] = "first";
    sassert(strlen2(word3) > 0);
    strcpy2(command[3], word3);
    sassert(strlen2(command[3]) > 0);

    Statement *statement = new_statement();
    checkSelect(command, statement);
//---------------------------------------------------------------------------------------------------

    Table *table = new_table("first", "first", 2 * CELL_SIZE, 1, 2, NULL);
    Cursor *cursor = table_start(table);
    void *source = cursor_value(cursor);
    g_end = nd_ptr();
    assume(g_bgn > 0);
    assume(g_end > g_bgn);
    deserialize_row_select(source, statement, table);

    return 0;
}