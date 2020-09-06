//
// Created by Nataf on 25/08/2020.
//

//
// Created by Nataf on 23/08/2020.
//
#include "main_create.h"

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

void *get_page(Pager *pager, uint32_t page_num) {
    if (page_num > TABLE_MAX_PAGES) {
        printf("Tried to fetch page number out of bounds. %d > %d\n", page_num,
               TABLE_MAX_PAGES);
        exit(EXIT_FAILURE);
    }

    if (pager->pages[page_num] == NULL) {
        // Cache miss. Allocate memory and load from file.
        void *page = xmalloc(PAGE_SIZE);
        memset(page, 0, 0);
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

uint32_t *leaf_node_num_cells(void *node) {
    return node + LEAF_NODE_NUM_CELLS_OFFSET;
}

uint32_t *leaf_node_next_leaf(void *node) {
    return node + LEAF_NODE_NEXT_LEAF_OFFSET;
}

void initialize_leaf_node(void *node) {
    set_node_type(node, NODE_LEAF);
    set_node_root(node, false);
    *leaf_node_num_cells(node) = 0;
    *leaf_node_next_leaf(node) = 0;  // 0 represents no sibling
}

Table *
new_table(const char *filename, char *name, int row_size, int num_of_keys, int num_cols, Column *columns[MAX_COLS]) {
    Pager* pager = pager_open(filename);
    Table* table = (Table*)xmalloc(sizeof(Table));

    //table->pager = pager;
    strcpy(table->name, name);
    for(int i=0; i<MAX_COLS; i++){
        table->columns[i] = (Column*)xmalloc(sizeof(Column));
        table->columns[i]->key = false;
        strcpy(table->columns[i]->colName, "");
    }
    table->num_of_keys = num_of_keys;
    table->num_cols = num_cols;

    table->row_size = row_size;
    table->nbl = new_nbl(row_size);
    table->rows_per_page = table->nbl->leafNodeMaxCells;
    table->table_max_rows = table->rows_per_page*TABLE_MAX_PAGES;
    int i = 0;
    while(i<num_cols && strcmp(columns[i]->colName,"") != 0) {
        strcpy(table->columns[i]->colName,columns[i]->colName);
        if (columns[i]->key){
            table->columns[i]->key = true;
        } else{
            table->columns[i]->key = false;
        }
        i++;
    }
    strcpy(table->columns[num_cols]->colName,"");
    table->root_page_num = 0;
    if (pager->num_pages == 0) {
        // New database file. Initialize page 0 as leaf node.
        void* root_node = get_page(pager, 0);
        initialize_leaf_node(root_node);
        set_node_root(root_node, true);
    }
    //seahorn verification
    table->saved = false;
    if (!g_active && nd()) {//g_active == 0 and nd()!=0
        g_active = !g_active;   //g_active = 1
        table->saved = true;
        assume((void*)table == g_bgn);
        assume(g_bgn + sizeof(Table) == g_end);
    }
    else {
        assume((void*)table > g_end);
    }
    return table;
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

PrepareResult checkCreate(char *command[], Statement *statement) {
    statement->type = STATEMENT_CREATE;
    if (!command[1] || !command[2] || !command[3]) { //at least "create table TAB (COL1)"
        return PREPARE_SYNTAX_ERROR;
    }
    assume(strlen2(command[2]) > 0);
    strcpy2(statement->table_to_create->name, command[2]);
    sassert(strlen2(statement->table_to_create->name) > 0);
    int num_of_keys = 0;
    int i = 3;
    uint32_t k = 0;
    while (command[i]) {
        if (strcmp(command[i], "PRIMARY-KEY") == 0 || strcmp(command[i], "primary-key") == 0) {
            if(k == 0){
                return PREPARE_SYNTAX_ERROR;
            }
            statement->table_to_create->columns[k - 1]->key = true;
            num_of_keys++;
            i++;
        } else {
            strcpy2(statement->table_to_create->columns[k++]->colName, command[i++]);
        }
        if (num_of_keys >= MAX_NUM_KEYS) {
            return PREPARE_TOO_MANY_KEYS;
        }
    }

    if (num_of_keys == 0) {
        return PREPARE_MISS_KEY;
    }
    statement->table_to_create->num_cols = k;
    statement->table_to_create->num_of_keys = num_of_keys;
    return PREPARE_SUCCESS;
}

ExecuteResult executeCreate(Statement *statement, DataBase *dataBase) {
    int row_size = CELL_SIZE * statement->table_to_create->num_cols;
    dataBase->Tables[dataBase->num_table] = new_table(statement->table_to_create->name,
                                                      statement->table_to_create->name, row_size,
                                                      statement->table_to_create->num_of_keys,
                                                      statement->table_to_create->num_cols,
                                                      statement->table_to_create->columns);

    if (!g_active && nd()) {//g_active == 0 and nd()!=0
        g_active = !g_active;   //g_active = 1
        dataBase->Tables[dataBase->num_table]->saved = true;
        assume((void *) dataBase->Tables[dataBase->num_table] == g_bgn);
        assume(g_bgn + sizeof(Table) == g_end);
    } else {
        assume((void *) dataBase->Tables[dataBase->num_table] > g_end);
    }
    dataBase->num_table++;
    return EXECUTE_SUCCESS;
}

int main(int argc, char *argv[]) {
    char *command[MAX_SIZE];

    char word0[MAX_SIZE] = "create";
    sassert(strlen2(word0) > 0);
    command[0] = (char *) xmalloc(strlen2(word0) + 1);
    strcpy2(command[0], word0);
    sassert(strlen2(command[0]) > 0);

    char word1[MAX_SIZE] = "table";
    sassert(strlen2(word1) > 0);
    command[1] = (char *) xmalloc(strlen2(word1) + 1);
    strcpy2(command[1], word1);
    sassert(strlen2(command[1]) > 0);

    char word2[MAX_SIZE] = "first";
    sassert(strlen2(word2) > 0);
    command[2] = (char *) xmalloc(strlen2(word2) + 1);
    strcpy2(command[2], word2);
    sassert(strlen2(command[2]) > 0);

    char word3[MAX_SIZE] = "col1";
    sassert(strlen2(word3) > 0);
    command[3] = (char *) xmalloc(strlen2(word3) + 1);
    strcpy2(command[3], word3);
    sassert(strlen2(command[3]) > 0);

    char word4[MAX_SIZE] = "col2";
    sassert(strlen2(word4) > 0);
    command[4] = (char *) xmalloc(strlen2(word4) + 1);
    strcpy2(command[4], word4);
    sassert(strlen2(command[4]) > 0);

    Statement *statement = new_statement();
    checkCreate(command, statement);
//---------------------------------------------------------------------------------------------------
    g_bgn = nd_ptr();
    g_end = nd_ptr();
    assume(g_bgn > 0);
    assume(g_end > g_bgn);
    g_active = false;

    DataBase *dataBase = new_data_base();

    Statement *s1 = new_statement();
    Statement *s2 = new_statement();
    Statement *s3 = new_statement();

    strcpy2(s1->table_to_create->name, "tab1");
    strcpy2(s2->table_to_create->name, "tab2");
    strcpy2(s3->table_to_create->name, "tab3");

    s1->table_to_create->num_of_keys = 1;
    s2->table_to_create->num_of_keys = 1;
    s3->table_to_create->num_of_keys = 1;

    s1->table_to_create->num_cols = 1;
    s2->table_to_create->num_cols = 1;
    s3->table_to_create->num_cols = 1;
    dataBase->Tables[0] = new_table("first","first", 2*CELL_SIZE, 1, 2, NULL);
    dataBase->Tables[1] = new_table("second","second", 4*CELL_SIZE,1, 4, NULL);
    dataBase->Tables[2] = new_table("third","third", 2*CELL_SIZE,1, 2, NULL);
    dataBase->Tables[3] = new_table("fourth","fourth", 3*CELL_SIZE,1, 3, NULL);
    dataBase->Tables[4] = new_table("fifth","fifth", 8*CELL_SIZE,1, 8, NULL);
    dataBase->num_table = 5;

    for (int i = 0; i < dataBase->num_table; i++) {
        Table *table = dataBase->Tables[i];
        if (g_active && !table->saved) {
            sassert((void *) table != g_bgn);
        }
    }
    return 0;
}