#include <stdio.h>
#include <stdlib.h>
#include <stdbool.h>
#include <stdint.h>
#include <fcntl.h>
#include <unistd.h>
#include <sys/stat.h>
#include "parser.h"
#include "string.h"
#include "database.h"


InputBuffer* new_input_buffer() {
    InputBuffer* input_buffer = (InputBuffer*)xmalloc(sizeof(InputBuffer));
    input_buffer->buffer = NULL;
    input_buffer->buffer_length = 0;
    input_buffer->input_length = 0;

    return input_buffer;
}

void freeBuffer(InputBuffer* bf){
    free(bf->buffer);
    bf->buffer = NULL;
    free(bf);
    bf = NULL;
}

size_t my_getline(char **lineptr, size_t *n, FILE *stream) {
    char *bufptr = NULL;
    char *p = bufptr;
    size_t size;
    int c;

    if (lineptr == NULL) {
        return -1;
    }
    if (stream == NULL) {
        return -1;
    }
    if (n == NULL) {
        return -1;
    }
    bufptr = *lineptr;
    size = *n;

    c = fgetc(stream);
    if (c == EOF) {
        return -1;
    }
    if (bufptr == NULL) {
        bufptr = xmalloc(128);
        if (bufptr == NULL) {
            return -1;
        }
        size = 128;
    }
    p = bufptr;
    while(c != EOF) {
        if ((p - bufptr) > (size - 1)) {
            size = size + 128;
            bufptr = realloc(bufptr, size);
            if (bufptr == NULL) {
                return -1;
            }
        }
        *p++ = c;
        if (c == '\n') {
            break;
        }
        c = fgetc(stream);
    }
    *p++ = '\0';
    *lineptr = bufptr;
    *n = size;

    return p - bufptr - 1;
}

Pager* pager_open(const char* filename) {
    //we give filename and open it. We return a pager (each table has its own) that will handle access to file.
    int fd = open(filename,
                  O_RDWR | 	// Read/Write mode
                  O_CREAT,	// Create file if it does not exist
                  S_IWUSR |	// User write permission
                  S_IRUSR	// User read permission
    );
    if (fd == -1) {
        printf("Unable to open file\n");
        exit(EXIT_FAILURE);
    }

    off_t file_length = lseek(fd, 0, SEEK_END);
    Pager* pager = xmalloc(sizeof(Pager));
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

Table* new_table(const char* filename, char* name, int row_size, int num_of_keys, int num_cols, Column* columns[MAX_COLS]){
    Pager* pager = pager_open(filename);
    Table* table = (Table*)xmalloc(sizeof(Table));

    table->pager = pager;
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
    return table;
}

void freeTable(Table** tb){
    for (int i = 0; (*tb)->pager->pages[i]; i++) {
        free((*tb)->pager->pages[i]);
        (*tb)->pager->pages[i] = NULL;
    }
    free((*tb)->pager);
    for (int i = 0; i<MAX_COLS; i++) {
        free((*tb)->columns[i]);
        (*tb)->columns[i]=NULL;
    }
    free((*tb)->nbl);
    free(*tb);
    *tb=NULL;

}

void freeValue(Value* val){
    if(val) {
        for (int i = 0; i < val->size; i++) {
            free(val->name[i]);
            val->name[i] = NULL;
        }
    }
}

void freeColumn(ColNames* col){
    if(col) {
        for (int i = 0; col->name[i]!=NULL; i++) {
            free(col->name[i]);
            col->name[i] = NULL;
        }
    }
}

Statement* new_statement() {
    Statement* statement = (Statement*)xmalloc(sizeof(Statement));
    statement->table_to_create =new_table("dummy"," ",1,0,0,NULL);
    statement->table_to_insert= new_table("dummy"," ",1,0,0,NULL);
    statement->table_from_select= new_table("dummy"," ",1,0,0,NULL);
    statement->table_to_update=new_table("dummy"," ",1,0,0,NULL);
    for(int i=0; i<MAX_COLS; i++) {
        statement->values_to_insert.name[i] = NULL;
        statement->values_to_select.name[i] = NULL;
        statement->values_for_update.name[i] = NULL;
    }
    for(int i=0; i<MAX_COLS; i++){
        //statement->columns_to_update.name[i] = "";
        strcpy(statement->table_to_create->columns[i]->colName , "");
        strcpy(statement->table_to_insert->columns[i]->colName , "");
        strcpy(statement->table_from_select->columns[i]->colName , "");
        strcpy(statement->table_to_update->columns[i]->colName , "");
        statement->columns_to_update.name[i] = NULL;
    }
    statement->values_to_insert.size = 0;
    statement->values_to_select.size = 0;
    statement->values_for_update.size = 0;
    close(statement->table_to_create->pager->file_descriptor);
    close(statement->table_to_insert->pager->file_descriptor);
    close(statement->table_from_select->pager->file_descriptor);
    close(statement->table_to_update->pager->file_descriptor);
    return statement;
}

void freeStatement(Statement** st){
    freeColumn(&(*st)->columns_to_update);
    freeTable((&(*st)->table_to_insert));
    freeTable((&(*st)->table_from_select));
    freeTable((&(*st)->table_to_update));
    freeTable((&(*st)->table_to_create));
    freeValue((&(*st)->values_to_insert));
    freeValue((&(*st)->values_to_select));
    freeValue((&(*st)->values_for_update));
    free(*st);
    *st = NULL;
}

DataBase* new_data_base(){
    DataBase* dataBase = (DataBase*)xmalloc(sizeof(DataBase));
    dataBase->num_table = 0;
    return dataBase;
}

PrepareResult checkSelect(char* command[], Statement* statement){
    int i = 1;
    int k = 0;
    bool star = false;
    while (command[i] && (strcmp(command[i], "from") != 0) && k < MAX_COLS) {
        if (strcmp("*", command[i]) == 0) {
            star = true;
        }
        if (star && k > 0)
            return PREPARE_SYNTAX_ERROR;
        int len = strlen(command[i]);
        statement->values_to_select.name[k] = (char*)xmalloc(len* sizeof(char)+1);
        strcpy(statement->values_to_select.name[k++], command[i++]);
    }
    statement->values_to_select.size = k;
    statement->values_to_select.name[k] = NULL;
    if (!command[i] || k == 0) { // there is no from
        return PREPARE_SYNTAX_ERROR;
    } else {
        if (command[i+1]) //there is a name of table after the word "from"
        {
            i++;
            strcpy(statement->table_from_select->name, command[i++]);
            statement->type = STATEMENT_SELECT;
        }
        if (command[i]) {
            if (strcmp(command[i], "where") != 0) {
                return PREPARE_SYNTAX_ERROR;
            }
        }
        return PREPARE_SUCCESS;
    }
}

PrepareResult checkInsert(char* command[], Statement* statement){
    statement->type = STATEMENT_INSERT;
    strcpy(statement->table_to_insert->name, command[2]);
    int j=4;
    int k=0;
    int count = 0;
    //INSERT INTO TABLE VALUES (val1, val2, val3) VALUES_TO_INSERT = 3
    while (NULL != command[j] && k < MAX_COLS){
        int len = strlen(command[j]);
        statement->values_to_insert.name[k] = xmalloc(len+1);//, sizeof(char));
        strcpy(statement->values_to_insert.name[k++], command[j++]);
        statement->values_to_insert.name[k-1][len]='\0';
        count++;
    }
    statement->values_to_insert.size = count;
    statement->values_to_insert.name[k] = NULL;

    return PREPARE_SUCCESS;
}

PrepareResult checkUpdate(char* command[], Statement* statement, DataBase* db){
    statement->type = STATEMENT_UPDATE;
    strcpy(statement->table_to_update->name, command[1]);
    Table* table_to_compare_key = getTable(statement->table_to_update->name, db);
    if (table_to_compare_key == NULL){
        return PREPARE_TABLE_DOESNT_EXIST;
    }
    int i = 3;
    int k = 0;
    int len;
    while(command[i]!=NULL && strcmp(command[i], "where")!=0){ // checks parameter until where
        len = strlen(command[i]);
        statement->columns_to_update.name[k] = xmalloc(len+1);
        //update second set c2 = v22,c3 =  v33  where col1  =  v1 and   col2  =  v2
        strcpy(statement->columns_to_update.name[k], command[i]);
        if(!command[i+1] || !command[i+2]){ // checks there are 2 next strings
            return PREPARE_SYNTAX_ERROR;
        }
        if(strcmp(command[i+1], "=")!=0 || strcmp(command[i+2], "where")==0){ // checks this is of the form col1 = val1
            return PREPARE_SYNTAX_ERROR;
        }
        len = strlen(command[i+2]);
        statement->values_for_update.name[k] = xmalloc(len+1); //puts col1
        strcpy(statement->values_for_update.name[k], command[i+2]); //puts val1
        i+=3;
        k++;
    }
    if (i <= 3) {                           // nothing after set
        return PREPARE_SYNTAX_ERROR;
    }
    statement->values_for_update.name[k] = NULL;
    statement->values_for_update.size = k;
    statement->columns_to_update.name[k] = NULL;
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
    statement->columns_to_compare.name[m] = NULL;
    for (int n = 0; n < k; n++) { //run on all the columns we want to update
        for (int j = 0; j < MAX_COLS; j++) { //run on all the columns in the table_to_compare_key
            if (strcmp(table_to_compare_key->columns[j]->colName, statement->columns_to_update.name[n])==0 &&
               table_to_compare_key->columns[j]->key){
                printf("TRIED TO EDIT KEY COLUMN\n");
                return PREPARE_SYNTAX_ERROR;
            }
        }
    }
    return PREPARE_SUCCESS;
}

PrepareResult checkCreate(char* command[], Statement* statement){
    statement->type = STATEMENT_CREATE;
    if(!command[1] || !command[2] || !command[3]){ //at least "create table TAB (COL1)"
        return PREPARE_SYNTAX_ERROR;
    }
    strcpy(statement->table_to_create->name, command[2]);
    int num_of_keys = 0;
    int i =3;
    uint32_t k=0;
    while(command[i]){
        if (strcmp(command[i],"PRIMARY-KEY") == 0 || strcmp(command[i],"primary-key") == 0){
            if(k==0){
                return PREPARE_SYNTAX_ERROR;
            }
            statement->table_to_create->columns[k-1]->key = true;
            num_of_keys++;
            i++;
        }
        else {
            strcpy(statement->table_to_create->columns[k++]->colName, command[i++]);
        }
        if (num_of_keys >= MAX_NUM_KEYS){
            return PREPARE_TOO_MANY_KEYS;
        }
    }
    if (num_of_keys == 0){
        return PREPARE_MISS_KEY;
    }
    statement->table_to_create->num_cols = k;
    statement->table_to_create->num_of_keys = num_of_keys;
    return PREPARE_SUCCESS;
}

void saveDataToFile(DataBase* db, FILE* file){
    char str[12];
    if (file != NULL) {
        for (int i = 0; i < db->num_table; i++) {
            fprintf(file,db->Tables[i]->name);
            fprintf(file, "\t");
            sprintf(str,"%d", db->Tables[i]->num_of_keys);
            fprintf(file, str);
            fprintf(file, "\t");
            sprintf(str,"%d", db->Tables[i]->num_cols);
            fprintf(file, str);
            fprintf(file, "\t");
            sprintf(str,"%d", db->Tables[i]->row_size);
            fprintf(file, str);
            fprintf(file, "\t");
            sprintf(str,"%d", db->Tables[i]->rows_per_page);
            fprintf(file, str);
            fprintf(file, "\t");
            sprintf(str,"%d", db->Tables[i]->table_max_rows);
            fprintf(file, str);
            fprintf(file, "\t");
            for (int j=0; j<db->Tables[i]->num_cols; j++){
                fprintf(file,db->Tables[i]->columns[j]->colName);
                fprintf(file, "\t");
                if(db->Tables[i]->columns[j]->key){
                    fprintf(file,"1");
                    fprintf(file, "\t");
                }
                else {
                    fprintf(file,"0");
                    fprintf(file, "\t");
                }
            }
            fprintf(file, "\n");
        }
    }
}

void extractDataFromFile(DataBase* db, FILE* file){
    InputBuffer *input_buffer = new_input_buffer();
    size_t len = 0;
    int num_line = 0;
    fseek(file, 0, 0);
    while ((my_getline(&input_buffer->buffer, &len, file)) != -1) {
        split_line(input_buffer, db, num_line);
        num_line++;
    }
    freeBuffer(input_buffer);
}

void  split_line(InputBuffer* input_buffer, DataBase* db, int num_table){
    char* split = strtok(input_buffer->buffer, "\t ,()\r\n");
    int i = 0;
    char* name = xmalloc(CELL_SIZE);
    strcpy(name,split);
    split = strtok(NULL, "\t");
    int num_of_keys=atoi(split);
    split = strtok(NULL, "\t");
    int num_cols=atoi(split);
    split = strtok(NULL, "\t");
    int row_size=atoi(split);
    split = strtok(NULL, "\t");
    int rows_per_page=atoi(split);
    split = strtok(NULL, "\t");
    int table_max_rows=atoi(split);
    split = strtok(NULL, "\t");
    Column* columns[MAX_COLS];
    while (split != NULL){
        columns[i] = (Column*)xmalloc(sizeof(Column));
        strcpy(columns[i]->colName, split);
        split = strtok(NULL, "\t");
        if(strcmp(split,"1")==0){
            columns[i]->key = true;
        }
        else {
            columns[i]->key = false;
        }
        split = strtok(NULL, "\t ,()\r\n");
        i++;
    }
    db->Tables[num_table] = new_table(name,name,row_size,num_of_keys,num_cols,columns);
    for(int j=0;j<i;j++){
        free(columns[j]);
    }
    db->num_table++;
    free(name);
}

