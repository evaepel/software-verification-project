#define _GNU_SOURCE
#include <stdio.h>
#include <stdlib.h>
#include <stdbool.h>
//#include "parser.h"
#include "database.h"
#include "string.h"
#include "parser.h"

#define MAX_SIZE 100

PrepareResult split_statement(InputBuffer* input_buffer, Statement* statement, DataBase* dataBase){
    char* command[MAX_SIZE];
    char* split = strtok(input_buffer->buffer, " ,()\r\n");
    int i = 0;
    size_t len;
    while (split != NULL){
        len = strlen(split);
        char* word = (char*)malloc(len + 1);
		strcpy(word, split);
        word[len] = '\0';
        command[i] = (char*)malloc(len + 1);
        strcpy(command[i], word);
        split = strtok(NULL, " ,()\r\n");
        i++;
        free(word);
    }
    command[i] = NULL;
    PrepareResult pr ;
    if (strcmp(command[0], "insert") == 0 && strcmp(command[1], "into") == 0 && strcmp(command[3], "values") == 0 && i >= 5){
        pr = checkInsert(command, statement);
    }
    else if (strcmp(command[0], "select")==0){
        pr =  checkSelect(command, statement);
    }
    else if (strcmp(command[0], "update")==0 && strcmp(command[2], "set")==0) {
        pr =  checkUpdate(command, statement, dataBase);
    }
    else if (strcmp(command[0], "create")==0 && strcmp(command[1], "table")==0) {
        pr =  checkCreate(command, statement);
    }
    else pr = PREPARE_UNRECOGNIZED_STATEMENT;
    for(int j =0; command[j]; j++){
        free(command[j]);
    }
    return pr;
}

int main(int argc, char* argv[]) {
    FILE * file= fopen("output", "a+");
    DataBase *db = new_data_base();
    extractDataFromFile(db,file);
    InputBuffer *input_buffer = new_input_buffer();
    FILE * fp;
    size_t len = 0;
    fp = fopen("small_complete", "r");
    if (fp == NULL)
        exit(EXIT_FAILURE);
    Statement* statement;
    int c = 0;
    while ((my_getline(&input_buffer->buffer, &len, fp)) != -1) {
        if(c > 0) freeStatement(&statement);
        statement = new_statement();
        c++;
        if  (input_buffer->buffer[0] == '*' || strcmp(input_buffer->buffer, "\r\n")==0){
            continue;
        }
        printf("Dealing with the line: \"%s\" \n", input_buffer->buffer);
        switch (split_statement(input_buffer, statement, db)) {
            case (PREPARE_SUCCESS):
                break;
            case (PREPARE_UNRECOGNIZED_STATEMENT):
                printf("Unrecognized keyword at start of \"%s\".\n\n",
                       input_buffer->buffer);
                continue;
            case(PREPARE_SYNTAX_ERROR):
                printf("Syntax error at start of: %s \n\n", input_buffer->buffer);
                continue;
            case(PREPARE_MISS_KEY):
                printf("Try to create a table with no key\n");
                continue;
            case(PREPARE_TOO_MANY_KEYS):
                printf("Try to create a table with too many key\n");
                continue;
        }
        switch (execute_statement(statement, db)){
            case(EXECUTE_SUCCESS):
                printf("Executed.\n");
                break;
            case(EXECUTE_MISMATCH_NUM_VALUES):
                printf("Error mismatch num values\n");
                break;
            default:
                printf("ERROR\n");
        }

    }
    freopen("output","wa+",file);
    saveDataToFile(db,file);
    db_close(db);
    fclose(file);
    freeStatement(&statement);
    freeBuffer(input_buffer);
    fclose(fp);
    return 0;
}