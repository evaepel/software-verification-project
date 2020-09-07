# SQL Database

We implemented a SQL database in C and its verification using SeaHorn, a  parser in order to deal with the user's commands.

The queries we support are create, insert, select and update. The main data structures we use are 500-B+ trees, with persistence to disk.

### The parser

We split the SQL command with relevant separators and keep it into an array. After the command's identification we call the relevant function that executes the query (create, insert, select or update). 

### The database

The database maintains an array of pointers to the tables. Tables are partitioned in files and are indexed using B+ tree.
Primary keys are hashed to integers and serve in the indexing process.

## Verification

SeaHorn checks allocation correctness throughout all the query flow. Further more, it ensures valid data is returned by the DB and no overwrites occur when adding new entries.


### Allocation

Memory allocation, file system management and disk access are key components in the DBMS. As, the OS ensures these services via syscalls external to our code, it's critical to thoroughly check arguments and success when using these.
As part of the verification process, no memory allocation exceed 4096 bytes in our program. Doing so, we dramatically reduce undesired heavy allocations and improve memory control over our DBMS.
We showcase below some of these checks.

```bash
void* xmalloc(size_t size){
        sassert(size <= 4096);
...}
```

We allocate constant sizes according to gradations:
```bash
 if (size <= 512)
    p = (malloc(512));
 else if (size <= 1024)
    p = (malloc(1024));
 else if (size <= 2048)
    p = (malloc(2048));
 else if (size <= 4096)
    p = (malloc(4096));
```

After calling malloc, we assume the allocation succeeds:

```bash
assume(((ptrdiff_t)p) > 0);
```

In main_malloc.c we checked the xmalloc function with typical objects we use in our database.
```bash
sea pf -O0 -g -m64 main_xmalloc.c --inline --log=cex --cex=out.ll
```
### Query verification

Queries represent the only entry point to the program. Thus, asserting good behavior at the query level represents a 2-fold benefit. First off, it validates parsing and string treatment (which is known to be delicate in C). Second, as the entry point of our program, making assertion on query result allows us to create many assumption rules later in the flow. Therefore, we check the SQL command contains valid values, especially no empty strings.

```bash
sassert(strlen2(command[i]) > 0);
```

Afterwards we call the relevant function corresponding to the query and we assume the length of command[i] value is bigger than zero before copying it to another struct:
```bash

assume(strlen2(command[i])>0);
strcpy2(statement->values_to_insert.name[j], command[i]);
sassert(strlen2(statement->values_to_insert.name[j])>0);
```

### Create

In order to ensure we do not overwrite an existing table when adding a new table to our DB we used  global ghost variables:

```bash
static int8_t* g_bgn;         // Points to the beginning of the buffer of arbitrary table.
static int8_t* g_end;         // Points to the end of the buffer of this arbitrary table.
static bool g_active;         // True if g_bgn and g_end points to the beginning and end of the buffer of the table. Will be turned on only once so that we "save" the address of                               // only one Table.  
```
Plus, in order to compare the addresses of the different existing tables to the arbitrary one which g_bgn and g_end point on, we added a boolean: table_saved in the struct of the tables that we turn on when keeping the addresses. Here is the code:
```bash
typedef struct {...
       bool table_saved;      //True when g_bgn and g_end point on the corresponding table.
...} Table;

Table* new_table(char* name, int row_size, int num_cols){...
    table->saved = false;
    if (!g_active && nd()) {//g_active == 0 and nd()!=0f
        g_active = !g_active;   //g_active = 1
        table->saved = true;
        assume((int8_t*)table == g_bgn);
        assume(g_bgn + sizeof(Table) == g_end);
    }...
}
```
In the main we check in a for loop the following:

If g_active is true  and the current table is not the one we saved the buffer of, we check the current table doesn't overwrite the one we saved.      
The verification is done on five tables.


```bash
    DataBase *dataBase  = new_data_base(0);
    dataBase->Tables[0] = new_table("first","first", 2*CELL_SIZE, 1, 2, NULL);
    dataBase->Tables[1] = new_table("second","second", 4*CELL_SIZE,1, 4, NULL);
    dataBase->Tables[2] = new_table("third","third", 2*CELL_SIZE,1, 2, NULL);
    dataBase->Tables[3] = new_table("fourth","fourth", 3*CELL_SIZE,1, 3, NULL);
    dataBase->Tables[4] = new_table("fifth","fifth", 8*CELL_SIZE,1, 8, NULL);
    dataBase->num_table = 5;

for (int i = 0; i < dataBase->num_table; i++) {
    Table *table = dataBase->Tables[i];
    if (g_active && !table->saved) {
        sassert((int8_t *) table != g_bgn);
    }
}
```
```bash
sea pf -O0 -g -m64 main_create.c --inline --log=cex --cex=out.ll
```
### Insert
In the insert query we do the basic verification we mentioned above but we didn't add more verification.

Indeed, We wanted to check with ghost variables that the address which we insert data in is located inside the required line and does not exceed from its bounds.
However, after adding the relevant functions SeaHorn didn't handle this verification (didn't return sat nor unsat but was blocked). We then keep only the query verification.
```bash
sea pf -O0 -g -m64 main_insert.c --inline --log=cex --cex=out.ll
```

### Select
We ensure when accessing the rows in the DB we do not exceed from the row bounds when extracting the data 
for each row of the select. In order to do that, we used global ghost variables. For each extraction we checked we did not go beyond the end of the row's buffer:

```bash
while(i<MAX_COLS && i<destination->values_to_select.size ){
    sassert(source + i * CELL_SIZE <= g_end - CELL_SIZE );
    memmove(&(results[0]), source + i * CELL_SIZE, CELL_SIZE);
    i++;
    }
```
```bash
sea pf -O0 -g -m64 main_select.c --inline --log=cex --cex=out.ll
```
### Update

When updating some cell in a row, we need to make sure we do not exceed from the row bounds. We check it in a similar way as in the select verification:
```bash
 while (i < MAX_COLS && cols_to_update[i] != -1) {
       assume(g_end == cursor_value(cursor)+table->row_size);
       sassert(cursor_value(cursor) + i * CELL_SIZE <= g_end - CELL_SIZE );
       memcpy(cursor_value(cursor) + cols_to_update[i] * CELL_SIZE, statement->values_for_update.name[i], CELL_SIZE);
       i++;
       }
```
```bash
sea pf -O0 -g -m64 main_update.c --inline --log=cex --cex=out.ll
```

## What we took from the project

From that projects we learned about two main subjects: verification and database implementation.

Database Implementation - We learn how to deal with files and persistence to disk and what are the best data structures to use in order to implement a DB.

Verification - During the lessons and the project design we discovered why it's critical to check the code correctness and how to check it.
The SeaHorn tool was new for both of us and the fact that it generated counter-examples was very helpful and enriching.
One thing we especially appreciated is the use of non deterministic values in order to cover every possible case, thing we cannot do with simple tests.


Finally we really want to thank Shachar and Yakir for the support, invest and availability during all the semester and the summer. 
