//
// Created by Nataf on 02/06/2020.
//
#include <stdio.h>
#include <stdlib.h>


#include "alloc_manager.h"
void* xmalloc(size_t size){
    if (size <= 512)
        return (malloc(512));
    else if (size <= 1024)
        return (malloc(1024));
    else if (size <= 2048)
        return (malloc(2048));
    else if (size <= 4096)
        return (malloc(4096));
    else {
        //printf("size = %u \n", size);
        printf("ERROR XMALLOC - TOO BIG\n");
    }
    return NULL;
}