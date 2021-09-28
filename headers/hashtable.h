
#include <stdio.h>
#include <string.h>
#include <stdlib.h>
#include "avl.h"

#ifndef MAIN_HASHTABLE_H
#define MAIN_HASHTABLE_H

#endif //MAIN_HASHTABLE_H

typedef struct htentry{     //entry of the bucket
    char *ID;
    treenode *root;
}htentry;

typedef struct bucket{
    char *storage;
}bucket;

typedef struct htdata{      //data of the bucket
    int count;  //number of entries
    int max;    //max number of entries
    bucket *next;   //pointer to the next bucket
}htdata;


