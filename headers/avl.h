
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include "records.h"

#ifndef MAIN_AVL_H
#define MAIN_AVL_H

#endif //MAIN_AVL_H

typedef struct treenode{
    char *date;
    struct record* rec;
    struct treenode *left;
    struct treenode *right;
    int height;
} treenode;

/*treenode *createnode(char *,struct record **);
int max(int,int);
int height(treenode *);
treenode *rightRotation(treenode *);
treenode *leftRotation(treenode *);
int BalanceFactor(treenode *);
treenode *treeinsert(treenode *,record **);
void printTree(treenode *);
void countnodes(recordNode **head, int *range1, int *range2, int *range3, int *range4, char *key);
int datecompare(char *date1, char *date2);*/