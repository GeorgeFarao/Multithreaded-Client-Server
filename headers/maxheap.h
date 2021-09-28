
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#ifndef MAIN_MAXHEAP_H
#define MAIN_MAXHEAP_H

#endif //MAIN_MAXHEAP_H



typedef struct heapnode{
     int count;     //data
     char *name;
     struct heapnode *previous;     //pointer to previous tail, used for deleting the root
     struct heapnode *parent;       //pointer to the parent of the node
     struct heapnode *left;
     struct heapnode *right;
}heapnode;

typedef struct Heap{
    heapnode *root;     //root of heap
    heapnode *tail;     //tail  of heap, the node of whose children we insert elements to
    int count;          //number of nodes in the heap
}heap;

void InitHeap(heap *h);

heapnode* createHeapNode(int ,char *);
void Heapify(heapnode **);
void Fixtail(heap **,heapnode *);
void insertheap(heap **,int ,char *);
void printheap(heapnode *);
void removeRoot(heap *);
