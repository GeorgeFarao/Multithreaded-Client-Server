
#include "maxheap.h"

void InitHeap(heap *h){ //intialize heap
    h->root=NULL;
    h->tail=NULL;
    h->count=0;
}

heapnode* createHeapNode(int count,char *name){
    heapnode *tmp=malloc(sizeof(heapnode));     //allocate memory for the node
    tmp->left=NULL;
    tmp->right=NULL;
    tmp->parent=NULL;
    tmp->previous=NULL;
    tmp->count=count;
    tmp->name=malloc(strlen(name)+1);
    strcpy(tmp->name,name);
    return tmp;
}

void Heapify(heapnode **node){      //restore heap
    if((*node)->parent!=NULL){
        if((*node)->parent->count<(*node)->count){      //if child has greater value than it's parent swap their values
            int tmp=(*node)->count;
            char *ctmp=(*node)->name;
            (*node)->count=(*node)->parent->count;
            (*node)->name=(*node)->parent->name;
            (*node)->parent->count=tmp;
            (*node)->parent->name=ctmp;
            Heapify(&((*node)->parent));
        }
    }
}

void Fixtail(heap **h,heapnode *node){
    if(node->parent==NULL){    //if node doesn't have a parent, we are in the root
        (*h)->tail=node;
        while((*h)->tail->left!=NULL){      //while left nodes exit
            (*h)->tail=(*h)->tail->left;    //make tail the most left node
        }
        return;
    }
    if(node->parent->left==node){       //if we are in parent's left child
        (*h)->tail=node->parent->right;
        while((*h)->tail->left!=NULL){
            (*h)->tail=(*h)->tail->left;    //make tail the most left node
        }
    }
    else if(node->parent->right==node)      //if we are in parent's right child
        Fixtail(h,node->parent);            //call function for node's parent
}

void  reHeapify(heapnode **node){
    if(*node==NULL || (*node)->left==NULL)      //node is empty or has no children
        return;
    heapnode *tmp= (*node)->left;
    if((*node)->right!=NULL)    //node has a right child
        if((*node)->left->count<(*node)->right->count)      //if right's count is greater than left's tmp becomes right
            tmp=(*node)->right;
    if(tmp->count>(*node)->count){      //if tmp has greater value than node swap their values
        int temp=(*node)->count;
        char *ctemp=(*node)->name;
        (*node)->count=tmp->count;
        (*node)->name=tmp->name;
        tmp->count=temp;
        tmp->name=ctemp;
        reHeapify(&tmp);    //call function for tmp
    }
}

void printheap(heapnode *node){     //function to print heap
    if(node!=NULL) {

        printf("%s %d\n", node->name,node->count);  //print name and count
        printheap(node->left);      //go to left child
        printheap(node->right);     //go to right child
    } else printf("NULL\n");
}

void insertheap(heap **h,int count,char *name){
    if((*h)->root==NULL) {      //root is null
        heapnode *tmp=createHeapNode(count, name);  //create node
        (*h)->root=tmp;     //booth root and tail point to the new node
        (*h)->tail=tmp;
        (*h)->count++;      //increase counter
        return;
    }


    if((*h)->tail->left==NULL){     //if tail's left child is null we can insert new node there
        (*h)->tail->left=createHeapNode(count,name);
        (*h)->count++;
        (*h)->tail->left->parent=(*h)->tail;    //update new node's parent
        Heapify(&((*h)->tail->left));       //restore heap
    }
    else{       //tail's right child is null
        (*h)->tail->right=createHeapNode(count,name);
        (*h)->count++;
        (*h)->tail->right->parent=(*h)->tail;   //update new node's parent
        Heapify(&((*h)->tail->right));
       heapnode *tnode=(*h)->tail;      //since tail's both children now exist we need to find the new tail

        Fixtail(h,(*h)->tail);
        (*h)->tail->previous=tnode;     //we holds the previous tail in the new tail's previous pointer
    }

}

void removeRoot(heap *h){
    if(h->root==NULL){
        printf("Heap is empty\n");
        return;
    }
    if(h->root==h->tail && h->root->left==NULL && h->root->right==NULL){      //only root node exists
        heapnode *tmp=h->root;
        h->root=NULL;
        h->tail=NULL;
        h->count=0;
        free(tmp);
        return;
    }
    else{
        if (h->tail->right!=NULL){      //tails has a right child
            int tmp=h->tail->right->count;      //swap tail's right child data with root's data
            char *ctmp=h->tail->right->name;
            h->tail->right->count=h->root->count;
            h->tail->right->name=h->root->name;
            h->root->count=tmp;
            h->root->name=ctmp;
            free(h->tail->right);   //free tail's right child
            h->tail->right=NULL;
            reHeapify(&(h->root));  //restore heap
            h->count--;
        }
        else if(h->tail->left!=NULL){   //left child exists
           int tmp=h->tail->left->count;    //swap tail's left child data with root's data
           char *ctmp=h->tail->left->name;
           h->tail->left->count=h->root->count;
           h->tail->left->name=h->root->name;
           h->root->count=tmp;
           h->root->name=ctmp;
           free(h->tail->left);     //free tail's left child
           h->tail->left=NULL;
           reHeapify(&(h->root));   //restore heap
           h->count--;
       }   else{    //if both children null
           h->tail=h->tail->previous;   //tail becomes the previous tail
           removeRoot(h);
           h->count++;
       }
    }
}
