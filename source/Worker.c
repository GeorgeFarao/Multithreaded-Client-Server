#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <fcntl.h>
#include <time.h>
#include <signal.h>
#include <sys/types.h>
#include <sys/wait.h>
#include <sys/stat.h>
#include <sys/select.h>
#include <errno.h>
#include <dirent.h>
#include <poll.h>
#include <limits.h>
//#include <sys/sockets.h>
#include <netinet/in.h>
#include <netdb.h>
#include <arpa/inet.h>
#include <ifaddrs.h>
#include "hashtable.h"
#include "maxheap.h"


int datecompare(char *date1, char *date2) {
    int res;
    res = strcmp(date1 + 6, date2 + 6);
    if (res > 0) {
        return 1;
    } else if (res < 0)
        return -1;
    else {
        res = strcmp(date1 + 3, date2 + 3);
        if (res > 0) {
            return 1;
        } else if (res < 0) {
            return -1;
        } else {
            res = strcmp(date1, date2);
            if (res > 0) {
                return 1;
            } else if (res < 0) {
                return -1;
            } else
                return 0;
        }
    }
}


int max(int x, int y) {   //returns the max off two elements
    if (x > y)
        return x;
    else
        return y;
}

int height(treenode *node) {     //returns the height of a node
    if (node != NULL)
        return node->height;
    return 0;
}


treenode *createnode(char *date, struct record **rec) {
    treenode *tmp = malloc(sizeof(treenode));
    tmp->date = malloc(strlen(date) + 1);
    strcpy(tmp->date, date);
    tmp->left = NULL;
    tmp->right = NULL;
    tmp->rec = *rec;
    tmp->height = 1;
    return tmp;
}

treenode *rightRotation(treenode *r) {
    treenode *l;
    treenode *tmp;

    l = r->left;
    tmp = l->right;

    l->right = r;     //l's right node becomes r
    r->left = tmp;    //r's left becomes tmp


    r->height = max(height(r->left), height(r->right) + 1);      //change heights
    l->height = max(height(l->left), height(l->right) + 1);
    return l;
}

treenode *leftRotation(treenode *l) {
    treenode *r;
    treenode *tmp;

    r = l->right;
    tmp = r->left;

    r->left = l;  //r's left node becomes l
    l->right = tmp;     //l's rigjt node becomes tmp

    l->height = max(height(l->left), height(l->right) + 1);  //change heights
    r->height = max(height(r->left), height(r->right) + 1);
    return r;
}

int BalanceFactor(treenode *node) {      //returns balance factor
    if (node != NULL)
        return height(node->left) - height(node->right);
    return 0;
}

void printTree(treenode *root) {
    if (root != NULL) {
        printf("%s  ", root->date);
        printTree(root->left);
        printTree(root->right);
    }
}

treenode *treeinsert(treenode *root, record **rec) {
    char *date = (*rec)->date;
    if (root == NULL) {     //empty node
        return createnode(date, rec);
    }

    if (datecompare(date, (root->date)) <= 0)       //date is less than root's date
        root->left = treeinsert(root->left, rec);
    else                                        //date is greater than root's date
        root->right = treeinsert(root->right, rec);
    root->height = max(height(root->left), height(root->right)) + 1;

    int balancefact = BalanceFactor(root);
    if ((balancefact < -1) && (datecompare(date, root->right->date) >
                               0)) //right's height is greater than left's and date is greater than root's date
        return leftRotation(root);
    if ((balancefact > 1) && (datecompare(date, root->left->date) <
                              0))   //left's height is greater than right's and date is greater than root's date
        return rightRotation(root);
    if ((balancefact < -1) && (datecompare(date, root->right->date) <
                               0)) {    //right's height is greater than left's and date is less than root's date
        root->right = rightRotation((root->right));
        return leftRotation(root);
    }
    if ((balancefact > 1) && (datecompare(date, root->left->date) >
                              0)) {      //left's height is greater than right's and date is less than root's date
        root->left = leftRotation(root->left);
        return rightRotation(root);
    }
    return root;
}

void countnodes(recordNode **head, int *range1, int *range2, int *range3, int *range4, char *key) {
    if (*head == NULL)
        return;
    recordNode *cur = *head;
    if (cur->next == NULL) {
        if (strcmp(cur->record->diseaseID, key) == 0) {
            if (cur->record->age >= 0 && cur->record->age <= 20)
                (*range1)++;
            else if (cur->record->age > 20 && cur->record->age <= 40)
                (*range2)++;
            else if (cur->record->age > 40 && cur->record->age <= 60)
                (*range3)++;
            else
                (*range4)++;
        }
    }
    while (cur->next != NULL) {
        if (strcmp(cur->record->diseaseID, key) == 0) {
            if (cur->record->age >= 0 && cur->record->age <= 20)
                (*range1)++;
            else if (cur->record->age > 20 && cur->record->age <= 40)
                (*range2)++;
            else if (cur->record->age > 40 && cur->record->age <= 60)
                (*range3)++;
            else
                (*range4)++;
        }
        cur = cur->next;
        if (cur->next == NULL) {
            if (strcmp(cur->record->diseaseID, key) == 0) {
                if (cur->record->age >= 0 && cur->record->age <= 20)
                    (*range1)++;
                else if (cur->record->age > 20 && cur->record->age <= 40)
                    (*range2)++;
                else if (cur->record->age > 40 && cur->record->age <= 60)
                    (*range3)++;
                else
                    (*range4)++;
            }
        }
    }

}

void countnodes2(recordNode **head, int *range1, int *range2, int *range3, int *range4, char *key, char *date1,
                 char *date2) {     //counts node for given key and between date1 and date2
    if (*head == NULL)
        return;
    recordNode *cur = *head;
    if (cur->next == NULL) {
        if (strcmp(cur->record->diseaseID, key) == 0)
            printf("%d\n", cur->record->age);
        if (strcmp(cur->record->diseaseID, key) == 0 &&
            (datecompare(cur->record->date, date1) >= 0 && datecompare(cur->record->date, date2) <= 0)) {
            if (cur->record->age >= 0 && cur->record->age <= 20)
                (*range1)++;
            else if (cur->record->age > 20 && cur->record->age <= 40)
                (*range2)++;
            else if (cur->record->age > 40 && cur->record->age <= 60)
                (*range3)++;
            else if (cur->record->age > 60)
                (*range4)++;
        }
    }
    while (cur->next != NULL) {
        if (strcmp(cur->record->diseaseID, key) == 0)
            printf("%d\n", cur->record->age);
        if (strcmp(cur->record->diseaseID, key) == 0 &&
            (datecompare(cur->record->date, date1) >= 0 && datecompare(cur->record->date, date2) <= 0)) {
            if (cur->record->age >= 0 && cur->record->age <= 20)
                (*range1)++;
            else if (cur->record->age > 20 && cur->record->age <= 40)
                (*range2)++;
            else if (cur->record->age > 40 && cur->record->age <= 60)
                (*range3)++;
            else if (cur->record->age > 60)
                (*range4)++;
        }
        cur = cur->next;
        if (cur->next == NULL) {
            if (strcmp(cur->record->diseaseID, key) == 0)
                printf("%d\n", cur->record->age);
            if (strcmp(cur->record->diseaseID, key) == 0 &&
                (datecompare(cur->record->date, date1) >= 0 && datecompare(cur->record->date, date2) <= 0)) {
                if (cur->record->age >= 0 && cur->record->age <= 20)
                    (*range1)++;
                else if (cur->record->age > 20 && cur->record->age <= 40)
                    (*range2)++;
                else if (cur->record->age > 40 && cur->record->age <= 60)
                    (*range3)++;
                else if (cur->record->age > 60)
                    (*range4)++;
            }
        }
    }

}


typedef struct disease_node {
    char *diseaseID;
    struct disease_node *next;
} disease;

int diseaseListInsert(disease **head, char *diseaseID) {
    disease *tmp = malloc(sizeof(disease));
    tmp->diseaseID = malloc(strlen(diseaseID) + 1);
    strcpy(tmp->diseaseID, diseaseID);
    tmp->next = NULL;
    if (*head == NULL) {
        *head = tmp;
    } else {
        disease *cur = *head;
        if (strcmp(tmp->diseaseID, cur->diseaseID) == 0) {
            free(tmp);
            return 0;
        }
        while (cur->next != NULL) {
            if (strcmp(tmp->diseaseID, cur->diseaseID) == 0) {
                free(tmp);
                return 0;
            }
            cur = cur->next;
        }
        cur->next = tmp;

    }
    return 1;
}


int Insert(recordNode **head, record **rec) {       //insert a record in the list
    recordNode *tmp = malloc(sizeof(recordNode));
    tmp->record = *rec;
    int flag1 = 0;
    int flag2 = 1;
    tmp->next = NULL;
    if (*head == NULL) {    //list is empty
        *head = tmp;
    } else {
        recordNode *cur = *head;
        if (strcmp((*rec)->recordID, cur->record->recordID) == 0) {  //same record id was given
            flag2 = 0;
            if (strcmp((*rec)->patientFirstName, cur->record->patientFirstName) &&
                strcmp((*rec)->patientLastName, cur->record->patientLastName))
                if (strcmp((*rec)->status, "EXIT") && strcmp(cur->record->patientLastName, "ENTER"))
                    flag1 = 1;
            //free(tmp);
            //return -1;
        }
        while (cur->next != NULL) {
            if (strcmp((*rec)->recordID, cur->next->record->recordID) == 0) {    ////same record id was given
                flag2 = 0;
                if (strcmp((*rec)->patientFirstName, cur->next->record->patientFirstName) &&
                    strcmp((*rec)->patientLastName, cur->record->patientLastName))
                    if (strcmp((*rec)->status, "EXIT") && strcmp(cur->next->record->status, "ENTER"))
                        flag1 = 1;

                //free(tmp);
                //return -1;
            }
            cur = cur->next;
        }
        if (flag1 == 0 && flag2 == 0) {
            free(tmp);
            return -1;
        } else
            cur->next = tmp;
    }
    return 1;
}

int hashfunc(char *key, int size) {     //produces hash value of the key
    int hashval = 0;
    int i;
    for (i = 0; i < strlen(key); i++) {
        hashval += ((int) key[i]);
    }

    return hashval % size;
}

void insert(char *id, struct record **rec, bucket **ht, int size, int bck_size) {        //inseret into the hash table
    int position = hashfunc(id, size);
    if ((ht)[position] == NULL) {   //hash table position is null
        ht[position] = malloc(sizeof(bucket));      //allocate memory
        ht[position]->storage = malloc(sizeof(char) * bck_size);    //allocate memory for the bucket
        htentry *tmp = malloc(sizeof(htentry));
        tmp->ID = id;
        tmp->root = NULL;
        tmp->root = treeinsert(tmp->root, rec);     //create the root for current entry
        htdata *data;
        data = malloc(sizeof(htdata));
        data->count = 1;    //only one entry
        data->max = ((bck_size - (int) sizeof(htdata)) /
                     ((int) sizeof(htentry)));  //max entries that can fit in a block
        data->next = NULL;
        memmove(ht[position]->storage, data, sizeof(htdata));   //data holds information for the bucket
        memmove((ht[position]->storage + sizeof(htdata)), tmp, sizeof(htentry));    //store entry in the bucket
        free(data);
        free(tmp);
    } else {    //bucket already exists in current position
        htdata *data = malloc(sizeof(htdata));
        memmove(data, ht[position]->storage, sizeof(htdata));   //get bucket's data
        int outerflag = 0;
        bucket *tempbucket = ht[position];
        while (data != NULL) {
            htentry *tempentry = malloc(sizeof(htentry));
            for (int i = 0; i < data->count; i++) {
                memmove(tempentry, tempbucket->storage + sizeof(htdata) + i * sizeof(htentry), sizeof(htentry));
                if (strcmp(id, tempentry->ID) ==
                    0) {   //if there is already an entry with the same id insert new entry in the tree
                    tempentry->root = treeinsert(tempentry->root, rec);
                    memmove((tempbucket->storage + sizeof(htdata) + i * sizeof(htentry)), tempentry, sizeof(htentry));
                    outerflag = 1;
                    break;
                }
            }
            bucket *prev = tempbucket;
            tempbucket = data->next;    //move to the next bucket
            if (tempbucket != NULL)
                memmove(data, tempbucket->storage, sizeof(htdata));
            else {

                free(tempentry);
                tempbucket = prev;    //if there is no next bucket stay to the previous one
                break;
            }
            free(tempentry);
            tempentry = NULL;
            if (outerflag == 1)
                break;
        }

        if (outerflag == 0) {   //no entry with the same id was found
            if (data->count == (data->max)) { //curent bucket is full so we create a new one
                bucket *newbucket = malloc(sizeof(bucket));
                newbucket->storage = malloc(sizeof(char) * bck_size);
                htentry *tmp = malloc(sizeof(htentry));
                tmp->ID = id;
                tmp->root = NULL;
                tmp->root = treeinsert(tmp->root, rec);
                htdata *newdata = malloc(sizeof(htdata));
                newdata->count = 1;
                newdata->max = ((bck_size - (int) sizeof(htdata)) / ((int) sizeof(htentry)));
                newdata->next = NULL;

                memmove(newbucket->storage, newdata, sizeof(htdata));
                memmove((newbucket->storage + sizeof(htdata)), tmp, sizeof(htentry));

                data->next = newbucket; //previous bucket ppoints to the newly created bucket
                memmove(ht[position]->storage, data, sizeof(htdata));

                //free(data);
            } else {    //there is space in the current bucket
                htentry *tmp = malloc(sizeof(htentry));

                tmp->ID = id;
                tmp->root = NULL;
                tmp->root = treeinsert(tmp->root, rec);
                //store entry and increase the counter
                memmove((tempbucket->storage + sizeof(htdata) + data->count * sizeof(htentry)), tmp,
                        sizeof(htentry));
                data->count++;
                memmove(tempbucket->storage, data, sizeof(htdata));
                free(tmp);
            }
        }
        free(data);
    }
}


int main(int argc, char *argv[]) {

    sleep(2);
    int fifo_send, fifo_receive;
    ssize_t n;
    int fd;
    char **dir_array;
    //fd_set fd_read;
    struct timeval tv;
    tv.tv_sec = 0;
    tv.tv_usec = 10000;
    struct pollfd pfd;
    pfd.events = POLLIN;
    pfd.revents = 0;
    int ret;
    int bufferSize = atoi(argv[2]);
    int worker_num = atoi(argv[3]);
    char buff[bufferSize];
    int numofDirs = 0;

    int message_size = 0;
    int bread = 0;


    int bytesRead = 0;
    int bytesToRead;
    char str_size[5];
    fifo_receive = pfd.fd = open(argv[1], O_RDONLY);
    read(fifo_receive, &bytesToRead, sizeof(int));      //read size of mesage
    int mbuff_size = bytesToRead;
    char message_buff[mbuff_size + 1];
    while (1) {         //read the directories
        size_t bytes = read(fifo_receive, buff, bufferSize);
        if (bytes == -1)
            continue;
        if (bytes == 0 && bytesRead == 0) {
            printf("waiting\n");
            //continue;
            break;
        }
        if ((bytes < 0) && errno == EAGAIN)
            continue;
        memmove(message_buff + bytesRead, buff, bytes);
        int length2 = 0;
        bytesToRead = bytesToRead - bytes;
        if (bytesToRead <= 0)
            break;
        bytesRead += bytes;

    }
    message_buff[mbuff_size] = '\0';
    memmove(str_size, message_buff, 2);
    str_size[2] = '\0';
    numofDirs = atoi(str_size);
    dir_array = malloc(numofDirs * sizeof(char *));
    memmove(message_buff, message_buff + 3, strlen(message_buff) - 3);
    int array_iter = 0;
    int length = 0;
    int total_length = 0;
    for (int j = 0; j < mbuff_size - 3; j++) {  //read each dir name and store it in the array
        if (message_buff[j] == '#') {
            dir_array[array_iter] = malloc(sizeof(char) * (length + 1));
            memmove(dir_array[array_iter], message_buff + total_length, length);
            dir_array[array_iter][length] = '\0';
            total_length += length;
            length = 0;
            array_iter++;
        }
        length++;
    }
    for (int i = 0; i < numofDirs; i++) {      //remove #
        if (dir_array[i][0] == '#') {
            memmove(dir_array[i], dir_array[i] + 1, strlen(dir_array[i]));
        }
    }
    bytesRead = 0;
    read(fifo_receive, &bytesToRead, sizeof(int));      //read size of mesage
    mbuff_size = bytesToRead;
    char message_buff2[mbuff_size + 1];
    while (1) {         //read server stats
        size_t bytes = read(fifo_receive, buff, bufferSize);
        if (bytes == -1)
            continue;
        if (bytes == 0 && bytesRead == 0) {
            printf("waiting\n");
            //continue;
            break;
        }
        if ((bytes < 0) && errno == EAGAIN)
            continue;
        memmove(message_buff2 + bytesRead, buff, bytes);
        int length2 = 0;
        bytesToRead = bytesToRead - bytes;
        if (bytesToRead <= 0)
            break;
        bytesRead += bytes;

    }
    message_buff2[mbuff_size] = '\0';

    array_iter = 0;
    length = 0;
    total_length = 0;
    char *serverStats[2];
    for (int m = 0; m < mbuff_size; ++m) {
        if (message_buff2[m] == '#') {
            serverStats[array_iter] = malloc(sizeof(char) * (length + 1));
            memmove(serverStats[array_iter], message_buff2 + total_length, length);
            serverStats[array_iter][length] = '\0';
            total_length += length;
            length = 0;
            array_iter++;
        }
        length++;
    }
    if (serverStats[0][0] == '#') {
        memmove(serverStats[0], serverStats[0] + 1, strlen(serverStats[0]));
    }
    if (serverStats[1][0] == '#') {
        memmove(serverStats[1], serverStats[1] + 1, strlen(serverStats[1]));
    }

    printf("%s %s\n", serverStats[0], serverStats[1]);

    char curWorkingDir[50];
    if ((getcwd(curWorkingDir, sizeof(curWorkingDir))) == NULL) {
        perror("getcwd");
        exit(3);
    }
    struct recordNode **recordList;      //list for the records
    bucket ***diseaseHashTable;     //hash tables for diseases
    diseaseHashTable = malloc(sizeof(bucket ***) * numofDirs);
    recordList = malloc(numofDirs * sizeof(recordNode *));
    disease **diseaselist;      //list of disease
    diseaselist = malloc(sizeof(disease *) * numofDirs);
    int disease_num;
    char **complete_mess_buff;
    char **mess_to_parent;
    complete_mess_buff = malloc(sizeof(char *) * numofDirs);

    int port, sock;
    struct sockaddr_in server;
    struct sockaddr *serverptr = (struct sockaddr *) &server;
    struct hostent *rem;
    if ((sock = socket(AF_INET, SOCK_STREAM, 0)) < 0) {
        perror("socket");
        exit(-1);
    }
    if ((rem = gethostbyname(serverStats[0])) == NULL) {
        perror("gethostbyname");
        exit(-1);
    }
    port = atoi(serverStats[1]);
    server.sin_family = AF_INET;
    memcpy(&server.sin_addr, rem->h_addr, rem->h_length);
    server.sin_port = htons(port);
    if (connect(sock, serverptr, sizeof(server)) < 0) {     //connect to the server
        perror("connect");
        exit(-1);
    }

   // printf("dirs:%d in %d\n", numofDirs, worker_num);
    write(sock, &worker_num, sizeof(int));      //write worker's number to server
    write(sock, &numofDirs, sizeof(int));       //write number of worker's directories to server
    fflush(stdout);
    printf("WOK\n");
    for (int i = 0; i < numofDirs; i++) {       //for each dir
        disease_num = 0;
        char temp_dir[100];
        sprintf(temp_dir, "%s/my_dir/%s", curWorkingDir, dir_array[i]);     //get name
        DIR *dir;
        struct dirent **rdir;
        int n;

        n = scandir(temp_dir, &rdir, 0, alphasort);     //use this to store all files in rdir
        if (n < 0)
            perror("scandir");
        else {
            for (int i = 0; i < n; i++) {
                if (strcmp(rdir[i]->d_name, ".") != 0 && strcmp(rdir[i]->d_name, "..") != 0) {
                    rdir[i]->d_name[10] = '\0';
                }
            }
            for (int i = 1; i < n; i++) {       //sort files by date
                for (int j = 0; j < n - i; j++) {
                    if (strcmp(rdir[j]->d_name, ".") != 0 && strcmp(rdir[j]->d_name, "..") != 0) {
                        if (datecompare(rdir[j]->d_name, rdir[j + 1]->d_name) < 0) {
                            char tmp[strlen(rdir[j]->d_name) + 1];
                            strcpy(tmp, rdir[j]->d_name);
                            strcpy(rdir[j]->d_name, rdir[j + 1]->d_name);
                            strcpy(rdir[j + 1]->d_name, tmp);
                        }
                    }
                }
            }

        }

        int cnt = n;
        diseaselist[i] = NULL;
        recordList[i] = NULL;
        char *rec_array[n];
        int r = n - 1;
        for (int l = n - 1; l >= 0; l--) {
            rec_array[r] = malloc(strlen(rdir[l]->d_name) + 1);
            strcpy(rec_array[r], rdir[l]->d_name);
            r--;
        }
        int normal_files = 0;
        for (int nf = n - 1; nf >= 0; nf--) {
            if (strcmp(rec_array[nf], ".") != 0 && strcmp(rec_array[nf], "..") != 0)
                normal_files++;
        }

        //write(sock, &normal_files, sizeof(int));    //write number of files to server
        for (int l = n - 1; l >= 0; l--) {      //for each file
            if (strcmp(rec_array[l], ".") != 0 && strcmp(rec_array[l], "..") != 0) {
                char temp_file_path[512];
                sprintf(temp_file_path, "%s/%s.txt", temp_dir, rdir[l]->d_name);
                FILE *ptr;
                char *token;
                char buffer[1280];
                char *values[7];
                if ((ptr = fopen(temp_file_path, "r")) == NULL) {    //open file
                    printf("Can't open file\n");
                    return 1;
                }
                while (fgets(buffer, sizeof(buffer), ptr) != NULL) {    //we get each line of the file
                    token = strtok(buffer, " \n\r\t");  //we split the line into words and ignore whitespaces
                    record *tmp = malloc(sizeof(record));
                    for (int j = 0; (j < 6) && (token != NULL); j++) {  //we store each word in values array
                        int length = strlen(token);
                        values[j] = malloc(length + 1);
                        strcpy(values[j], token);
                        token = strtok(NULL, " \n\r\t");
                    }
                    tmp->recordID = malloc(sizeof(char) * (strlen(values[0]) + 1));
                    tmp->status = malloc(sizeof(char) * (strlen(values[1]) + 1));
                    tmp->patientFirstName = malloc(sizeof(char) * (strlen(values[2]) + 1));
                    tmp->patientLastName = malloc(sizeof(char) * (strlen(values[3]) + 1));
                    tmp->diseaseID = malloc(sizeof(char) * (strlen(values[4]) + 1));
                    tmp->age = atoi(values[5]);
                    tmp->date = malloc(sizeof(char) * (strlen(rdir[l]->d_name) + 1));

                    strcpy(tmp->recordID, values[0]);       //we save the words in the record
                    strcpy(tmp->status, values[1]);
                    strcpy(tmp->patientFirstName, values[2]);
                    strcpy(tmp->patientLastName, values[3]);
                    strcpy(tmp->diseaseID, values[4]);
                    strcpy(tmp->date, rdir[l]->d_name);
                    disease_num += diseaseListInsert(&(diseaselist[i]), values[4]);
                    int flag = 0;
                    flag = Insert(&(recordList[i]), &tmp);      //inset record into the list
                    //printf("%s\n",tmp->diseaseID);
                    for (int i = 0; i < 6; i++) {
                        free(values[i]);
                    }
                }

                disease *curdis = diseaselist[i];
                char *startbuff;
                char *writebuff[disease_num];
                int count = 0;
                int total_size = 0;
                int first_mess_size = strlen(rdir[l]->d_name) + strlen(dir_array[i]) + 2 * strlen("$") +
                                      1;     //first message for parent
                char *first_mess_buff = malloc(first_mess_size);
                memset(first_mess_buff, '0', first_mess_size);
                memmove(first_mess_buff, rdir[l]->d_name, strlen(rdir[l]->d_name));
                memmove(first_mess_buff + strlen(rdir[l]->d_name), "$", strlen("$"));
                memmove(first_mess_buff + strlen(rdir[l]->d_name) + strlen("$"), dir_array[i], strlen(dir_array[i]));
                memmove(first_mess_buff + strlen(rdir[l]->d_name) + strlen("$") + strlen(dir_array[i]), "$",
                        strlen("$"));
                first_mess_buff[first_mess_size - 1] = '\0';
                startbuff = malloc(first_mess_size);
                strcpy(startbuff, first_mess_buff);
                free(first_mess_buff);
                total_size += first_mess_size - 1;

                while (curdis != NULL) {
                    int range1 = 0;
                    int range2 = 0;
                    int range3 = 0;
                    int range4 = 0;
                    countnodes(&(recordList[i]), &range1, &range2, &range3, &range4,
                               curdis->diseaseID);    //count statistics for each disease
                    if (range1 == 0 && range2 == 0 && range3 == 0 && range4 == 0)
                        continue;

                    else {
                        char str1[40];
                        char str2[40];
                        char str3[40];
                        char str4[40];
                        sprintf(str1, "Age range 0-20 years: %d cases", range1);
                        sprintf(str2, "Age range 21-40 years: %d cases", range2);
                        sprintf(str3, "Age range 41-60 years: %d cases", range3);
                        sprintf(str4, "Age range 60+ years: %d cases", range4);
                        int message_size = 0;
                        message_size =
                                strlen(curdis->diseaseID) + strlen(str1) + strlen(str2) + strlen(str3) + strlen(str4) +
                                6;
                        char *mess_to_parent = malloc(message_size);
                        memmove(mess_to_parent, curdis->diseaseID, strlen(curdis->diseaseID));
                        memmove(mess_to_parent + strlen(curdis->diseaseID), "$", strlen("$"));
                        memmove(mess_to_parent + strlen(curdis->diseaseID) + strlen("$"), str1, strlen(str1));
                        memmove(mess_to_parent + strlen(curdis->diseaseID) + strlen("$") + strlen(str1), "$",
                                strlen("$"));
                        memmove(mess_to_parent + strlen(curdis->diseaseID) + 2 * strlen("$") + strlen(str1), str2,
                                strlen(str2));
                        memmove(mess_to_parent + strlen(curdis->diseaseID) + 2 * strlen("$") + strlen(str1) +
                                strlen(str2), "$", strlen("$"));
                        memmove(mess_to_parent + strlen(curdis->diseaseID) + 3 * strlen("$") + strlen(str1) +
                                strlen(str2), str3, strlen(str3));
                        memmove(mess_to_parent + strlen(curdis->diseaseID) + 3 * strlen("$") + strlen(str1) +
                                strlen(str2) + strlen(str3), "$", strlen("$"));
                        memmove(mess_to_parent + strlen(curdis->diseaseID) + 4 * strlen("$") + strlen(str1) +
                                strlen(str2) + strlen(str3), str4, strlen(str4));
                        memmove(mess_to_parent + strlen(curdis->diseaseID) + 4 * strlen("$") + strlen(str1) +
                                strlen(str2) + strlen(str3) + strlen(str4), "$", strlen("$"));
                        //na grapsw to mess size
                        mess_to_parent[message_size - 1] = '\0';       //second part of the message to parent
                        writebuff[count] = malloc(message_size);
                        strcpy(writebuff[count], mess_to_parent);
                        total_size += message_size - 1;
                        free(mess_to_parent);
                        count++;
                    }
                    curdis = curdis->next;
                }

                char *final_mess_buff = malloc(total_size + 1);
                //memset(final_mess_buff,'0',total_size+1);
                memmove(final_mess_buff, startbuff, strlen(startbuff));
                int move_val = strlen(startbuff);
                for (int ds = 0; ds < disease_num; ds++) {  //make final message
                    memmove(final_mess_buff + move_val, writebuff[ds], strlen(writebuff[ds]));
                    move_val += strlen(writebuff[ds]);
                }
                final_mess_buff[total_size] = '\0';

                int wv;
                int flag = 0;
                int retval;
               // write(sock, &total_size, sizeof(int));      //write size of message
                //write(sock, &bufferSize, sizeof(int));      //write buffersize to send message in parts
               // write(sock,final_mess_buff,total_size);
                /*int bytesWritten = 0;
                int bytesToWrite = total_size;
                if (bytesToWrite > bufferSize)
                    memmove(buff, final_mess_buff, bufferSize);
                else
                    memmove(buff, final_mess_buff, bytesToWrite);
                while (bytesToWrite > 0) {  //write statistics to server
                    size_t bytes = write(sock, buff, bufferSize);
                    if (bytes < 0) {
                        perror("Write mess size");
                        exit(-5);
                    }
                    bytesToWrite = bytesToWrite - bytes;
                    bytesWritten += bytes;
                    if (bytesToWrite > 0) {
                        if (bytesToWrite > bufferSize)
                            memmove(buff, final_mess_buff + bytesWritten, bufferSize);
                        else
                            memmove(buff, final_mess_buff + bytesWritten, bytesToWrite);

                    }
                }
                char serv_mess[4];
               read(sock, serv_mess, 2);
                serv_mess[2] = '\0';
                if (strcmp(serv_mess, "OK") == 0);*/

                free(final_mess_buff);
            }
        }
        /*char serv_mess[4];
        read(sock, serv_mess, 2);
        serv_mess[2] = '\0';
        if (strcmp(serv_mess, "OK") == 0);*/
       // printf("OK1 %d\n",worker_num);
    }


    int mysock;

    struct sockaddr_in my_serv, client;
    struct sockaddr *my_servptr = (struct sockaddr *) &my_serv;
    socklen_t len = sizeof(my_serv);
    struct sockaddr *clientptr = (struct sockaddr *) &client;
    if ((mysock = socket(AF_INET, SOCK_STREAM, 0)) < 0) {       //socket in which worker listens
        perror("socket");
        exit(-1);
    }
    my_serv.sin_family = AF_INET;
    my_serv.sin_addr.s_addr = htonl(INADDR_ANY);
    my_serv.sin_port = htons(0);
    if (bind(mysock, my_servptr, sizeof(my_serv)) < 0) {
        perror("bind");
        exit(-1);
    }
    listen(mysock, 10);
    if (getsockname(mysock, my_servptr, &len) < 0) {
        perror("getsockname");
        exit(-1);
    }
    char *myIP;
    myIP = inet_ntoa(my_serv.sin_addr);
   // printf("%d\n", my_serv.sin_port);
    write(sock, &my_serv.sin_port, sizeof(int));
    write(sock, myIP, sizeof(myIP));
   // printf("OK2 %d\n",worker_num);
    close(sock);    //close socket 

    pfd.fd = mysock;
    pfd.events = POLLIN;
    int rec;
    socklen_t clientlen = 0;
    int newsock;
    while (1) {
        rec = poll(&pfd, 1, -1);      //wait for connection
        if (rec == -1) {
            perror("poll");
            exit(-1);
        }
        if (pfd.revents & POLLIN) {
            if (pfd.fd == mysock) {     //new connction
                clientlen = sizeof(client);
                if ((newsock = accept(mysock, clientptr, &clientlen)) < 0) {    //accept connection
                    perror("accept");
                    exit(-1);
                }
                int size;
                read(newsock, &size, sizeof(int));      //read size of query
                char query[size + 1];
                read(newsock, &query, size);            //read query
                query[size] = '\0';
                char *words[8];
                for (int j = 0; j < 8; ++j) {
                    words[j] = NULL;
                }
                int len = 0;
                int array_it = 0;
                int tot_len = 0;
                for (int j = 0; j <= strlen(query); j++) {
                    if (query[j] == ' ' || query[j] == '\0') {      //split query into words
                        words[array_it] = malloc((len + 1) * sizeof(char));
                        memmove(words[array_it], query + tot_len, len);
                        tot_len += len;
                        words[array_it][len] = '\0';
                        array_it++;
                        len = 0;
                    }
                    len++;
                }
                for (int l = 0; l < 8; ++l) {       //remove unwanted whitespaces
                    if (words[l] != NULL) {
                        if (words[l][0] == ' ') {
                            memmove(words[l], words[l] + 1, strlen(words[l]));
                        }
                        for (int k = 0; k <= strlen(words[l]); k++) {
                            if (words[l][k] == '\n') {
                                words[l][k] = '\0';
                                break;
                            }
                        }
                    }
                }

                if ((strcmp(words[0], "/diseaseFrequency")) == 0) {
                    if (words[3] == NULL) {
                        printf("Incomplete command\n");
                        continue;
                    }
                    if (strlen(words[4]) < 2) {
                        free(words[4]);
                        words[4] = NULL;
                    }
                    if (words[4] == NULL) {     //no country was given
                        int count = 0;
                        for (int m = 0; m < numofDirs; m++) {   //get each list
                            recordNode *cur = recordList[m];
                            while (cur != NULL) {  //traverse through list
                                record *tmp = cur->record;
                                if ((strcmp(tmp->diseaseID, words[1]) == 0) &&
                                    (datecompare(tmp->date, words[2]) > 0 && datecompare(tmp->date, words[3]) < 0) &&
                                    (strcmp(tmp->status, "ENTER") == 0)) {
                                    printf("OK\n");
                                    count++;
                                }
                                cur = cur->next;
                            }
                        }
                        write(newsock, &count, sizeof(int));    //write count to server
                    }
                    if (words[4] != NULL) {     //country was given
                        //printf("OK1 %d\n", worker_num);
                        int count = 0;
                        for (int i = 0; i < numofDirs; ++i) {       //for each list
                            if (strcmp(dir_array[i], words[4]) == 0) {  //if we are in the country we are looking for
                                recordNode *cur = recordList[i];
                                while (cur != NULL) {
                                    record *tmp = cur->record;
                                    if (strcmp(tmp->diseaseID, words[1]) == 0 &&
                                        (datecompare(tmp->date, words[2]) > 0 &&
                                         datecompare(tmp->date, words[3]) < 0) &&
                                        strcmp(tmp->status, "ENTER") == 0) {
                                        count++;
                                    }
                                    cur = cur->next;
                                }
                            }
                        }
                        printf("count:%d\n", count);
                        write(newsock, &count, sizeof(int));    //write count to server
                    }
                }

                if (strcmp(words[0], "/searchPatientRecord") == 0) {
                    if (words[1] == NULL) {
                        printf("Incomplete command\n");
                    }
                    int flag1 = 0;
                    int flag2 = 0;
                    record *save = NULL;
                    char date[11];
                    for (int i = 0; i < numofDirs; ++i) {       //for each list
                        recordNode *cur = recordList[i];
                        while (cur != NULL) {
                            record *tmp = cur->record;

                            if (words[1][0] == ' ') {
                                memmove(words[1], words[1] + 1, strlen(words[1]));
                            }
                            for (int k = 0; k <= strlen(words[1]); k++) {
                                if (words[1][k] == '\n') {
                                    words[1][k] = '\0';
                                    break;
                                }
                            }
                            if ((strcmp(tmp->recordID, words[1]) == 0) && strcmp(tmp->status, "ENTER") == 0 &&
                                flag1 == 0 && flag2 == 0) {     //find wanted patient for the first time with enter
                                flag1 = 1;
                                save = tmp;
                            }
                            if (strcmp(tmp->recordID, words[1]) == 0 && strcmp(tmp->status, "EXIT") == 0 &&
                                flag1 == 1 && flag2 == 0) {     //find patient for the second time with exit
                                flag2 = 1;
                                strcpy(date, tmp->date);
                            }
                            cur = cur->next;
                        }
                    }
                    if (save != NULL) {     //patient was found
                        if (flag1 == 1 && flag2 == 1) {     //patient has exited
                            char result[100];
                            sprintf(result, "%s %s &s %s %d %s %s", save->recordID, save->patientFirstName,
                                    save->patientLastName, save->age, save->date, date);
                            int res_size = strlen(result);
                            write(newsock, &res_size, sizeof(int));
                            write(newsock, result, res_size);
                        }
                        if (flag1 == 1 && flag2 == 0) {     //patient hasn't exited
                            char result[100];
                            sprintf(result, "%s %s &s %s %d %s --", save->recordID, save->patientFirstName,
                                    save->patientLastName, save->age, save->date);
                            int res_size = strlen(result);
                            write(newsock, &res_size, sizeof(int));
                            write(newsock, result, res_size);
                        }
                    }
                    if (save == NULL) {     //patient wasn't found
                        int res_size = 0;
                        write(newsock, &res_size, sizeof(int));
                    }
                }

                if (strcmp(words[0], "/numPatientAdmissions") == 0) {
                    if (words[3] == NULL) {
                        printf("Incomplete command\n");
                        continue;
                    }
                    if (strlen(words[4]) < 2) {
                        free(words[4]);
                        words[4] = NULL;
                    }

                    if (words[4] == NULL) {
                        write(newsock, &numofDirs, sizeof(int));
                        for (int m = 0; m < numofDirs; m++) {
                            int count = 0;
                            recordNode *cur = recordList[m];
                            while (cur != NULL) {
                                record *tmp = cur->record;
                                if ((strcmp(tmp->diseaseID, words[1]) == 0) &&
                                    (datecompare(tmp->date, words[2]) > 0 && datecompare(tmp->date, words[3]) < 0) &&
                                    (strcmp(tmp->status, "ENTER") == 0)) {
                                    printf("OK\n");
                                    count++;
                                }
                                cur = cur->next;
                            }
                            char res[100];
                            sprintf(res, "%s %d", dir_array[m], count);     //store count for each country
                            int res_size = strlen(res);
                            write(newsock, &res_size, sizeof(int));
                            write(newsock, res, res_size);
                        }
                    }
                    if (words[4] != NULL) {
                        int flag = 0;
                        char res[100];
                        for (int i = 0; i < numofDirs; ++i) {
                            if (strcmp(dir_array[i], words[4]) == 0) {
                                int count = 0;
                                recordNode *cur = recordList[i];
                                while (cur != NULL) {
                                    record *tmp = cur->record;
                                    if (strcmp(tmp->diseaseID, words[1]) == 0)
                                        printf("OK\n");
                                    if (strcmp(tmp->diseaseID, words[1]) == 0 &&
                                        (datecompare(tmp->date, words[2]) > 0 &&
                                         datecompare(tmp->date, words[3]) < 0) &&
                                        strcmp(tmp->status, "ENTER") == 0) {
                                        count++;
                                    }
                                    cur = cur->next;
                                }
                                sprintf(res, "%s %d", dir_array[i], count); //store count for given country
                                flag = 1;
                                break;
                            }
                        }
                        if (flag == 1) {    //country was found
                            int res_size = strlen(res);
                            write(newsock, &res_size, sizeof(int));
                            write(newsock, res, res_size);
                        } else {
                            int res_size = 0;
                            write(newsock, &res_size, sizeof(int));
                        }
                    }
                }

                if (strcmp(words[0], "/numPatientDischarges") == 0) {
                    if (words[3] == NULL) {
                        printf("Incomplete command\n");
                        continue;
                    }
                    if (strlen(words[4]) < 2) {
                        free(words[4]);
                        words[4] = NULL;
                    }

                    if (words[4] == NULL) {
                        write(newsock, &numofDirs, sizeof(int));
                        for (int m = 0; m < numofDirs; m++) {
                            int count = 0;
                            recordNode *cur = recordList[m];
                            while (cur != NULL) {
                                record *tmp = cur->record;
                                if ((strcmp(tmp->diseaseID, words[1]) == 0) &&
                                    (datecompare(tmp->date, words[2]) > 0 && datecompare(tmp->date, words[3]) < 0) &&
                                    (strcmp(tmp->status, "EXIT") == 0)) {
                                    printf("OK\n");
                                    count++;
                                }
                                cur = cur->next;
                            }
                            char res[100];
                            sprintf(res, "%s %d", dir_array[m], count);
                            int res_size = strlen(res);
                            write(newsock, &res_size, sizeof(int));
                            write(newsock, res, res_size);
                        }
                    }
                    if (words[4] != NULL) {
                        int flag = 0;
                        char res[100];
                        for (int i = 0; i < numofDirs; ++i) {
                            if (strcmp(dir_array[i], words[4]) == 0) {
                                int count = 0;
                                recordNode *cur = recordList[i];
                                while (cur != NULL) {
                                    record *tmp = cur->record;
                                    if (strcmp(tmp->diseaseID, words[1]) == 0)
                                        printf("OK\n");
                                    if (strcmp(tmp->diseaseID, words[1]) == 0 &&
                                        (datecompare(tmp->date, words[2]) > 0 &&
                                         datecompare(tmp->date, words[3]) < 0) &&
                                        strcmp(tmp->status, "EXIT") == 0) {
                                        count++;
                                    }
                                    cur = cur->next;
                                }
                                sprintf(res, "%s %d", dir_array[i], count);
                                flag = 1;
                                break;
                            }
                        }
                        if (flag == 1) {
                            int res_size = strlen(res);
                            // printf("%s\n", res);
                            write(newsock, &res_size, sizeof(int));
                            write(newsock, res, res_size);
                        } else {
                            int res_size = 0;
                            write(newsock, &res_size, sizeof(int));
                        }
                    }
                }
                if (strcmp(words[0], "/topk-AgeRanges") == 0) {
                    heap *h = malloc(sizeof(heap));
                    InitHeap(h);
                    int flag = 0;
                    int i = 0;
                    for (i = 0; i < numofDirs; ++i) {       //for each directory
                        if (strcmp(dir_array[i], words[2]) == 0) {  //country was found
                            flag = 1;
                            int range1 = 0;
                            int range2 = 0;
                            int range3 = 0;
                            int range4 = 0;
                            countnodes2(&(recordList[i]), &range1, &range2, &range3, &range4, words[3], words[4],
                                        words[5]);      //find ranges
                            printf("%d %d %d %d\n", range1, range2, range3, range4);
                            char srange1[] = "0-20";
                            char srange2[] = "21-40";
                            char srange3[] = "41-60";
                            char srange4[] = "60+";
                            insertheap(&h, range1, srange1);
                            insertheap(&h, range2, srange2);
                            insertheap(&h, range3, srange3);
                            insertheap(&h, range4, srange4);
                            break;
                        }
                    }
                    if (flag == 1) {    //country was found
                        write(newsock, &flag, sizeof(int));
                        int pcount = 0;
                        recordNode *cur = recordList[i];
                        while (cur != NULL) {
                            pcount++;
                            cur = cur->next;
                        }
                        int cnt = atoi(words[1]);
                        for (int k = 0;;) {     //we call removeRoot k times to print k top diseases
                            heapnode *tempnode = h->root;
                           // printf("orig val:%d\n", tempnode->count);
                            float pos = ((float) tempnode->count / (float) pcount) * 100;   //find %
                            char res[100];
                            sprintf(res, "%s: %.2f%%", tempnode->name, pos);
                           // printf("%s\n", res);
                            int res_size = strlen(res);
                            write(newsock, &res_size, sizeof(int));
                            write(newsock, &res, res_size);     //write result
                            removeRoot(h);
                            k++;
                            if (k == cnt)
                                break;
                            if (h->count == 0)
                                break;
                        }
                        while (h->count != 0)   //free the heap
                            removeRoot(h);
                        free(h);

                    } else {    //country wasn't found
                        write(newsock, &flag, sizeof(int));
                    }
                }

            }
        }
    }


    for (
            int i = 0;
            i < numofDirs;
            i++) {
        free(dir_array[i]);
        recordNode *cur = recordList[i];
        struct recordNode *next;

        while (cur != NULL) {       //free memory for recordList
            next = cur->next;
            free(cur->record->recordID);
            free(cur->record->patientLastName);
            free(cur->record->patientFirstName);
            free(cur->record->diseaseID);
            free(cur->record->status);
            free(cur->record->date);
            free(cur->record);
            free(cur);
            cur = next;
        }
        recordList[i] = NULL;
    }
    free(dir_array);
    free(recordList);


    return 0;
}