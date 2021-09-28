#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <fcntl.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <sys/wait.h>
#include <errno.h>
#include <dirent.h>
#include <poll.h>
#include <signal.h>
#include <netinet/in.h>
#include <netdb.h>
#include <pthread.h>

int start_flag = 0;
char *serverPort;
char *serverIP;
pthread_mutex_t mutex = PTHREAD_MUTEX_INITIALIZER;
pthread_cond_t cond = PTHREAD_COND_INITIALIZER;
pthread_mutex_t wmtx = PTHREAD_MUTEX_INITIALIZER;
pthread_cond_t wcond = PTHREAD_COND_INITIALIZER;
pthread_mutex_t pmtx = PTHREAD_MUTEX_INITIALIZER;
int writer = 0;

int countlines(char *filename) {    //counts lines of a file
    FILE *fp = fopen(filename, "r");        //open file for reading
    int c = 0;
    int count = 0;
    if (fp == NULL)
        return count;
    count++;
    while (!feof(fp)) {     //while we haven't reached the end of the file
        c = fgetc(fp);
        if (c == '\n')      //count lines
            count++;
    }
    fclose(fp);             //close file
    return count;
}

char ***queries;
int *q_num;

void *thread_func(void *arg) {
    int thread_num = *((int *) arg);

    pthread_mutex_lock(&mutex);
    pthread_cond_wait(&cond, &mutex);       //wait for all the threads to be created
    pthread_mutex_unlock(&mutex);
    //printf("OK2\n");
    for (int i = 0; i < q_num[thread_num]; i++) {   //each query assigned to thread
        //printf("from thread:%s\n", queries[thread_num][i]);
        int port, sock;
        struct sockaddr_in server;
        struct sockaddr *serverptr = (struct sockaddr *) &server;
        struct hostent *rem;
        if ((sock = socket(AF_INET, SOCK_STREAM, 0)) < 0) {
            perror("socket");
            exit(-1);
        }
        if ((rem = gethostbyname(serverIP)) == NULL) {
            perror("gethostbyname");
            exit(-1);
        }
        port = atoi(serverPort);
        server.sin_family = AF_INET;
        memcpy(&server.sin_addr, rem->h_addr, rem->h_length);
        server.sin_port = htons(port);
        if (connect(sock, serverptr, sizeof(server)) < 0) {     //connect to server
            perror("connect");
            exit(-1);
        }
        int size = strlen(queries[thread_num][i]);
        pthread_mutex_lock(&wmtx);
        /*while(writer)
            pthread_cond_wait(&wcond,&wmtx);
        writer=1;*/
        //pthread_mutex_unlock(&wmtx);

        write(sock, &size, sizeof(int));    //write the size of query to server
        // printf("Writing this query to server:%s, size:%d\n",str,size);
        write(sock, queries[thread_num][i], strlen(queries[thread_num][i]));    //write query to server
        // pthread_mutex_lock(&wmtx);
        //writer=0;
        //pthread_cond_signal(&wcond);
        pthread_mutex_unlock(&wmtx);
        char *words[8];
        for (int j = 0; j < 8; ++j) {
            words[j] = NULL;
        }
        int len = 0;
        int array_it = 0;
        int tot_len = 0;
        for (int j = 0; j <= strlen(queries[thread_num][i]); j++) {     //break query into words
            if (queries[thread_num][i][j] == ' ' || queries[thread_num][i][j] == '\0') {
                words[array_it] = malloc((len + 1) * sizeof(char));
                memmove(words[array_it], queries[thread_num][i] + tot_len, len);
                tot_len += len;
                words[array_it][len] = '\0';
                // printf("%s\n",words[array_it]);
                array_it++;
                len = 0;
            }
            len++;
        }
        for (int l = 0; l < 8; ++l) {
            if (words[l] != NULL) {     //remove unwanted whitespaces
                if (words[l][0] == ' ') {
                    memmove(words[l], words[l] + 1, strlen(words[l]));
                    //printf("%sa\n", words[l]);
                }
                for (int k = 0; k <= strlen(words[l]); k++) {
                    if (words[l][k] == '\n') {
                        words[l][k] = '\0';
                        break;
                    }
                }
            }
        }
        if (strcmp(words[0], "/diseaseFrequency") == 0) {
            int res = 0;
            read(sock, &res, sizeof(int));
            pthread_mutex_lock(&pmtx);
            printf("dfn:%d\n", res);
            pthread_mutex_unlock(&pmtx);
        }
        if (strcmp(words[0], "/searchPatientRecord") == 0) {
            int res_size;
            read(sock, &res_size, sizeof(int));     //read size ofnext message
            char *res;
            //if(res_size>0) {
            res = malloc(sizeof(char) * (res_size + 1));        //
            read(sock, res, res_size);      //read message
            res[res_size] = '\0';
            pthread_mutex_lock(&pmtx);
            printf("%s\n", res);        //print result
            pthread_mutex_unlock(&pmtx);
            free(res);
            //}
        }
        if (strcmp(words[0], "/numPatientAdmissions") == 0) {
            if (words[4] != NULL)
                if (strlen(words[4]) < 2) {
                    free(words[4]);
                    words[4] = NULL;
                }
            if (words[4] == NULL) {     //no country
                int wk = 0;
                read(sock, &wk, sizeof(int));
                for (int j = 0; j < wk; ++j) {  //for each worker
                    int dirs = 0;
                    read(sock, &dirs, sizeof(int));
                    for (int k = 0; k < dirs; ++k) {        //print for each country
                        int res_size = 0;
                        read(sock, &res_size, sizeof(int));
                        char *res;

                        res = malloc(sizeof(char) * (res_size + 1));
                        read(sock, res, res_size);
                        res[res_size] = '\0';
                        pthread_mutex_lock(&pmtx);
                        printf("%s\n", res);
                        pthread_mutex_unlock(&pmtx);
                        free(res);
                    }
                }
            }
            else{       //country was given
                int res_size;
                read(sock, &res_size, sizeof(int));
                char *res;
                //if(res_size>0) {
                res = malloc(sizeof(char) * (res_size + 1));
                read(sock, res, res_size);
                res[res_size] = '\0';
                pthread_mutex_lock(&pmtx);
                printf("%s\n", res);
                pthread_mutex_unlock(&pmtx);
                free(res);
            }
        }
        if (strcmp(words[0], "/numPatientDischarges") == 0) {
            if (words[4] != NULL)
                if (strlen(words[4]) < 2) {
                    free(words[4]);
                    words[4] = NULL;
                }
            if (words[4] == NULL) {
                //printf("OK\n");
                int wk = 0;
                read(sock, &wk, sizeof(int));
                for (int j = 0; j < wk; ++j) {
                    int dirs = 0;
                    read(sock, &dirs, sizeof(int));
                    for (int k = 0; k < dirs; ++k) {
                        int res_size = 0;
                        read(sock, &res_size, sizeof(int));
                        char *res;

                        res = malloc(sizeof(char) * (res_size + 1));
                        read(sock, res, res_size);
                        res[res_size] = '\0';
                        pthread_mutex_lock(&pmtx);
                        printf("%s\n", res);
                        pthread_mutex_unlock(&pmtx);
                        free(res);
                    }
                }
            }
            else{
                int res_size;
                read(sock, &res_size, sizeof(int));
                char *res;
                //if(res_size>0) {
                res = malloc(sizeof(char) * (res_size + 1));
                read(sock, res, res_size);
                res[res_size] = '\0';
                pthread_mutex_lock(&pmtx);
                printf("%s\n", res);
                pthread_mutex_unlock(&pmtx);
                free(res);
            }
        }
        if(strcmp(words[0],"/topk-AgeRanges")==0){
            //printf("TOPK\n");
            int k=atoi(words[1]);
            //printf("%d\n",k);
            for (int j = 0; j < k; ++j) {
                //printf("T1\n");
                int res_size;
                read(sock, &res_size, sizeof(int));

                //printf("%d\n",res_size);
                char *res;
                //if(res_size>0) {
                res = malloc(sizeof(char) * (res_size + 1));
               // printf("T2\n");
                read(sock, res, res_size);
                res[res_size] = '\0';
                pthread_mutex_lock(&pmtx);
                printf("%s\n", res);
                pthread_mutex_unlock(&pmtx);
                free(res);
            }
        }
    }
    printf("End of thread\n");
}


int main(int argc, char *argv[]) {
    if (argc != 9) {
        printf("Invalid number of arguments\n");
        exit(-1);
    }

    int numThreads = atoi(argv[4]);
    serverPort = malloc(strlen(argv[6]) + 1);
    serverIP = malloc(strlen(argv[8]) + 1);
    strcpy(serverPort, argv[6]);
    strcpy(serverIP, argv[8]);
    int count = 0;
    int flag = 0;
    FILE *ptr;
    char *token;
    char buffer[1280];
    char *values[7];
    pthread_t *tids;
    tids = malloc(numThreads * sizeof(pthread_t));

    int numofLines = countlines(argv[2]);       //count lines of queries file
    if ((ptr = fopen(argv[2], "r")) == NULL) {    //open file
        printf("Can't open file\n");
        return 1;
    }
    queries = malloc(sizeof(char **) * numThreads);     //store queries for each thread
    int thread_num = 0;

    int d = numofLines / numThreads;
    int m = numofLines % numThreads;
    q_num = malloc(sizeof(int) * numThreads);
    for (int k = 0; k < numThreads; ++k) {
        q_num[k] = 0;
    }

    for (int j = 0; j < numThreads; ++j) {      //number of queries for each thread
        if (d == 0) {
            q_num[j] = 1;
            queries[j] = malloc(sizeof(char *));
        } else {
            if (m > 0) {
                q_num[j] = d + 1;
                queries[j] = malloc(sizeof(char *) * (d + 1));
                m--;
            } else {
                q_num[j] = d;
                queries[j] = malloc(sizeof(char *) * (d));
            }
        }
    }
    int line = 0;
    while (fgets(buffer, sizeof(buffer), ptr) != NULL) {    //we get each line of the file
        queries[thread_num][line] = malloc(strlen(buffer) + 1);
        strcpy(queries[thread_num][line], buffer);
        int *arg = malloc(sizeof(*arg));
        *arg = thread_num;
        pthread_create(tids + count, NULL, thread_func, arg);
        count++;
        line++;
        if (count == q_num[thread_num]) {   //max number of queries for current thread
            line = 0;
            count = 0;
            thread_num++;
        }

    }
    sleep(1);
    pthread_cond_broadcast(&cond);      //start all threads
    for (int i = 0; i < numThreads; i++) {      //wait for threads
        pthread_join(*(tids + i), NULL);
    }


}

