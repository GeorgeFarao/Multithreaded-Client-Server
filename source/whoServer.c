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
#include <arpa/inet.h>
#include <pthread.h>

pthread_mutex_t buff_mtx = PTHREAD_MUTEX_INITIALIZER;       //mutex for buffer
pthread_cond_t cond_nonempty;
pthread_cond_t cond_nonfull;

void perror_exit(char *message) {
    perror(message);
    exit(EXIT_FAILURE);
}

typedef struct worker {     //struct for worker stats
    int worker_num;
    int port;
    char IP[16];
    struct worker *next;
} worker;

int workerListInsert(worker **head, int port, char *IP, int worker_num) {       //insert into list of workers
    worker *tmp = malloc(sizeof(worker));
    tmp->worker_num = worker_num;
    strcpy(tmp->IP, IP);
    tmp->port = port;
    tmp->next = NULL;
    if (*head == NULL) {
        *head = tmp;
    } else {
        worker *cur = *head;
        if (cur->worker_num == tmp->worker_num) {
            free(tmp);
            return 0;
        }
        while (cur->next != NULL) {
            if (cur->worker_num == tmp->worker_num) {
                free(tmp);
                return 0;
            }
            cur = cur->next;
        }
        cur->next = tmp;

    }
    return 1;
}

typedef struct connection {     //holds information about connection
    int fd;         //descriptor returned by accpet
    char type;      //connection type: Query or Statistics
    char *IP;       //IP of client
} connection;

typedef struct circ_Buff {      //circular buffer
    int start;
    int end;
    int max;
    int size;
    struct connection **buffer;
} circ_Buff;

void init_circ_Buff(circ_Buff *buff, int numThreads, int buffersize) {
    buff->start = 0;
    buff->end = -1;
    buff->size = 0;
    buff->max = buffersize;
    buff->buffer = malloc(buffersize * sizeof(connection *));
    for (int i = 0; i < buffersize; i++) {
        buff->buffer[i] = malloc(sizeof(connection));
    }
}

int isEmpty(circ_Buff *buff) {
    if (buff->size == 0)
        return 1;
    return 0;
}

void Push(circ_Buff *buff, int fd, char *IP, char type) {
    pthread_mutex_lock(&buff_mtx);
    while (buff->size >= buff->max) {       //wait while there is space in the buffer
        pthread_cond_wait(&cond_nonfull, &buff_mtx);
    }

    buff->end = (buff->end + 1) % buff->max;    //new end
    buff->buffer[buff->end]->fd = fd;           //store data
    buff->buffer[buff->end]->type = type;
    buff->buffer[buff->end]->IP = IP;
    buff->size++;

    pthread_mutex_unlock(&buff_mtx);
}

int Pop(circ_Buff *buff, char *type, char **IP) {
    int data = 0;
    pthread_mutex_lock(&buff_mtx);
    while (buff->size <= 0) {               //wait for a connection
        pthread_cond_wait(&cond_nonempty, &buff_mtx);
    }
    data = buff->buffer[buff->start]->fd;       //taake data
    *type = buff->buffer[buff->start]->type;
    *IP = buff->buffer[buff->start]->IP;
    buff->start = (buff->start + 1) % buff->max;    //new start
    buff->size--;
    pthread_mutex_unlock(&buff_mtx);
    return data;
}

circ_Buff *cBuff;
worker *head;

_Noreturn void *thread_func(void *x) {
    int fd;
    char type;
    char *IP;
    while (1) {
        while (cBuff->size == 0) {  //wait for a connection
            //printf("sleeping\n");
            sleep(10);
        }
        while (cBuff->size > 0) {       //while buffer is not empty
            fd = Pop(cBuff, &type, &IP);
            pthread_cond_signal(&cond_nonfull);
            if (type == 'S') {      //Statistics
                int times_to_read;
                int wport;
                int worker_num;
                read(fd, &worker_num, sizeof(int));     //number of worker, used to tell them apart
                read(fd, &times_to_read, sizeof(int));
                printf("worker:%d ttr:%d\n", worker_num, times_to_read);
                /*for (int i = 0; i < times_to_read; i++) {
                    int files;
                    read(fd, &files, sizeof(int));      //files of worker
                    for (int j = files - 1; j >= 0; j--) {
                        int size, buffsize = 0;
                        size = 0;
                        if ((read(fd, &size, sizeof(int))) < 0)     //size of message
                            perror_exit("read");
                        if ((read(fd, &buffsize, sizeof(int))) < 0)     //we read it in parts cause it's a big message
                            perror_exit("read");
                        char *mess_buff = malloc(sizeof(char) * (size + 1));
                        //read(fd,mess_buff,size);
                        /*char *buff = malloc(sizeof(char) * buffsize);
                        int bytesToRead = size;
                        int bytesRead = 0;
                        while (bytesToRead > 0) {       //read while we have bytes to be readen
                            ssize_t bytes = read(fd, buff, buffsize);
                            if (bytes == -1)
                                continue;
                            if (bytes == 0) {
                                break;
                            }
                            if ((bytes < 0) && errno == EAGAIN)
                                continue;
                            memmove(mess_buff + bytesRead, buff, bytes);    //store message
                            bytesToRead = bytesToRead - bytes;
                            bytesRead += bytes;
                        }
                        mess_buff[size] = '\0';
                        int tot_length = 0;
                        int length = 0;
                        /*for (int sz = 0; sz <= size; sz++) {
                            if (mess_buff[sz] == '$') {  //split the lines
                                char temp[length + 1];
                                memmove(temp, mess_buff + tot_length, length);
                                temp[length] = '\0';
                                //printf("%s\n", temp);   //print the statistics
                                tot_length += length + 1;
                                length = 0;
                                continue;
                            }
                            length++;
                        }
                       // write(fd, "OK", strlen("OK"));
                        //free(mess_buff);
                        //free(buff);
                    }
                    //write(fd, "OK", strlen("OK"));
                }*/
                printf("Read stats from %d\n",worker_num);
                int addr = 0;
                read(fd, &wport, sizeof(int));      //read port of worker
                char wIP[16];
                read(fd, &wIP, sizeof(wIP));        //read IP of worker

                printf("worker %d listens in %d with ip %s\n", worker_num, wport, IP);
                workerListInsert(&head, wport, IP, worker_num);     //insert into the list of workers
            }
            if (type == 'Q') {      //Query from whoClient
                int size;
                read(fd, &size, sizeof(int));      //read size of message
                char *query;
                query = malloc(size + 1);
                read(fd, query, size);      //read query
                query[size] = '\0';
                if (size == 0)
                    continue;
                //printf("%s\n", query);
                char *words[8];
                for (int j = 0; j < 8; ++j) {
                    words[j] = NULL;
                }
                int len = 0;
                int array_it = 0;
                int tot_len = 0;
                for (int j = 0; j <= strlen(query); j++) {      //split query into words
                    if (query[j] == ' ' || query[j] == '\0') {
                        words[array_it] = malloc((len + 1) * sizeof(char));
                        memmove(words[array_it], query + tot_len, len);
                        tot_len += len;
                        words[array_it][len] = '\0';
                        array_it++;
                        len = 0;
                    }
                    len++;
                }
                for (int l = 0; l < 8; ++l) {      //remove unwanted white spaces
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
                int disFreq_count = 0;
                char *searchPatient = NULL;

                int numofWorkers = 0;
                worker *cur = head;
                while (cur != NULL) {      //count workers
                    numofWorkers++;
                    cur = cur->next;
                }
                char ***numPatAdm = NULL;
                char ***numPatDis = NULL;
                char *no_count_NPA = NULL;
                char *no_count_NPD = NULL;
                char **topK = NULL;
                int countryflag1 = 0;
                int countryflag2 = 0;
                int *dirs_num;
                int fk = 0;
                dirs_num = malloc(numofWorkers * sizeof(int));
                cur = head;
                int work_num = 0;
                while (cur != NULL) {   //for each worker
                    int port, sock;
                    struct sockaddr_in worker;
                    struct sockaddr *workerptr = (struct sockaddr *) &worker;
                    struct hostent *rem;

                    if ((sock = socket(AF_INET, SOCK_STREAM, 0)) < 0) {
                        perror("socket");
                        exit(-1);
                    }
                    if ((rem = gethostbyname(cur->IP)) == NULL) {
                        perror("gethostbyname");
                        exit(-1);
                    }
                    port = cur->port;
                    worker.sin_family = AF_INET;
                    memcpy(&worker.sin_addr, rem->h_addr, rem->h_length);
                    worker.sin_port = (port);
                    if (connect(sock, workerptr, sizeof(worker)) < 0) {     //we connect to the worker
                        perror("connect");
                        exit(-1);
                    }
                    write(sock, &size, sizeof(int));
                    write(sock, query, strlen(query));      //write query to the worker
                    if (strcmp(words[0], "/diseaseFrequency") == 0) {
                        int res = 0;
                        read(sock, &res, sizeof(int));
                        disFreq_count += res;       //read result from each worker and add it to total sum
                    }
                    if (strcmp(words[0], "/searchPatientRecord") == 0) {
                        int res_size = 0;
                        read(sock, &res_size, sizeof(int));
                        if (res_size != 0) {    //correct worker
                            searchPatient = malloc(sizeof(char) * (res_size + 1));
                            read(sock, searchPatient, res_size);    //read result
                            searchPatient[res_size] = '\0';
                        }
                    }
                    if (strcmp(words[0], "/numPatientAdmissions") == 0) {
                        if (strlen(words[4]) < 2) {
                            free(words[4]);
                            words[4] = NULL;
                        }
                        if (words[4] == NULL) {     //country wasn't given
                            countryflag1 = 1;
                            if (numPatAdm == NULL)
                                numPatAdm = malloc(sizeof(char **) * numofWorkers);
                            int dirs = 0;
                            read(sock, &dirs, sizeof(int));
                            numPatAdm[work_num] = malloc(sizeof(char *) * dirs);
                            dirs_num[work_num] = dirs;
                            for (int i = 0; i < dirs; ++i) {  //we read results for each country
                                int res_size = 0;
                                read(sock, &res_size, sizeof(int));
                                numPatAdm[work_num][i] = malloc(sizeof(char) * (res_size + 1));
                                read(sock, numPatAdm[work_num][i], res_size);
                                numPatAdm[work_num][i][res_size] = '\0';

                            }
                        }
                        if (words[4] != NULL) {     //country was given
                            int res_size = 0;
                            read(sock, &res_size, sizeof(int));
                            if (res_size != 0) {    //we read result for given country
                                no_count_NPA = malloc(sizeof(char) * (res_size + 1));
                                read(sock, no_count_NPA, res_size);
                                no_count_NPA[res_size] = '\0';
                            }
                        }
                    }

                    if (strcmp(words[0], "/numPatientDischarges") == 0) {
                        if (words[4] != NULL)
                            if (strlen(words[4]) < 2) {
                                free(words[4]);
                                words[4] = NULL;
                            }
                        if (words[4] == NULL) {
                            countryflag1 = 1;
                            if (numPatDis == NULL)
                                numPatDis = malloc(sizeof(char **) * numofWorkers);
                            int dirs = 0;
                            read(sock, &dirs, sizeof(int));
                            numPatDis[work_num] = malloc(sizeof(char *) * dirs);
                            dirs_num[work_num] = dirs;
                            for (int i = 0; i < dirs; ++i) {
                                int res_size = 0;
                                read(sock, &res_size, sizeof(int));
                                numPatDis[work_num][i] = malloc(sizeof(char) * (res_size + 1));
                                read(sock, numPatDis[work_num][i], res_size);
                                numPatDis[work_num][i][res_size] = '\0';
                            }
                        }
                        if (words[4] != NULL) {
                            int res_size = 0;
                            read(sock, &res_size, sizeof(int));
                            if (res_size != 0) {
                                no_count_NPD = malloc(sizeof(char) * (res_size + 1));
                                read(sock, no_count_NPD, res_size);
                                no_count_NPD[res_size] = '\0';
                            }
                        }
                    }

                    if (strcmp(words[0], "/topk-AgeRanges") == 0) {
                        int flag = 0;
                        read(sock, &flag, sizeof(int));
                        if (flag == 1) {
                            int k = atoi(words[1]);
                            fk = k;
                            topK = malloc(sizeof(char *) * k);
                            for (int i = 0; i < k; k++) {
                                int res_size = 0;
                                read(sock, &res_size, sizeof(int));
                                topK[i] = malloc(sizeof(char) * (res_size + 1));
                                read(sock, topK[i], res_size);      //read results and store them in topk
                                topK[i][res_size] = '\0';
                                printf("%s\n", topK[i]);
                                int n;
                                n = write(fd, &res_size, sizeof(int));     //write size of result
                                if (n < 0)
                                    printf("Write error\n");
                                n = write(fd, topK[i], res_size);       //write result
                                if (n < 0)
                                    printf("Write error\n");
                            }
                        }

                    }
                    work_num++;
                    cur = cur->next;
                }

                if (disFreq_count != 0) {
                    write(fd, &disFreq_count, sizeof(int));
                }
                if (searchPatient != NULL) {
                    int size = strlen(searchPatient);
                    write(fd, &size, sizeof(int));
                    write(fd, searchPatient, size);
                }
                if (numPatAdm != NULL) {
                    int wk = work_num;
                    write(fd, &wk, sizeof(int));
                    for (int i = 0; i < wk; ++i) {
                        write(fd, &dirs_num[i], sizeof(int));
                        for (int j = 0; j < dirs_num[i]; ++j) {
                            int res_size = strlen(numPatAdm[i][j]);
                            write(fd, &res_size, sizeof(int));
                            write(fd, numPatAdm[i][j], res_size);
                        }
                    }
                }
                if (no_count_NPA != NULL) {
                    int size = strlen(no_count_NPA);
                    write(fd, &size, sizeof(int));
                    write(fd, no_count_NPA, size);
                }
                if (numPatDis != NULL) {
                    int wk = work_num;
                    write(fd, &wk, sizeof(int));
                    for (int i = 0; i < wk; ++i) {
                        write(fd, &dirs_num[i], sizeof(int));
                        for (int j = 0; j < dirs_num[i]; ++j) {
                            int res_size = strlen(numPatDis[i][j]);
                            write(fd, &res_size, sizeof(int));
                            write(fd, numPatDis[i][j], res_size);
                        }
                    }
                }
                if (no_count_NPD != NULL) {
                    int size = strlen(no_count_NPD);
                    write(fd, &size, sizeof(int));
                    write(fd, no_count_NPD, size);
                }
                /*if(topK!=NULL){
                    printf("WRITING TOPK\n");
                    for (int i = 0; i <fk ; ++i) {
                        int res_size=strlen(topK[i]);
                        write(fd,&res_size,sizeof(int));
                        write(fd,topK[i],res_size);
                    }
                }*/

                usleep(10);
            }
        }
        printf("looping\n");
    }
}

int main(int argc, char *argv[]) {
    if (argc != 9) {
        printf("Wrong number or arguments\n");
        exit(-1);
    }
    int err;
    struct pollfd pfd[2];
    int queryPortNum = atoi(argv[2]);
    int statisticsPortNum = atoi(argv[4]);
    int numThreads = atoi(argv[6]);
    int bufferSize = atoi(argv[8]);

    head = NULL;
    pthread_t *tids;
    int qport, sport, qsock, stat_sock, newsock;
    struct sockaddr_in qserver, stat_server, client;
    struct sockaddr *qserverptr = (struct sockaddr *) &qserver;
    struct sockaddr *stat_serverptr = (struct sockaddr *) &stat_server;
    struct sockaddr *clientptr = (struct sockaddr *) &client;
    cBuff = malloc(sizeof(circ_Buff));
    init_circ_Buff(cBuff, numThreads, bufferSize);
    pthread_mutex_init(&buff_mtx, 0);
    pthread_cond_init(&cond_nonempty, 0);
    pthread_cond_init(&cond_nonfull, 0);

    if ((tids = malloc(numThreads * sizeof(pthread_t))) == NULL)
        perror_exit("malloc");
    for (int i = 0; i < numThreads; i++) {
        if (err = pthread_create(tids + i, NULL, thread_func, NULL))        //create threads
            perror_exit("pthread_create");
    }

    if ((qsock = socket(AF_INET, SOCK_STREAM, 0)) < 0) {
        perror_exit("query socket");
    }
    qserver.sin_family = AF_INET;
    qserver.sin_addr.s_addr = htonl(INADDR_ANY);
    qserver.sin_port = htons(queryPortNum);
    if (bind(qsock, qserverptr, sizeof(qserver)) < 0)
        perror_exit("bind");
    if (listen(qsock, bufferSize) < 0)      //socket for queries
        perror_exit("listen");


    if ((stat_sock = socket(AF_INET, SOCK_STREAM, 0)) < 0) {
        perror_exit("query socket");
    }
    stat_server.sin_family = AF_INET;
    stat_server.sin_addr.s_addr = htonl(INADDR_ANY);
    stat_server.sin_port = htons(statisticsPortNum);
    if (bind(stat_sock, stat_serverptr, sizeof(stat_server)) < 0)
        perror_exit("bind");
    if (listen(stat_sock, bufferSize) < 0)          //socket for statistics
        perror_exit("listen");

    pfd[0].fd = qsock;
    pfd[0].events = POLLIN;
    pfd[1].fd = stat_sock;
    pfd[1].events = POLLIN;

    int count = 0;
    int clientlen = 0;
    int rec;
    while (1) {
        rec = poll(pfd, 2, -1);     //wait for connction
        if (rec == -1)
            perror_exit("poll");
        for (int i = 0; i < 2; ++i) {
            if (pfd[i].revents & POLLIN) {
                if (pfd[i].fd == qsock) {       //query
                    clientlen = sizeof(client);
                    if ((newsock = accept(qsock, clientptr, &clientlen)) < 0)   //accept connction
                        perror_exit("accept");

                    Push(cBuff, newsock, NULL, 'Q');    //put descriptor in circular buffer
                    pthread_cond_signal(&cond_nonempty);        //signal that buffer is not empty
                    usleep(10);
                }
                if (pfd[i].fd == stat_sock) {       //statistics
                    clientlen = sizeof(client);
                    if ((newsock = accept(stat_sock, clientptr, &clientlen)) < 0)   //accept connection
                        perror_exit("accept");
                    char *IP;
                    IP = inet_ntoa(client.sin_addr);        //get client's IP
                    printf("OK2\n");

                    Push(cBuff, newsock, IP, 'S');      //put descriptor in circular buffer
                    pthread_cond_signal(&cond_nonempty);     //signal that buffer is not empty
                    usleep(100);
                }
            }
        }

    }

    for (int i = 0; i < numThreads; ++i) {
        if (err = pthread_join(*(tids + i), NULL))
            perror_exit("pthread_join");
    }

    pthread_exit(NULL);
}