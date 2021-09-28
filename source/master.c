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



struct worker_stats {   //struct for worker
    pid_t worker_id;
    char send[512];
    char receive[512];
    char **directories;
    int total_dirs;
};

int main(int argc, char *argv[]) {
    if (argc != 11) {
        printf("Invalid number of arguments\n");
        exit(-1);
    }

    int numWorkers = atoi(argv[2]);
    int bufferSize = atoi(argv[4]);
    int serverIP=atoi(argv[6]);
    int serverPort=atoi(argv[8]);
    const char *input_dir = argv[10];
    struct worker_stats *worker_table[numWorkers];
    pid_t pid = getpid();    //parent id
    int status;

    int numofSubDirs = 0;
    char buff[bufferSize];
    memset(buff, 0, bufferSize);
    ssize_t n;
    struct pollfd pfd[numWorkers];      //struct for writing fifos
    //struct pollfd rpfd[numWorkers];     //struct for read fifos
    for (int i = 0; i < numWorkers; i++) {      //initialize them
        pfd[i].events = POLLOUT;
        pfd[i].revents = 0;
    }
    int ret;

    DIR *dir;
    struct dirent *rdir;

    dir = opendir(input_dir);   //open directory
    if (dir == NULL) {
        perror("DIR");
        exit(-2);
    } else {
        while ((rdir = readdir(dir)) != NULL) {     //read directory and count subdirs
            if (strcmp(rdir->d_name, ".") != 0 && strcmp(rdir->d_name, "..") != 0) {
                numofSubDirs++;
            }
        }
        closedir(dir);
    }

    int dir_pos_start = 0;
    int dir_pos_end = 0;
    int d = numofSubDirs / numWorkers;
    int m = numofSubDirs % numWorkers;
    dir_pos_end = d;


    char curWorkDir[256];
    int tempd = numofSubDirs;
    int fifo_toWorker[numWorkers];
    int fifo_fromWorker[numWorkers];

    char *tmp_storage[numWorkers];

    for (int i = 0; i < numWorkers; i++) {

        if ((getcwd(curWorkDir, sizeof(curWorkDir))) == NULL) {     //get current working dir
            perror("getcwd");
            exit(3);
        }
        worker_table[i] = malloc(sizeof(struct worker_stats));
        char temp_s[512];
        sprintf(temp_s, "%s/fifo%d_receive", curWorkDir, i + 1);    //make names for receive fifo
        strcpy(worker_table[i]->receive, temp_s);
        if ((mkfifo(worker_table[i]->receive, 0666) < 0) &&
            (errno != EEXIST)) {
            //unlink(worker_table[i].send);
            perror("can't create fifo");
            exit(4);
        }

        if ((pid = fork()) < 0) {       //fork children and check if fork failed
            perror("fork");
            exit(2);
        }

        if (pid == 0) {
            //child
            int retval = 0;
            char str[12];
            char worker_num[12];
            sprintf(worker_num, "%d", i);
            sprintf(str, "%d", bufferSize);
            //child calls worker with exec
            char cwd[100];
            if (getcwd(cwd,sizeof(cwd))==NULL){
                perror("getcwd");
            }
            char Worker[200];
            sprintf(Worker,"%s/Worker",cwd);
            retval = execl(/*"/home/gfarao/syspro3/Worker"*/Worker, /*"/home/gfarao/syspro3/Worker"*/Worker,
                           worker_table[i]->receive, str, worker_num, (char *) NULL);
            if (retval == -1)   //execl failed
                perror("execl");
            exit(0);
        }
        if (pid != 0) {     //parent
            worker_table[i]->worker_id = pid;
        }
    }

    sleep(2);

    char end[5];
    strcpy(end, "end");
    int total_dirs[numWorkers];
    char **directories;
    directories = malloc(sizeof(char *) * numWorkers);
    char **final_value;
    final_value = malloc(sizeof(char *) * numWorkers);
    for (int i = 0; i < numWorkers; ++i) {
        if (i == (numWorkers - 1) && m != 0 && tempd >= 0) {    //give the rest of the directories left
            dir_pos_end += m;
            tempd = tempd - m;
        }

        int temp_start = 0;
        total_dirs[i] = dir_pos_end - dir_pos_start;    //dirs for worker

        if (total_dirs != 0) {
            char **dir_array;
            dir_array = malloc(total_dirs[i] * sizeof(char *));     //array of dirs
            worker_table[i]->directories = malloc(total_dirs[i] * sizeof(char *));
            worker_table[i]->total_dirs = total_dirs[i];
            dir = opendir(input_dir);
            if (dir == NULL) {
                perror("DIR");
                exit(-2);
            }
            int cnt = 0;
            while ((rdir = readdir(dir)) != NULL) {     //store each directory name in the array
                if (strcmp(rdir->d_name, ".") != 0 && strcmp(rdir->d_name, "..") != 0) {
                    if (temp_start >= dir_pos_start && temp_start <= dir_pos_end && cnt < total_dirs[i]) {
                        n = strlen(rdir->d_name);
                        dir_array[cnt] = NULL;
                        dir_array[cnt] = malloc(sizeof(char) * (n + 1));
                        worker_table[i]->directories[cnt] = malloc(sizeof(char) * (n + 1));
                        strcpy(dir_array[cnt], rdir->d_name);
                        strcpy(worker_table[i]->directories[cnt], rdir->d_name);
                        cnt++;

                    }
                    if (temp_start >= dir_pos_end) {

                        break;
                    }
                    temp_start++;
                }

            }

            int size = 0;
            for (int j = 0; j < total_dirs[i]; j++) {   //count size of message
                if (dir_array[j] != NULL)
                    size = size + strlen(dir_array[j]) + strlen("#");
            }

            directories[i] = malloc(sizeof(char) * (size + 1));
            int l = 0;
            for (int j = 0; j < total_dirs[i]; j++) {   //store the whole message in directories array
                if (dir_array[j] != NULL) {
                    for (int m = 0; m < strlen(dir_array[j]); m++) {
                        directories[i][l] = dir_array[j][m];
                        l++;
                    }
                    directories[i][l] = '#';
                    l++;
                    free(dir_array[j]);
                }
            }
            directories[i][size] = '\0';

            closedir(dir);
            dir_pos_start = dir_pos_end;
            dir_pos_end += d;
            if (dir_pos_end > numofSubDirs)
                dir_pos_end = dir_pos_start;
            tempd = tempd - d;


            free(dir_array);
        }
    }

    int finished_array[numWorkers];
    for (int j = 0; j < numWorkers; j++) {      //flags
        finished_array[j] = -1;
    }

    for (int j = 0; j < numWorkers; j++) {      //make final  message
        char temp[2];
        sprintf(temp, "%d", total_dirs[j]);
        final_value[j] = malloc(strlen(directories[j]) + strlen(temp) + 2);
        strcpy(final_value[j], temp);
        strcat(final_value[j], "$");
        strcat(final_value[j], directories[j]);
    }

    for (int j = 0; j < numWorkers; j++) {      //open fifos for writing
        if ((fifo_toWorker[j] = open(worker_table[j]->receive, O_WRONLY)) < 0) {
            perror("parent: can't open send fifo ");
        }
        pfd[j].fd = fifo_toWorker[j];
    }

    int f_cnt = 0;
    while (1) {     //while we have worker to write to
        ret = poll(pfd, numWorkers, -1);        //call poll to wait for available worker
        if (ret == -1) {
            perror("poll");
            exit(-2);
        }
        if (ret > 0) {
            for (int i = 0; i < numWorkers; i++) {
                if (pfd[i].revents & POLLOUT) {     //if there is fifo ready
                    //printf("%s\n",worker_table[i]->send );
                    int message_size;
                    message_size = strlen(final_value[i]);
                    if (write(fifo_toWorker[i], &message_size, sizeof(int)) !=
                        sizeof(int)) {   //first write size of the message
                        perror("server: errmesg write error");
                    }
                    if (message_size > bufferSize)
                        memmove(buff, final_value[i], bufferSize);
                    else
                        memmove(buff, final_value[i], message_size);
                    int bytesWritten = 0;
                    int bytesToWrite = strlen(final_value[i]);
                    while (bytesToWrite > 0) {      //while we havent writen the whole message

                        size_t bytes = write(fifo_toWorker[i], buff, bufferSize);   //write buffersize bytes
                        if (bytes < 0) {
                            perror("Write dir size");
                            exit(-5);
                        }
                        bytesToWrite = bytesToWrite - bytes;
                        bytesWritten += bytes;
                        if (bytesToWrite > 0) {     //if there are bytes left to write
                            if (bytesToWrite > bufferSize)
                                memmove(buff, final_value[i] + bytesWritten,
                                        bufferSize);   //store them in buff and continue writing
                            else
                                memmove(buff, final_value[i] + bytesWritten, bytesToWrite);
                        }
                    }
                    finished_array[i] = 0;
                }
                if (pfd[i].revents & POLLHUP) {
                    continue;
                }

            }
        }
        for (int k = 0; k < numWorkers; k++) {      //check who has finished
            if (finished_array[k] == 0)
                f_cnt++;
        }
        if (f_cnt == numWorkers)        //if all workers have finished break loop
            break;
        else
            f_cnt = 0;

    }


    for (int j = 0; j < numWorkers; j++) {
        finished_array[j] = -1;
    }
    f_cnt = 0;

    int sec_mess_size=strlen(argv[6]) +strlen(argv[8]) +2;
    char *sec_mess=malloc(sec_mess_size+1);
    int move=0;
    memmove(sec_mess,argv[6],strlen(argv[6]));
    move+=strlen(argv[6]);
    memmove(sec_mess+move,"#",strlen("#"));
    move+=strlen("#");
    memmove(sec_mess+move,argv[8],strlen(argv[8]));
    move+=strlen(argv[8]);
    memmove(sec_mess+move,"#",strlen("#"));
    sec_mess[sec_mess_size]='\0';


    while (1) {     //while we have worker to write to
        ret = poll(pfd, numWorkers, -1);        //call poll to wait for available worker
        if (ret == -1) {
            perror("poll");
            exit(-2);
        }
        if (ret > 0) {
            for (int i = 0; i < numWorkers; i++) {
                if (pfd[i].revents & POLLOUT) {     //if there is fifo ready
                    int message_size;
                    message_size = sec_mess_size;
                    if (write(fifo_toWorker[i], &message_size, sizeof(int)) !=
                        sizeof(int)) {   //first write size of the message
                        perror("server: errmesg write error");
                    }
                    if (message_size > bufferSize)
                        memmove(buff, sec_mess, bufferSize);
                    else
                        memmove(buff, sec_mess, message_size);
                    int bytesWritten = 0;
                    int bytesToWrite = strlen(sec_mess);
                    while (bytesToWrite > 0) {      //while we havent writen the whole message

                        size_t bytes = write(fifo_toWorker[i], buff, bufferSize);   //write buffersize bytes
                        if (bytes < 0) {
                            perror("Write dir size");
                            exit(-5);
                        }
                        bytesToWrite = bytesToWrite - bytes;
                        bytesWritten += bytes;
                        if (bytesToWrite > 0) {     //if there are bytes left to write
                            if (bytesToWrite > bufferSize)
                                memmove(buff, sec_mess + bytesWritten,
                                        bufferSize);   //store them in buff and continue writing
                            else
                                memmove(buff, sec_mess + bytesWritten, bytesToWrite);
                        }
                    }
                    finished_array[i] = 0;
                }
                if (pfd[i].revents & POLLHUP) {
                    continue;
                }

            }
        }
        for (int k = 0; k < numWorkers; k++) {      //check who has finished
            if (finished_array[k] == 0)
                f_cnt++;
        }
        if (f_cnt == numWorkers)        //if all workers have finished break loop
            break;
        else
            f_cnt = 0;

    }

    for (int i = 0; i < numWorkers; i++) {  //free memory
        free(final_value[i]);
        free(directories[i]);
        free(worker_table[i]);
    }
    free(final_value);
    free(directories);
    while (1) {     //wait for children to finish
        int status;
        pid_t finished = wait(&status);
        if (finished == -1) {
            if (errno == ECHILD)
                break;
        } else {
            if (!WIFEXITED(status) || WEXITSTATUS(status) != 0) {
                printf("Process with pid %d failed\n", finished);
                //exit(1);
            }
        }
    }
    return 0;

}