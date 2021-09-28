
#ifndef MAIN_RECORDS_H
#define MAIN_RECORDS_H

#endif //MAIN_RECORDS_H
typedef struct record {
    char *recordID;
    char *status;
    char *patientFirstName;
    char *patientLastName;
    char *diseaseID;
    int age;
    char *date;
} record;

typedef struct recordNode {
	//char *date;
    struct record *record;
    struct recordNode *next;
} recordNode;

