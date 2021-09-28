all: Worker2 diseaseAggregator

diseaseAggregator: diseaseAggregator2.o avl.o
	gcc -g diseaseAggregator2.o avl.o -o diseaseAggregator

avl.o: avl.h records.h
	gcc -g -c avl.c
Worker2: Worker2.c  hashtable.h
	gcc -g -o Worker2 Worker2.c
diseaseAggregator2.o: diseaseAggregator2.c
	gcc -g -c diseaseAggregator2.c
clean:
	rm *.o
