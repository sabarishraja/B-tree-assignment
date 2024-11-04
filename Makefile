CFLAGS = -Wno-implicit-function-declaration
CC = gcc

ifeq ($(OS),Windows_NT)
	RM= del
	TEST1_EXECUTE_FILE = ./test_assign4_1.exe
	TEST2_EXECUTE_FILE = ./test_expr.exe
else
	RM= rm -f
	TEST1_EXECUTE_FILE = ./test_assign4_1
	TEST2_EXECUTE_FILE = ./test_expr
endif

dberror.o: dberror.c dberror.h
	echo "compiling dberror.c"
	$(CC) $(CFLAGS) -c dberror.c

storage_mgr.o: storage_mgr.c storage_mgr.h
	echo "Compiling the storage_mgr file"
	$(CC) $(CFLAGS) -c storage_mgr.c

buffer_mgr.o: buffer_mgr.c buffer_mgr.h dt.h storage_mgr.h
	echo "Compiling the buffer_mgr file"
	$(CC) $(CFLAGS) -c buffer_mgr.c

buffer_mgr_stat.o: buffer_mgr_stat.c buffer_mgr_stat.h buffer_mgr.h
	echo "Compiling the buffer mgr stat file"
	$(CC) $(CFLAGS) -c buffer_mgr_stat.c

btree_mgr.o: btree_mgr.c btree_mgr.h
	echo "Compiling the btree manager file"
	$(CC) $(CFLAGS) -c btree_mgr.c

rm_serializer.o: rm_serializer.c dberror.h tables.h record_mgr.h
	echo "Compiling the rm serializer file"
	$(CC) $(CFLAGS) -c rm_serializer.c

expr.o: expr.c dberror.h record_mgr.h expr.h tables.h
	echo "compiling the expr file"
	$(CC) $(CFLAGS) -c expr.c

record_mgr.o: record_mgr.c record_mgr.h buffer_mgr.h storage_mgr.h
	echo "compiling the record manager file"
	$(CC) $(CFLAGS) -c record_mgr.c

test_expr.o: test_expr.c storage_mgr.h dberror.h expr.h btree_mgr.h tables.h test_helper.h
	echo "compiling the test_expr file"
	$(CC) $(CFLAGS) -c test_expr.c

test_assign4_1.o: test_assign4_1.c dberror.h storage_mgr.h test_helper.h buffer_mgr.h buffer_mgr_stat.h record_mgr.h btree_mgr.h
	echo "compiling the test_assign4_1 file"
	$(CC) $(CFLAGS) -c test_assign4_1.c

test_assign4_1: test_assign4_1.o dberror.o expr.o record_mgr.o rm_serializer.o storage_mgr.o buffer_mgr.o buffer_mgr_stat.o btree_mgr.o
	echo "linking file to generate test_assign4_1 file"
	$(CC) $(CFLAGS) -o test_assign4_1 test_assign4_1.o dberror.o expr.o record_mgr.o rm_serializer.o storage_mgr.o buffer_mgr.o buffer_mgr_stat.o btree_mgr.o

test_expr: test_expr.o dberror.o storage_mgr.o expr.o record_mgr.o rm_serializer.o storage_mgr.o buffer_mgr.o buffer_mgr_stat.o btree_mgr.o
	echo "linking file to generate the final file of test_expr"
	$(CC) $(CFLAGS) -o test_expr storage_mgr.o buffer_mgr.o buffer_mgr_stat.o btree_mgr.o rm_serializer.o expr.o record_mgr.o test_expr.o dberror.o  

execute_test1: 
	echo "executing test_assign4_1"
	$(TEST1_EXECUTE_FILE)

execute_test2: 
	echo "executing test_expr"
	$(TEST2_EXECUTE_FILE)

clean:
	echo "removing generated files"
	$(RM) *.o test_assign4_1 test_assign4_1.exe test_expr test_expr.exe testidx