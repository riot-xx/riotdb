#Makefile for building the C language shared libray

C = gcc
OPTS    = -Wall -ansi -I/usr/local/include/mysql -I/usr/local/R/include -L/usr/local/mysql/lib -L/usr/local/R/lib -lmysqlclient -lmysqlclient -lnsl -lm -lz -std=c99
LOADER  = gcc

FILES = rdb_basics.c rdb_handle_vector_tables.c rdb_insert_vector_data.c rdb_get_vector_data.c rdb_set_vector_data.c rdb_binary_operators.c rdb_handle_vector_views.c rdb_handle_vectors.c rdb_compare_vectors.c rdb_aggregate_functions.c rdb_element_functions.c rdb_delete_vector_data.c mainDriver.c

mainDriver: 
	$(C) $(OPTS) $(FILES)

clean:
	-rm *~
