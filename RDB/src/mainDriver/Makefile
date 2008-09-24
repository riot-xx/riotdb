# Makefile for building the C language shared libray
# Author: Herodotos Herodotou

# Useful flags
C = gcc
SQL_DIR = /usr/mysql
R_DIR = /usr/local/lib/R
OPTS    = -Wall -ansi -I$(SQL_DIR)/include/mysql -I$(R_DIR)/include -L$(SQL_DIR)/lib -L$(R_DIR)/lib -lmysqlclient -lmysqlclient -lnsl -lm -lz -std=c99
LOADER  = gcc

# All c files
FILES = rdb_basics.c rdb_handle_vector_tables.c rdb_insert_vector_data.c rdb_get_vector_data.c rdb_set_vector_data.c rdb_binary_operators.c rdb_handle_vector_views.c rdb_convert_vectors.c rdb_compare_vectors.c rdb_aggregate_functions.c rdb_element_functions.c rdb_delete_vector_data.c rdb_handle_metadata.c rdb_handle_matrix_tables.c rdb_handle_matrix_views.c rdb_insert_matrix_data.c rdb_get_matrix_data.c rdb_matrix_functions.c

# Compiling commands
all: vector matrix delete

vector: $(FILES) testVector.c
	$(C) $(OPTS) $(FILES) testVector.c -o testVector.out

matrix: $(FILES) testMatrix.c
	$(C) $(OPTS) $(FILES) testMatrix.c -o testMatrix.out

delete: $(FILES) testDelete.c
	$(C) $(OPTS) $(FILES) testDelete.c -o testDelete.out

# Perform regresion testing
regression: all
	make reset
	./testVector.out > output1.temp
	mysql rdb -umysql < sql_view_results.sql > output2.temp
	diff output1.temp outputMainVector.txt
	diff output2.temp outputSqlVector.txt

# Reset the RDB database
reset: delete
	./testDelete.out
	mysql rdb -umysql < sql_init_riot.sql
	mysql rdb -umysql < sql_view_results.sql

# Clear all temp files, executables and core files
clean:
	-rm -rf *~ *.out *.temp core
