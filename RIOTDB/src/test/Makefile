# Makefile for building the C language shared libray
# for the RIOT-DB Project and the testing code.
# Author: Herodotos Herodotou

# Directories
SQL_DIR = /usr/mysql
R_DIR   = /usr/local/lib/R
LIB_DIR = ../clib
SCRIPT_DIR = ../scripts

# Useful flags
C = gcc
OPTS    = -Wall -ansi \
          -I$(SQL_DIR)/include/mysql -I$(R_DIR)/include -I$(LIB_DIR) \
          -L$(SQL_DIR)/lib -L$(R_DIR)/lib \
          -lmysqlclient -lnsl -lm -lz -std=c99

# All library c files
FILES = $(LIB_DIR)/*.c

# Compiling commands
all: vector matrix delete

vector: $(FILES) testVector.c
	$(C) $(OPTS) $(FILES) testVector.c -o testVector.out

matrix: $(FILES) testMatrix.c
	$(C) $(OPTS) $(FILES) testMatrix.c -o testMatrix.out

delete: $(FILES) testDelete.c
	$(C) $(OPTS) $(FILES) testDelete.c -o testDelete.out

# Perform regresion testing
regression:
	make regression_vector
	make regression_matrix
	make reset

# Perform regresion testing for vectors
regression_vector: vector delete
	make reset
	./testVector.out > outputVec1.temp
	mysql rdb -umysql < $(SCRIPT_DIR)/sql_view_results.sql > outputVec2.temp
	diff outputVec1.temp outputMainVector.txt
	diff outputVec2.temp outputSqlVector.txt

# Perform regresion testing for matrices
regression_matrix: matrix delete
	make reset
	./testMatrix.out > outputMatr1.temp
	mysql rdb -umysql < $(SCRIPT_DIR)/sql_view_results.sql > outputMatr2.temp
	diff outputMatr1.temp outputMainMatrix.txt
	diff outputMatr2.temp outputSqlMatrix.txt

# Reset the database
reset: delete
	./testDelete.out
	mysql rdb -umysql < $(SCRIPT_DIR)/sql_init_riot.sql
	mysql rdb -umysql < $(SCRIPT_DIR)/sql_view_results.sql

# Clear all temp files, executables and core files
clean:
	-rm -rf *~ *.out *.temp core
