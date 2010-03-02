# "R CMD INSTALL RIOT" will call make with this Makefile
# debug options on for gcc
# feel free to change relevant directories
# author: Yi Zhang 1/14/09, v1

R_HOME=/home/home4/junyang/R/lib64/R
MYSQL_DIR=/home/home4/junyang/mysql
LIB_DIR=clib
BIN=riotdb
CC=gcc
CFLAGS=-std=gnu99 -g -Wall -I$(MYSQL_DIR)/include/mysql -I$(R_HOME)/include
PKG_CFLAGS=$(CFLAGS)

PLATFORM=$(shell uname -s)
ifeq ($(PLATFORM),SunOS)
	LIBS=-L$(MYSQL_DIR)/lib/mysql -R$(MYSQL_DIR)/lib/mysql -lmysqlclient -lnsl -lm -lz
else
	LIBS=-L$(MYSQL_DIR)/lib/mysql -lmysqlclient -lnsl -lm -lz
endif

OBJ := $(patsubst %.c,%.o, $(wildcard $(LIB_DIR)/*.c)) 
OBJ += $(patsubst %.c,%.o, $(wildcard *.c)) 

all: before $(SHLIB) 

before:$(OBJ)
	/home/home4/junyang/R/bin/R CMD SHLIB -g $(LIBS) -o riotdb.so $(OBJ) $(LIBS)
#	$(CC) -std=gnu99 -G $(LIBS) -o $(BIN).so $(OBJ) 
#	$(CC) -std=gnu99 -g $(LIBS) -o $(BIN).so $(OBJ) 

after:
	rm -f *.o
	rm -f *.so
	rm -f $(LIB_DIR)/*.o
.c.o:
	$(CC) $(CFLAGS) -fpic -c $< -o $@
