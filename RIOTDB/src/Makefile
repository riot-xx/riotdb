# "R CMD INSTALL RIOT" will call make with this Makefile
# debug options on for gcc
# feel free to change relevant directories
# author: Yi Zhang 1/14/09, v1

R_HOME=/usr/local/lib/R
MYSQL_INC=/usr/include/mysql
MYSQL_LIB=/usr/lib/mysql
LIB_DIR=clib
BIN=riotdb
CC=gcc
CFLAGS=-std=gnu99 -g -Wall -I$(MYSQL_INC) -I$(R_HOME)/include
PKG_CFLAGS=$(CFLAGS)

PLATFORM=$(shell uname -s)
ifeq ($(PLATFORM),SunOS)
	LIBS=-G -L$(MYSQL_LIB) -R$(MYSQL_LIB) -lmysqlclient -lnsl -lm -lz
else
# Hopefully this will work for all others like Linux, BSD, ...
	LIBS=-shared -L$(MYSQL_LIB) -lmysqlclient -lnsl -lm -lz
endif

OBJ := $(patsubst %.c,%.o, $(wildcard $(LIB_DIR)/*.c)) 
OBJ += $(patsubst %.c,%.o, $(wildcard *.c)) 

all: before $(SHLIB) 

before:$(OBJ)
#	R CMD SHLIB -g $(LIBS) -o riotdb.so $(OBJ) $(LIBS)
	$(CC) -std=gnu99 $(LIBS) -o $(BIN).so $(OBJ) 

after:
	rm -f *.o
	rm -f *.so
	rm -f $(LIB_DIR)/*.o
.c.o:
	$(CC) $(CFLAGS) -fpic -c $< -o $@
