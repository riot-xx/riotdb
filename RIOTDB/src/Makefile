# "R CMD INSTALL RIOT" will call make with this Makefile
# debug options on for gcc
# feel free to change relevant directories
# author: Yi Zhang 1/14/09, v1

R_HOME=/usr/local/lib/R
MYSQL_DIR=/usr/mysql
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
#	R CMD SHLIB -g $(LIBS) -o rdb.so $(OBJ) $(LIBS)
	$(CC) -std=gnu99 -G $(LIBS) -o $(BIN).so $(OBJ) 

after:
	rm *.o
	rm *.so
.c.o:
	$(CC) $(CFLAGS) -fpic -c $< -o $@
