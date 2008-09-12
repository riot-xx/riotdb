#Makefile for building the C language shared libray

C = gcc
RDIR = /usr/local/lib/R
# for building objects
OPTS    =  -g -I/usr/mysql/include/mysql -I$(RDIR)/include
# for building R dynamic lib
PKG_CFLAGS= -I/usr/mysql/include/mysql
PKG_LIBS= -L/usr/mysql/lib/mysql -R/usr/mysql/lib/mysql -lmysqlclient -lnsl -lm -lz

objects := $(patsubst %.c,%.o, $(wildcard *.c)) $(patsubst %.c,%.o, $(wildcard mainDriver/*.c))

OBJECTS = mainDriver/rdb_basics.o\
mainDriver/rdb_handle_vector_tables.o\
mainDriver/rdb_handle_vector_views.o\
mainDriver/rdb_insert_vector_data.o \
mainDriver/rdb_handle_vectors.o \
mainDriver/rdb_get_vector_data.o\
mainDriver/rdb_set_vector_data.o\
mainDriver/rdb_binary_operators.o\
mainDriver/rdb_delete_vector_data.o\
mainDriver/rdb_aggregate_functions.o\
mainDriver/rdb_element_functions.o\
mainDriver/rdb_sort_vector.o\
mainDriver/rdb_compare_vectors.o

all: dbvector_wrapper.so

dbvector_wrapper.so:  $(objects)
	@echo "make shared library"
	CFLAGS="-g " PKG_CFLAGS="$(PKG_CFLAGS)" PKG_LIBS="$(PKG_LIBS)" R CMD SHLIB -o $@ $^

.c.o:
	$(C) $(OPTS) -c $<

clean:
	-rm *.o *.so mainDriver/*.o
