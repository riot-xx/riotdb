#ifndef DBVECTOR_H
#define DBVECTOR_H

#include <stdlib.h>
#include "mysql.h"

#include "R.h"
#include "Rinternals.h"
/*#include "Rdefines.h"*/

#include "mainDriver/rdb_basics.h"
#include "mainDriver/rdb_handle_vectors.h"
#include "mainDriver/rdb_handle_vector_tables.h"
#include "mainDriver/rdb_insert_vector_data.h"
#include "mainDriver/rdb_binary_operators.h"
#include "mainDriver/rdb_get_vector_data.h"
#include "mainDriver/rdb_set_vector_data.h"
#include "mainDriver/rdb_compare_vectors.h"
#include "mainDriver/rdb_aggregate_functions.h"
#include "mainDriver/rdb_element_functions.h"
#include "mainDriver/rdb_delete_vector_data.h"

#define MAX_TABLE_NAME 64

int IS_DBVECTOR(SEXP x);
rdbVector *getInfo(SEXP x);
int getType(SEXP x);
void rdbvectorFinalizer(SEXP);

#define IS_DBCOMPLEX(x) (getType(x)==CPLXSXP)
#define IS_DBINTEGER(x) (getType(x)==INTSXP)
#define IS_DBDOUBLE(x) (getType(x)==REALSXP)

SEXP add_dbvectors(SEXP x, SEXP y);

SEXP subtract_dbvectors(SEXP x, SEXP y);

SEXP multiply_dbvectors(SEXP x, SEXP y);
SEXP divide_dbvectors(SEXP x, SEXP y);
#define GET_RDBINFO(x) (rdbVector*)RAW((x))

#endif
