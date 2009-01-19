#ifndef DBVECTOR_H
#define DBVECTOR_H

#include <stdlib.h>
#include "mysql.h"

#include "R.h"
#include "Rinternals.h"
/*#include "Rdefines.h"*/

#include "clib/rdb_basics.h"
#include "clib/rdb_handle_vector_views.h"
#include "clib/rdb_convert_vectors.h"
#include "clib/rdb_handle_vector_tables.h"
#include "clib/rdb_insert_vector_data.h"
#include "clib/rdb_binary_operators.h"
#include "clib/rdb_get_vector_data.h"
#include "clib/rdb_set_vector_data.h"
#include "clib/rdb_compare_vectors.h"
#include "clib/rdb_aggregate_functions.h"
#include "clib/rdb_element_functions.h"
#include "clib/rdb_delete_vector_data.h"
#include "clib/rdb_sort_vector.h"
#include "clib/rdb_handle_matrix_views.h"
#include "clib/rdb_handle_matrix_tables.h"
#include "clib/rdb_insert_matrix_data.h"

#include "utils.h"

#define MAX_TABLE_NAME 64

#define IS_DBCOMPLEX(x) (getType(x)==CPLXSXP)
#define IS_DBINTEGER(x) (getType(x)==INTSXP)
#define IS_DBDOUBLE(x) (getType(x)==REALSXP)

#endif
