#ifndef DBVECTOR_H
#define DBVECTOR_H

#include <stdlib.h>
#include "mysql.h"

#include "R.h"
#include "Rinternals.h"
/*#include "Rdefines.h"*/

#include "clib/basics.h"
#include "clib/handle_vector_views.h"
#include "clib/convert_vectors.h"
#include "clib/handle_vector_tables.h"
#include "clib/insert_vector_data.h"
#include "clib/vector_binary_ops.h"
#include "clib/get_vector_data.h"
#include "clib/set_vector_data.h"
#include "clib/compare_vectors.h"
#include "clib/vector_aggregate_functions.h"
#include "clib/vector_element_functions.h"
#include "clib/delete_vector_data.h"
#include "clib/sort_vector.h"
#include "clib/handle_matrix_views.h"
#include "clib/handle_matrix_tables.h"
#include "clib/insert_matrix_data.h"

#include "utils.h"

#define MAX_TABLE_NAME 64

#define IS_DBCOMPLEX(x) (getType(x)==CPLXSXP)
#define IS_DBINTEGER(x) (getType(x)==INTSXP)
#define IS_DBDOUBLE(x) (getType(x)==REALSXP)

#endif
