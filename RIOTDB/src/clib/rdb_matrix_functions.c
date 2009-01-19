/*****************************************************************************
 * Contains functions for matrices (multiplication)
 *
 * Author: Herodotos Herodotou
 * Date:   Sep 23, 2008
 ****************************************************************************/

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <mysql.h>
#include "Rinternals.h"
#include "rdb_basics.h"
#include "rdb_matrix_functions.h"
#include "rdb_handle_matrix_views.h"


int performMatrixMultiplication(MYSQL * sqlConn, rdbMatrix * result, 
		                rdbMatrix * input1, rdbMatrix * input2)
{
  /* Both inputs must either be integers or doubles */
  if( input1->sxp_type != SXP_TYPE_INTEGER &&
      input1->sxp_type != SXP_TYPE_DOUBLE )
    return 0;

  if( input2->sxp_type != SXP_TYPE_INTEGER &&
      input2->sxp_type != SXP_TYPE_DOUBLE )
    return 0;

  /* Dimensions must match */
  if( input1->numCols != input2->numRows )
    return 0;

  /* Build the sql string */
  int length = strlen(sqlTemplateMatrixMultiplication) + 
               strlen(input1->tableName) + strlen(input2->tableName) + 1;
  char * sqlMultiply = (char*) malloc( length * sizeof(char*) );
  sprintf(sqlMultiply, sqlTemplateMatrixMultiplication,
	  input1->tableName, input2->tableName);

  /* Create the view with the results */
  initRDBMatrix(&result, 1, 0);
  result->numRows = input1->numRows;
  result->numCols = input2->numCols;
  
  int success = 0;
  if( input1->sxp_type == SXP_TYPE_INTEGER && 
      input2->sxp_type == SXP_TYPE_INTEGER )
  {
     success = createNewIntegerMatrixView(sqlConn, result, sqlMultiply);
  }
  else
  {
     success = createNewDoubleMatrixView(sqlConn, result, sqlMultiply);
  }
  free(sqlMultiply);

  /* Create the references */
  if( success )
  {
     createMatrixViewReferences(sqlConn, result, input1, input2);
  }
  else
  {
    result->numRows = 0;
    result->numCols = 0;
  }

  return success;
}


