/*****************************************************************************
 * Contains functions for matrices (multiplication, solve)
 *
 * Author: Herodotos Herodotou, Jun Yang
 * Date:   Sep 23, 2008
 ****************************************************************************/

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <mysql.h>
#include <assert.h>
#include "Rinternals.h"
#include "basics.h"
#include "matrix_functions.h"
#include "handle_matrix_views.h"
#include "handle_matrix_tables.h"

int restructureMatrixFromVector(MYSQL *sqlConn, rdbMatrix *result,
                                rdbVector *input, int nrows, int ncols, int byrow) {

  /* Build the sql string: */
  char *sql;
  if (byrow) {
    sql = (char *) malloc(sizeof(char) *
                          (strlen(sqlTemplateMatrixFromVectorByRow) + 4*MAX_INT_LENGTH + 1));
    sprintf(sql, sqlTemplateMatrixFromVectorByRow, ncols, ncols, ncols, ncols, input->tableName);
  } else {
    sql = (char *) malloc(sizeof(char) *
                          (strlen(sqlTemplateMatrixFromVectorByCol) + 4*MAX_INT_LENGTH + 1));
    sprintf(sql, sqlTemplateMatrixFromVectorByCol, nrows, nrows, nrows, nrows, input->tableName);
  }

  /* Create the view with the results: */
  result->sxp_type = SXP_TYPE_DOUBLE;
  result->isView = TRUE;
  result->numRows = nrows;
  result->numCols = ncols;

  if (!createNewDoubleMatrixView(sqlConn, result, sql)) {
    free(sql);
    return FALSE;
  }
  free(sql);

  /* Create the references: */
  if (!createMatrixViewReferencesToVectors(sqlConn, result, input, input)) {
    return FALSE;
  }

  return TRUE;
}

int solveByGuassianElimination(MYSQL *sqlConn, rdbMatrix *result, rdbMatrix *A, rdbMatrix *b) {
  /* Santity checks: */
  if (A->numRows != b->numRows ||
      b->numCols != 1)
    return FALSE;

  result->sxp_type = SXP_TYPE_DOUBLE;
  result->isView = FALSE;
  result->numRows = A->numCols;
  result->numCols = b->numCols;

  const char *sqlTemplate;
  {
    if (mysql_query(sqlConn, "DROP VIEW IF EXISTS ge_solve_A") != 0)
      return FALSE;
    sqlTemplate =
      "CREATE VIEW ge_solve_A(i, j, v) AS "
      "(SELECT mRow, mCol, CAST(mValue AS DECIMAL)"
      " FROM %s)";
    char sql[strlen(sqlTemplate)+strlen(A->tableName)+1];
    sprintf(sql, sqlTemplate, A->tableName);
    if (mysql_query(sqlConn, sql) != 0)
      return FALSE;
  }
  {
    if (mysql_query(sqlConn, "DROP VIEW IF EXISTS ge_solve_b") != 0)
      return FALSE;
    sqlTemplate =
      "CREATE VIEW ge_solve_b(i, v) AS "
      "(SELECT mRow, CAST(mValue AS DECIMAL)"
      " FROM %s)";
    char sql[strlen(sqlTemplate)+strlen(b->tableName)+1];
    sprintf(sql, sqlTemplate, b->tableName);
    if (mysql_query(sqlConn, sql) != 0)
      return FALSE;
  }
  if (!createNewDoubleMatrixTable(sqlConn, result))
    return FALSE;
  {
    if (mysql_query(sqlConn, "DROP VIEW IF EXISTS ge_solve_x") != 0)
      return FALSE;
    sqlTemplate =
      "CREATE VIEW ge_solve_x(i, v) AS "
      "(SELECT mRow, mValue"
      " FROM %s)";
    char sql[strlen(sqlTemplate)+strlen(result->tableName)+1];
    sprintf(sql, sqlTemplate, result->tableName);
    if (mysql_query(sqlConn, sql) != 0)
      return FALSE;
  }
  {
    sqlTemplate = "CALL ge_solve(%d, %d)";
    char sql[strlen(sqlTemplate)+2*MAX_INT_LENGTH + 1];
    sprintf(sql, sqlTemplate, A->numRows, A->numCols);
    if (mysql_query(sqlConn, sql) != 0)
      return FALSE;
  }
  {
    /* This step is needed because ge_solve doesn't see and therefore
       doesn't set the column index. */
    sqlTemplate = "UPDATE %s SET mCol = 1";
    char sql[strlen(sqlTemplate)+strlen(result->tableName)+1];
    sprintf(sql, sqlTemplate, result->tableName);
    if (mysql_query(sqlConn, sql) != 0)
      return FALSE;
    /* Check for abnormal exit of the stored procedure. */
    if (mysql_affected_rows(sqlConn) != result->numRows * result->numCols)
      return FALSE;
  }
  if (mysql_query(sqlConn, "DROP VIEW ge_solve_x") != 0)
    return FALSE;
  if (mysql_query(sqlConn, "DROP VIEW ge_solve_b") != 0)
    return FALSE;
  if (mysql_query(sqlConn, "DROP VIEW ge_solve_A") != 0)
    return FALSE;

  return TRUE;
}

int computeDoubleMatrixInverse(MYSQL *sqlConn, rdbMatrix *result, 
                               rdbMatrix *input) {

  /* Sanity checks: */
  if (input->numRows != input->numCols)
    return FALSE;

  result->sxp_type = SXP_TYPE_DOUBLE;
  result->isView = FALSE;
  result->numRows = input->numRows;
  result->numCols = input->numCols;

  const char *sqlTemplate;
  {
    if (mysql_query(sqlConn, "DROP VIEW IF EXISTS gj_invert_A") != 0)
      return FALSE;
    sqlTemplate =
      "CREATE VIEW gj_invert_A(i, j, v) AS "
      "(SELECT mRow, mCol, CAST(mValue AS DECIMAL)"
      " FROM %s)";
    char sql[strlen(sqlTemplate)+strlen(input->tableName)+1];
    sprintf(sql, sqlTemplate, input->tableName);
    if (mysql_query(sqlConn, sql) != 0)
      return FALSE;
  }
  if (!createNewDoubleMatrixTable(sqlConn, result))
    return FALSE;
  {
    if (mysql_query(sqlConn, "DROP VIEW IF EXISTS gj_invert_B") != 0)
      return FALSE;
    sqlTemplate =
      "CREATE VIEW gj_invert_B(i, j, v) AS "
      "(SELECT mRow, mCol, mValue"
      " FROM %s)";
    char sql[strlen(sqlTemplate)+strlen(result->tableName)+1];
    sprintf(sql, sqlTemplate, result->tableName);
    if (mysql_query(sqlConn, sql) != 0)
      return FALSE;
  }
  {
    sqlTemplate = "CALL gj_invert(%d)";
    char sql[strlen(sqlTemplate)+MAX_INT_LENGTH + 1];
    sprintf(sql, sqlTemplate, input->numRows);
    if (mysql_query(sqlConn, sql) != 0)
      return FALSE;
  }
  {
    /* Check for abnormal exit of the stored procedure. */
    sqlTemplate = "SELECT COUNT(*) FROM %s";
    char sql[strlen(sqlTemplate)+strlen(result->tableName)+1];
    sprintf(sql, sqlTemplate, result->tableName);
    if (mysql_query(sqlConn, sql) != 0)
      return FALSE;
    MYSQL_RES *sqlRes = mysql_use_result(sqlConn);
    MYSQL_ROW sqlRow = mysql_fetch_row(sqlRes);
    if (sqlRow == NULL || sqlRow[1] == NULL ||
        atoi(sqlRow[0]) != input->numRows * input->numCols) {
      mysql_free_result(sqlRes);
      return FALSE;
    }
    mysql_free_result(sqlRes);
  }
  if (mysql_query(sqlConn, "DROP VIEW gj_invert_B") != 0)
    return FALSE;
  if (mysql_query(sqlConn, "DROP VIEW gj_invert_A") != 0)
    return FALSE;

  return TRUE;
}

int computeMatrixeTranspose(MYSQL *sqlconn, rdbMatrix *result,
                            rdbMatrix *input) {

  result->sxp_type = input->sxp_type;
  result->isView = TRUE;
  result->numRows = input->numCols;
  result->numCols = input->numRows;

  const char *sqlTemplate =
    "SELECT mRow AS mCol, mCol AS mRow, mValue FROM %s";
  char sql[strlen(sqlTemplate)+strlen(input->tableName)+1];
  sprintf(sql, sqlTemplate, input->tableName);
  assert(createNewDoubleMatrixView(sqlconn, result, sql));
  createMatrixViewReferences(sqlconn, result, input, input);
  return TRUE;
}

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
  char * sqlMultiply = (char*) malloc( length * sizeof(char) );
  sprintf(sqlMultiply, sqlTemplateMatrixMultiplication,
	  input1->tableName, input2->tableName);

  /* Create the view with the results */
  result->isView = TRUE;
  result->numRows = input1->numRows;
  result->numCols = input2->numCols;
  
  int success = 0;
  if( input1->sxp_type == SXP_TYPE_INTEGER && 
      input2->sxp_type == SXP_TYPE_INTEGER )
  {
     result->sxp_type = SXP_TYPE_INTEGER;
     success = createNewIntegerMatrixView(sqlConn, result, sqlMultiply);
  }
  else
  {
     result->sxp_type = SXP_TYPE_DOUBLE;
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


