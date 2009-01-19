/*****************************************************************************
 * Contains functions for accessing elements in matrices
 *
 * Author: Herodotos Herodotou
 * Date:   Sep 23, 2008
 ****************************************************************************/
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <mysql.h>
#include "Rinternals.h"
#include "basics.h"
#include "get_matrix_data.h"

/* --------- Functions to access matrix tables by element -------------- */

int getIntMatrixElement(MYSQL * sqlConn, rdbMatrix * matrixInfo, 
		  	unsigned int row, unsigned int col, 
			int * value, char * byte)
{
  /* Execute the query to get element */
  if( !execGetMatrixElementSQLCall(sqlConn, matrixInfo->tableName, row, col) )
    return 0;

  /* Get the value */
  MYSQL_RES *sqlRes;
  MYSQL_ROW sqlRow;
  sqlRes = mysql_use_result(sqlConn);
  *byte = 0;

  if( (sqlRow = mysql_fetch_row(sqlRes)) != NULL )
  {
    if(sqlRow[0] != NULL)
    {
      *value = atoi(sqlRow[0]);
      *byte = 1;
    }
  }

  /* Clear out the results set */
  while( (sqlRow = mysql_fetch_row(sqlRes)) != NULL );
  mysql_free_result(sqlRes);

  return 1;
}


int getDoubleMatrixElement(MYSQL * sqlConn, rdbMatrix * matrixInfo, 
		  	   unsigned int row, unsigned int col, 
			   double * value, char * byte)
{
  /* Execute the query to get element */
  if( !execGetMatrixElementSQLCall(sqlConn, matrixInfo->tableName, row, col) )
    return 0;

  /* Get the value */
  MYSQL_RES *sqlRes;
  MYSQL_ROW sqlRow;
  sqlRes = mysql_use_result(sqlConn);
  *byte = 0;

  if( (sqlRow = mysql_fetch_row(sqlRes)) != NULL )
  {
    if(sqlRow[0] != NULL)
    {
      *value = strtod(sqlRow[0], NULL);
      *byte = 1;
    }
  }

  /* Clear out the results set */
  while( (sqlRow = mysql_fetch_row(sqlRes)) != NULL );
  mysql_free_result(sqlRes);

  return 1;
}


int execGetMatrixElementSQLCall(MYSQL * sqlConn, char* tableName, 
			  	unsigned int row, unsigned int col)
{
  /* Build the sql string */
  int length = strlen(sqlTemplateGetSingleMatrixValue) + strlen(tableName) + 
               2*MAX_INT_LENGTH + 1;
  char strGetElem[length];
  sprintf( strGetElem, sqlTemplateGetSingleMatrixValue, tableName, row, col );

  /* Execute the query */
  int success = mysql_query(sqlConn, strGetElem);
  return (success != 0) ? 0 : 1;
}


/* ------------------------------------------------------------------ */
/* ----- Functions to access matrix tables and get all elements ----- */

int getAllIntMatrixElements(MYSQL * sqlConn, rdbMatrix * matrixInfo,
		      	    int values[], char byteArray[])
{
  /* Execute the query to get element */
  if( !execGetAllMatrixElementsSQLCall(sqlConn, matrixInfo->tableName) )
    return 0;

  /* Get the query results */
  MYSQL_RES *sqlRes;
  MYSQL_ROW sqlRow;
  sqlRes = mysql_use_result(sqlConn);
  int index, row, col;

  while( (sqlRow = mysql_fetch_row(sqlRes)) != NULL )
  {
    if(sqlRow[2] != NULL)
    {
      col = atoi(sqlRow[0])-1;
      row = atoi(sqlRow[1])-1;
      index = row + col * matrixInfo->numRows;
      values[index] = atoi(sqlRow[2]);
      byteArray[index] = 1;
    }
  }

  mysql_free_result(sqlRes);

  return 1;
}


int getAllDoubleMatrixElements(MYSQL * sqlConn, rdbMatrix * matrixInfo,
		      	       double values[], char byteArray[])
{
  /* Execute the query to get element */
  if( !execGetAllMatrixElementsSQLCall(sqlConn, matrixInfo->tableName) )
    return 0;

  /* Get the query results */
  MYSQL_RES *sqlRes;
  MYSQL_ROW sqlRow;
  sqlRes = mysql_use_result(sqlConn);
  int index, row, col;

  while( (sqlRow = mysql_fetch_row(sqlRes)) != NULL )
  {
    if(sqlRow[2] != NULL)
    {
      col = atoi(sqlRow[0])-1;
      row = atoi(sqlRow[1])-1;
      index = row + col * matrixInfo->numRows;
      values[index] = strtod(sqlRow[2], NULL);
      byteArray[index] = 1;
    }
  }

  mysql_free_result(sqlRes);

  return 1;
}


int execGetAllMatrixElementsSQLCall(MYSQL * sqlConn, char * tableName)
{
  /* Build the sql string */
  int length = strlen(sqlTemplateGetAllMatrixValues) + strlen(tableName) + 1;
  char strGetElems[length];
  sprintf( strGetElems, sqlTemplateGetAllMatrixValues, tableName);

  /* Execute the query */
  int success = mysql_query(sqlConn, strGetElems);
  return (success != 0) ? 0 : 1;
}


