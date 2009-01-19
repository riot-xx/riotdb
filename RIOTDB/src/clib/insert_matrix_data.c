/*****************************************************************************
 * Contains functions for inserting data to a matrix either explicitely 
 * or load from a file
 *
 * Author: Herodotos Herodotou
 * Date:   Sep 22, 2008
 ****************************************************************************/
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <mysql.h>
#include "basics.h"
#include "insert_matrix_data.h"
#include "handle_metadata.h"
#include "handle_matrix_tables.h"


/* ---------- Functions to load data from a local file ------------ */

int loadIntoMatrixTable(MYSQL * sqlConn, rdbMatrix * matrixInfo,
			char * filename)
{
  /* Build the sql string */
  int length = strlen(sqlTemplateLoadIntoMatrix) + 
	       strlen(matrixInfo->tableName) + 
               strlen(filename) + 1;
  char strLoadMatrix[length];
  sprintf( strLoadMatrix, sqlTemplateLoadIntoMatrix, filename, 
	   matrixInfo->tableName );

  /* Execute the query */
  int success = mysql_query(sqlConn, strLoadMatrix);
  if( success == 0 )
  {
     success = 1;
     int numElems =  mysql_affected_rows(sqlConn);
     if( numElems >= 0 )
        success = updateLogicalMatrixSize(sqlConn, matrixInfo);

     return success;
  }
  else
    return 0;
}


/* ------- Functions to insert values into matrix tables ----------- */

int insertIntoIntMatrixTable(MYSQL * sqlConn, rdbMatrix * matrixInfo, 
			     int * values, int size, int nRows, int nCols )
{
  /* Some error checking */
  if( size < 1 || nRows < 1 || nCols < 1 )
      return 0;

  /* Perform the insertions in parts */
  int remain = nRows * nCols;
  int index, startRow = 1, startCol = 1;
  int success = 1;
  while( remain > MAX_NUM_INT_STRING )
  {
    success *= insertPartIntMatrixTable(sqlConn, matrixInfo, values, size, 
					MAX_NUM_INT_STRING, startRow, 
					startCol, nRows, nCols);

    remain -= MAX_NUM_INT_STRING;
    index = (startRow-1) + (startCol-1)*nRows + MAX_NUM_INT_STRING;
    startCol = 1 + index / nRows;
    startRow = 1 + index % nRows;

/*
    index = (startCol-1) + (startRow-1)*nCols + MAX_NUM_INT_STRING;
    startRow = 1 + index / nCols;
    startCol = 1 + index % nCols;
*/
  }

  success *= insertPartIntMatrixTable(sqlConn, matrixInfo, values, size, 
				remain, startRow, startCol, nRows, nCols);

  /* Update the size */
  success *= updateLogicalMatrixSize(sqlConn, matrixInfo);

  return success;
}


int insertIntoDoubleMatrixTable(MYSQL * sqlConn, rdbMatrix * matrixInfo, 
			     double * values, int size, int nRows, int nCols )
{
  /* Some error checking */
  if( size < 1 || nRows < 1 || nCols < 1 )
      return 0;

  /* Perform the insertions in parts */
  int remain = nRows * nCols;
  int index, startRow = 1, startCol = 1;
  int success = 1;
  while( remain > MAX_NUM_DOUBLE_STRING )
  {
    success *= insertPartDoubleMatrixTable(sqlConn, matrixInfo, values, size, 
					MAX_NUM_DOUBLE_STRING, startRow, 
					startCol, nRows, nCols);

    remain -= MAX_NUM_DOUBLE_STRING;
    index = (startRow-1) + (startCol-1)*nRows + MAX_NUM_DOUBLE_STRING;
    startCol = 1 + index / nRows;
    startRow = 1 + index % nRows;
/*
    index = (startCol-1) + (startRow-1)*nCols + MAX_NUM_DOUBLE_STRING;
    startRow = 1 + index / nCols;
    startCol = 1 + index % nCols;
*/
  }

  success *= insertPartDoubleMatrixTable(sqlConn, matrixInfo, values, size, 
				 remain, startRow, startCol, nRows, nCols);

  /* Update the size */
  success *= updateLogicalMatrixSize(sqlConn, matrixInfo);

  return success;
}


/* ------- Functions to insert sequences into matrix tables ------- */
int insertSeqIntMatrixTable(MYSQL * sqlConn, rdbMatrix * matrixInfo, 
			    int start, int end, int step,
			    int nRows, int nCols)
{
  if( step == 0 || (start < end && step < 0) || (start > end && step > 0) )
  {
    return 0;
  }

  /* Initialize necessary strings */
  int stringSize = MAX_NUM_INT_STRING * (3*MAX_INT_LENGTH + 8) + 1;
  char * strValues = (char *)malloc( stringSize * sizeof(char) );
  char strTemp[3*MAX_INT_LENGTH + 8];
  strValues[0] = '\0';

  /* Build string with the values of the form "(r,c,n),...,(r,c,n)" */
  int row, col, iter = 0;
  int element = start;
  int success = 1;
  for( col = 1 ; col <= nCols ; col++ )
  {
     for( row = 1 ; row <= nRows ; row++ )
     {
        sprintf(strTemp, sqlTemplateInsertIntMatrixValue, 
		col, row, element);
        strcat(strValues, strTemp);

	++iter;
	element += step;
	if( (step > 0) ? element > end : element < end )
	   element = start;

	if( iter == MAX_NUM_INT_STRING )
	{
	  /* Execute the sql query */
	  strValues[strlen(strValues) - 1] = '\0';
	  success *= internalInsertIntoMatrixTable(sqlConn, 
			sqlTemplateInsertIntoMatrix, 
			matrixInfo->tableName, strValues);
	  iter = 0;
	  strValues[0] = '\0';
	}
     }
  }

  /* Execute the sql query */
  if( iter > 0 )
  {
     strValues[strlen(strValues) - 1] = '\0';
     success *= internalInsertIntoMatrixTable(sqlConn, 
	sqlTemplateInsertIntoMatrix, matrixInfo->tableName, strValues);
  }

  /* Update the size */
  success *= updateLogicalMatrixSize(sqlConn, matrixInfo);

  free(strValues);

  return success;
}

int insertSeqDoubleMatrixTable(MYSQL * sqlConn, rdbMatrix * matrixInfo, 
			       double start, double end, double step,
			       int nRows, int nCols)
{
  if( step == 0 || (start < end && step < 0) || (start > end && step > 0) )
  {
    return 0;
  }

  /* Initialize necessary strings */
  int stringSize = MAX_NUM_DOUBLE_STRING * (2 * MAX_INT_LENGTH 
					    + MAX_DOUBLE_LENGTH + 8) + 1;
  char * strValues = (char *)malloc( stringSize * sizeof(char) );
  char strTemp[2*MAX_INT_LENGTH + MAX_DOUBLE_LENGTH + 8];
  strValues[0] = '\0';

  /* Build string with the values of the form "(r,c,n),...,(r,c,n)" */
  int row, col, iter = 0;
  double element = start;
  int success = 1;
  for( col = 1 ; col <= nCols ; col++ )
  {
     for( row = 1 ; row <= nRows ; row++ )
     {
        sprintf(strTemp, sqlTemplateInsertDoubleMatrixValue, 
		col, row, element);
        strcat(strValues, strTemp);

	++iter;
	element += step;
	if( (step > 0) ? element > end : element < end )
	   element = start;

	if( iter == MAX_NUM_DOUBLE_STRING )
	{
	  /* Execute the sql query */
	  strValues[strlen(strValues) - 1] = '\0';
	  success *= internalInsertIntoMatrixTable(sqlConn, 
			sqlTemplateInsertIntoMatrix, 
			matrixInfo->tableName, strValues);
	  iter = 0;
	  strValues[0] = '\0';
	}
     }
  }

  /* Execute the sql query */
  if( iter > 0 )
  {
     strValues[strlen(strValues) - 1] = '\0';
     success *= internalInsertIntoMatrixTable(sqlConn, 
	sqlTemplateInsertIntoMatrix, matrixInfo->tableName, strValues);
  }

  /* Update the size */
  success *= updateLogicalMatrixSize(sqlConn, matrixInfo);

  free(strValues);

  return success;
}


/* ----------------------------------------------------------------- */
/* ----- Helper Functions to insert values into matrix tables ------ */
int insertPartIntMatrixTable(MYSQL * sqlConn, rdbMatrix * matrixInfo, 
			     int * values, int size, int numElems,
			     int startRow, int startCol, int nRows, int nCols)
{
  /* Initialize necessary strings */
  int stringSize = numElems * (3*MAX_INT_LENGTH + 8) + 1;
  char * strValues = (char *)malloc( stringSize * sizeof(char) );
  char strTemp[3*MAX_INT_LENGTH + 8];
  strValues[0] = '\0';

  /* Build string with the values of the form "(r,c,n),...,(r,c,n)" */
  int row, col, elem, iter = 0;
  for( col = startCol ; col <= nCols && iter < numElems ; col++ )
  {
     for( row = startRow ; row <= nRows && iter < numElems ; row++ )
     {
        elem = ((row-1) + (col-1)*nRows) % size;
        sprintf(strTemp, sqlTemplateInsertIntMatrixValue, 
		col, row, values[elem]);
        strcat(strValues, strTemp);
	++iter;
     }
     startRow = 1;
  }

  /* Execute the sql query */
  strValues[strlen(strValues) - 1] = '\0';
  int success = internalInsertIntoMatrixTable(sqlConn, 
	sqlTemplateInsertIntoMatrix, matrixInfo->tableName, strValues);

  free(strValues);

  return success;
}

int insertPartDoubleMatrixTable(MYSQL * sqlConn, rdbMatrix * matrixInfo, 
			     double * values, int size, int numElems,
			     int startRow, int startCol, int nRows, int nCols)
{
  /* Initialize necessary strings */
  int stringSize = numElems * (2*MAX_INT_LENGTH + MAX_DOUBLE_LENGTH + 8) + 1;
  char * strValues = (char *)malloc( stringSize * sizeof(char) );
  char strTemp[2*MAX_INT_LENGTH + MAX_DOUBLE_LENGTH + 8];
  strValues[0] = '\0';

  /* Build string with the values of the form "(r,c,n),...,(r,c,n)" */
  int row, col, elem, iter = 0;
  for( col = startCol ; col <= nCols && iter < numElems ; col++ )
  {
     for( row = startRow ; row <= nRows && iter < numElems ; row++ )
     {
        elem = ((row-1) + (col-1)*nRows) % size;
        sprintf(strTemp, sqlTemplateInsertDoubleMatrixValue, 
		col, row, values[elem]);
        strcat(strValues, strTemp);
	++iter;
     }
     startRow = 1;
  }

  /* Execute the sql query */
  strValues[strlen(strValues) - 1] = '\0';
  int success = internalInsertIntoMatrixTable(sqlConn, 
	sqlTemplateInsertIntoMatrix, matrixInfo->tableName, strValues);

  free(strValues);

  return success;
}


int internalInsertIntoMatrixTable(MYSQL * sqlConn, char * sqlTemplate, 
			  	  char * tableName, char * strValues)
{
  /* Build the sql string */
  int length = strlen(sqlTemplate) + strlen(tableName) + 
	       strlen(strValues) + 1;
  char strInsertValues[length];
  sprintf( strInsertValues, sqlTemplate, tableName, strValues);

  /* Execute the query */
  int success = mysql_query(sqlConn, strInsertValues);

  return (success != 0) ? 0 : 1;
}


