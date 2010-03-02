/*****************************************************************************
 * Contains functions for managing matrix tables (create, delete, dublicate)
 *
 * Author: Herodotos Herodotou
 * Date:   Sep 17, 2008
 ****************************************************************************/

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <mysql.h>
#include "basics.h"
#include "handle_matrix_tables.h"
#include "handle_matrix_views.h"
#include "handle_metadata.h"


/*---------------  Functions to create matrix tables ---------------- */

int createNewIntMatrixTable(MYSQL * sqlConn, rdbMatrix * matrixInfo)
{
  matrixInfo->sxp_type = SXP_TYPE_INTEGER;
  return internalCreateNewMatrixTable(sqlConn, matrixInfo, 
				      sqlTemplateCreateIntMatrix);
}


int createNewDoubleMatrixTable(MYSQL * sqlConn, rdbMatrix * matrixInfo)
{
  matrixInfo->sxp_type = SXP_TYPE_DOUBLE;
  return internalCreateNewMatrixTable(sqlConn, matrixInfo, 
				      sqlTemplateCreateDoubleMatrix);
}


int internalCreateNewMatrixTable(MYSQL * sqlConn, rdbMatrix * matrixInfo, 
				 char * sqlTemplate)
{
  /* Build the name of the new table */
  if( !buildUniqueMatrixTableName(sqlConn, &(matrixInfo->tableName)) )
    return 0;

  /* Build the sql string */
  int length = strlen(sqlTemplate) + strlen(matrixInfo->tableName) + 1;
  char strCreateMatrixSQL[length];
  sprintf( strCreateMatrixSQL, sqlTemplate, matrixInfo->tableName );

  /* Execute the query to create the table*/
  int success = mysql_query(sqlConn, strCreateMatrixSQL);
  if( success != 0 )
     return 0;

  /* Insert info into Metadata table */
  success = insertMatrixMetadataInfo(sqlConn, matrixInfo);

  return success;
}


/* ------------------ Functions for naming tables ------------------- */

int buildUniqueMatrixTableName(MYSQL * sqlConn, char ** newTableName)
{
  int next = 0;
  if( !getNextTableID(sqlConn, &next) )
    return 0;

  /* Build the name of the new table */
  if( *newTableName != NULL )
      free(*newTableName);
  int length = strlen(sqlTemplateMatrixTableName) + MAX_INT_LENGTH;
  *newTableName = (char*) malloc(length * sizeof(char));
  sprintf( *newTableName, sqlTemplateMatrixTableName, next);

  return 1;
}


/* ------------ Functions to delete/drop matrix tables -------------- */

int deleteRDBMatrix(MYSQL * sqlConn, rdbMatrix * matrixInfo)
{
  /* Decrement the reference counter. 
     If it reaches zero, then we can delete */
  if( !decrementRefCounter(sqlConn, &(matrixInfo->refCounter), 
			   matrixInfo->metadataID) )
    return 0;

  if( matrixInfo->refCounter <= 0 && !matrixInfo->isView )
  {
     /* Delete the table */
     return dropMatrixTable(sqlConn, matrixInfo);
  }
  else if( matrixInfo->refCounter <= 0 && matrixInfo->isView )
  {
     /* Delete the view */
     return dropMatrixView(sqlConn, matrixInfo);
  }

  return 1;
}

int dropMatrixTable(MYSQL * sqlConn, rdbMatrix * matrixInfo)
{
  /* Safe guard - only drop table if num references == 0 */
  int refCounter = 0;
  getMatrixViewRefCount(sqlConn, matrixInfo, &refCounter);
  if( refCounter != 0 )
  {
     matrixInfo->refCounter = refCounter;
     return setRefCounter(sqlConn, matrixInfo->metadataID, refCounter);
  }

  /* Build the sql string and drop the table */
  int length = strlen(sqlTemplateDropMatrixTable) + 
               strlen(matrixInfo->tableName) + 1;
  char strDropTableSQL[length];
  sprintf( strDropTableSQL, sqlTemplateDropMatrixTable, matrixInfo->tableName );

  int success = mysql_query(sqlConn, strDropTableSQL);
  if( success != 0 )
     return 0;

  /* Delete corresponding metadata info */
  success = deleteMetadataInfo(sqlConn, matrixInfo->metadataID);

  return success;
}


/* --------------- Functions to dublicate a matrix table -------------- */
int duplicateMatrixTable(MYSQL * sqlConn, rdbMatrix *  originalMatrix,
			 rdbMatrix *copyMatrix)
{
  /* Create the new table */
  copyRDBMatrix(copyMatrix, originalMatrix);
  copyMatrix->isView = 0;

  int success = 0;
  if( originalMatrix->sxp_type == SXP_TYPE_INTEGER )
     success = createNewIntMatrixTable(sqlConn, copyMatrix);
  else if( originalMatrix->sxp_type == SXP_TYPE_DOUBLE )
     success = createNewDoubleMatrixTable(sqlConn, copyMatrix);

  if( !success )
    return 0;

  /* Copy all the data to the new table */
  int length = strlen(sqlTemplateDublicateMatrixTable) + strlen(copyMatrix->tableName)+
               strlen(originalMatrix->tableName) + 1;
  char strDublicateTableSQL[length];
  sprintf( strDublicateTableSQL, sqlTemplateDublicateMatrixTable, 
	   copyMatrix->tableName, originalMatrix->tableName );

  success = mysql_query(sqlConn, strDublicateTableSQL);

  return( success != 0 )? 0 : 1;
}


/* -------------------- Function to handle table sizes ---------------------- */
int getLogicalMatrixSize(MYSQL * sqlConn, rdbMatrix * matrixInfo,
			 int * numRows, int * numCols)
{
  /* Build the sql string and execute the query */
  int length = strlen(sqlTemplateGetLogicalMatrixSize) + 
	       strlen(matrixInfo->tableName) + 1;
  char strGetSizeSQL[length];
  sprintf( strGetSizeSQL, sqlTemplateGetLogicalMatrixSize,
	   matrixInfo->tableName );
  int success = mysql_query(sqlConn, strGetSizeSQL);

  if( success != 0 )
     return 0;

  /* Get the value */
  success = 0;
  MYSQL_RES *sqlRes;
  MYSQL_ROW sqlRow;
  sqlRes = mysql_use_result(sqlConn);

  if( (sqlRow = mysql_fetch_row(sqlRes)) != NULL )
  {
     if(sqlRow[0] != NULL && sqlRow[1] != NULL)
     {
        *numRows = atoi(sqlRow[0]);
        *numCols = atoi(sqlRow[1]);
	success = 1;
     }
  }

  while( (sqlRow = mysql_fetch_row(sqlRes)) != NULL );
  mysql_free_result(sqlRes);

  return success;
}


int setLogicalMatrixSize(MYSQL * sqlConn, rdbMatrix * matrixInfo,
		 	int newNumRows, int newNumCols)

{
  /* Build the sql string */
  int length = strlen(sqlTemplateSetCurrentMatrixSize) + 2*MAX_INT_LENGTH + 1;
  char strUpdateSize[length];
  char strSize[2*MAX_INT_LENGTH + 2];
  sprintf( strSize, "%d,%d", newNumRows, newNumCols );
  sprintf( strUpdateSize, sqlTemplateSetCurrentMatrixSize, 
	   strSize, matrixInfo->metadataID);
 
  /* Execute the query */
  int success = mysql_query(sqlConn, strUpdateSize);

  /* Update the structure*/
  matrixInfo->numRows = newNumRows;
  matrixInfo->numCols = newNumCols;

  return (success != 0) ? 0 : 1;
}


int updateLogicalMatrixSize(MYSQL * sqlConn, rdbMatrix * matrixInfo)
{
   int newNumRows, newNumCols;
   int success = getLogicalMatrixSize(sqlConn, matrixInfo, 
				      &newNumRows, &newNumCols);

   if( success )
	success = setLogicalMatrixSize(sqlConn, matrixInfo, 
				       newNumRows, newNumCols);

   return success;
}




