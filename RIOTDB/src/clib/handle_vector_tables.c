/*****************************************************************************
 * Contains functions for managing vector tables (create, delete, dublicate,
 * check for N/A).
 *
 * Author: Herodotos Herodotou
 * Date:   Sep 17, 2008
 ****************************************************************************/

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <mysql.h>
#include "basics.h"
#include "handle_vector_tables.h"
#include "handle_matrix_tables.h"
#include "handle_vector_views.h"
#include "handle_matrix_views.h"
#include "handle_metadata.h"
#include "insert_vector_data.h"


/*---------------  Functions to create vector tables ---------------- */

int createNewIntVectorTable(MYSQL * sqlConn, rdbVector * vectorInfo)
{
  vectorInfo->sxp_type = SXP_TYPE_INTEGER;
  return internalCreateNewVectorTable(sqlConn, vectorInfo,
				      sqlTemplateCreateIntVector);
}


int createNewDoubleVectorTable(MYSQL * sqlConn, rdbVector * vectorInfo)
{
  vectorInfo->sxp_type = SXP_TYPE_DOUBLE;
  return internalCreateNewVectorTable(sqlConn, vectorInfo,
				      sqlTemplateCreateDoubleVector);
}


int createNewComplexVectorTable(MYSQL * sqlConn, rdbVector * vectorInfo)
{
  vectorInfo->sxp_type = SXP_TYPE_COMPLEX;
  return internalCreateNewVectorTable(sqlConn, vectorInfo,
				      sqlTemplateCreateComplexVector);
}


int createNewStringVectorTable(MYSQL * sqlConn, rdbVector * vectorInfo)
{
  vectorInfo->sxp_type = SXP_TYPE_STRING;
  return internalCreateNewVectorTable(sqlConn, vectorInfo,
				      sqlTemplateCreateStringVector);
}


int createNewLogicVectorTable(MYSQL * sqlConn, rdbVector * vectorInfo)
{
  vectorInfo->sxp_type = SXP_TYPE_LOGIC;
  return internalCreateNewVectorTable(sqlConn, vectorInfo,
				      sqlTemplateCreateLogicVector);
}


int internalCreateNewVectorTable(MYSQL * sqlConn, rdbVector * vectorInfo,
				 char * sqlTemplate)
{
  /* Build the name of the new table */
  if( !buildUniqueVectorTableName(sqlConn, &(vectorInfo->tableName)) )
    return 0;

  /* Build the sql string */
  int length = strlen(sqlTemplate) + strlen(vectorInfo->tableName) + 1;
  char strCreateVectorSQL[length];
  sprintf( strCreateVectorSQL, sqlTemplate, vectorInfo->tableName );

  /* Execute the query to create the table*/
  int success = mysql_query(sqlConn, strCreateVectorSQL);
  if( success != 0 )
     return 0;

  /* Insert info into Metadata table */
  success = insertVectorMetadataInfo(sqlConn, vectorInfo);

  return success;
}


/* ------------------ Functions for naming tables ------------------- */

int buildUniqueVectorTableName(MYSQL * sqlConn, char ** newTableName)
{
  int next = 0;
  if( !getNextTableID(sqlConn, &next) )
    return 0;

  /* Build the name of the new table */
   if( *newTableName != NULL )
      free(*newTableName);
   int length = strlen(sqlTemplateVectorTableName) + MAX_INT_LENGTH;
  *newTableName = (char*) malloc(length * sizeof(char));
  sprintf( *newTableName, sqlTemplateVectorTableName, next);

  return 1;
}


/* ------------ Functions to delete/drop vector tables -------------- */

int deleteRDBObject(MYSQL * sqlConn, rdbObject *objectInfo)
{
  int *refCounter = objectInfo->isVector ?
    &(objectInfo->payload.vector.refCounter) :
    &(objectInfo->payload.matrix.refCounter);
  unsigned long int metadataID = getMetadataIDInRDBObject(objectInfo);
  int isView =  objectInfo->isVector ?
    objectInfo->payload.vector.isView :
    objectInfo->payload.matrix.isView;
  /* Decrement the reference counter.
     If it reaches zero, then we can delete */
  if( !decrementRefCounter(sqlConn, refCounter, metadataID) )
    return 0;

  if( *refCounter <= 0 && !isView )
  {
     /* Delete the table */
     if (objectInfo->isVector)
       return dropVectorTable(sqlConn, &(objectInfo->payload.vector));
     else
       return dropMatrixTable(sqlConn, &(objectInfo->payload.matrix));
  }
  else if( *refCounter <= 0 && isView )
  {
     /* Delete the view */
     if (objectInfo->isVector)
       return dropVectorView(sqlConn, &(objectInfo->payload.vector));
     else
       return dropMatrixView(sqlConn, &(objectInfo->payload.matrix));
  }

  return 1;
}

int deleteRDBVector(MYSQL * sqlConn, rdbVector * vectorInfo)
{
  /* Decrement the reference counter.
     If it reaches zero, then we can delete */
  if( !decrementRefCounter(sqlConn, &(vectorInfo->refCounter),
			   vectorInfo->metadataID) )
    return 0;

  if( vectorInfo->refCounter <= 0 && !vectorInfo->isView )
  {
     /* Delete the table */
     return dropVectorTable(sqlConn, vectorInfo);
  }
  else if( vectorInfo->refCounter <= 0 && vectorInfo->isView )
  {
     /* Delete the view */
     return dropVectorView(sqlConn, vectorInfo);
  }

  return 1;
}

int dropVectorTable(MYSQL * sqlConn, rdbVector * vectorInfo)
{
  /* Safe guard - only drop table if num references == 0 */
  int refCounter = 0;
  getVectorViewRefCount(sqlConn, vectorInfo, &refCounter);
  if( refCounter != 0 )
  {
     vectorInfo->refCounter = refCounter;
     return setRefCounter(sqlConn, vectorInfo->metadataID, refCounter);
  }

  /* Build the sql string and drop the table */
  int length = strlen(sqlTemplateDropVectorTable) +
               strlen(vectorInfo->tableName) + 1;
  char strDropTableSQL[length];
  sprintf( strDropTableSQL, sqlTemplateDropVectorTable, vectorInfo->tableName );

  int success = mysql_query(sqlConn, strDropTableSQL);
  if( success != 0 )
     return 0;

  /* Delete corresponding metadata info */
  success = deleteMetadataInfo(sqlConn, vectorInfo->metadataID);

  return success;
}


/* --------------- Functions to dublicate a vector table -------------- */
int duplicateVectorTable(MYSQL * sqlConn, rdbVector *  originalVector,
			 rdbVector *copyVector)
{
  /* Create the new table */
  copyRDBVector(copyVector, originalVector);
  copyVector->isView = 0;

  int success = 0;
  if( originalVector->sxp_type == SXP_TYPE_INTEGER )
     success = createNewIntVectorTable(sqlConn, copyVector);
  else if( originalVector->sxp_type == SXP_TYPE_DOUBLE )
     success = createNewDoubleVectorTable(sqlConn, copyVector);
  else if( originalVector->sxp_type == SXP_TYPE_COMPLEX )
     success = createNewComplexVectorTable(sqlConn, copyVector);
  else if( originalVector->sxp_type == SXP_TYPE_STRING )
     success = createNewStringVectorTable(sqlConn, copyVector);
  else if( originalVector->sxp_type == SXP_TYPE_LOGIC )
     success = createNewLogicVectorTable(sqlConn, copyVector);

  if( !success )
    return 0;

  /* Copy all the data to the new table */
  int length = strlen(sqlTemplateDublicateTable) + strlen(copyVector->tableName)+
               strlen(originalVector->tableName) + 1;
  char strDublicateTableSQL[length];
  sprintf( strDublicateTableSQL, sqlTemplateDublicateTable,
	   copyVector->tableName, originalVector->tableName );

  success = mysql_query(sqlConn, strDublicateTableSQL);

  return( success != 0 )? 0 : 1;
}



/* ----------------------- Function to check for NA ----------------------- */
int checkRDBVectorForNA(MYSQL * sqlConn, rdbVector * vectorInfo,
			rdbVector * resultVector)
{
  /* Create a new logic table and initialize every entry to true*/
  if( !createNewLogicVectorTable(sqlConn, resultVector) ||
      !insertSeqLogicVectorTable(sqlConn, resultVector, '1', vectorInfo->size) )
    return 0;

  /* Update the non NA entries to False */
  int length = strlen(sqlTemplateCheckForNA) + strlen(vectorInfo->tableName) +
               strlen(resultVector->tableName) + 1;
  char strUpdateSQL[length];
  sprintf( strUpdateSQL, sqlTemplateCheckForNA, vectorInfo->tableName,
	   resultVector->tableName );

  int success = mysql_query(sqlConn, strUpdateSQL);
  return ( success != 0 ) ? 0 : 1;
}

int hasRDBVectorAnyNA(MYSQL * sqlConn, rdbVector * vectorInfo, int * flag)
{
  int logicalSize = 0;
  int actualSize = 0;
  if( !getLogicalVectorSize(sqlConn, vectorInfo, &logicalSize) ||
      !getActualVectorSize(sqlConn, vectorInfo, &actualSize) )
    return 0;

  *flag = (logicalSize == actualSize) ? 0 : 1;

  return 1;
}



/* -------------------- Function to handle table sizes ---------------------- */
int getLogicalVectorSize(MYSQL * sqlConn, rdbVector * vectorInfo, int * size)
{
  return internalGetVectorSize(sqlConn, vectorInfo, size,
			       sqlTemplateGetVectorLogicalSize);
}


int getActualVectorSize(MYSQL * sqlConn, rdbVector * vectorInfo, int * size)
{
  return internalGetVectorSize(sqlConn, vectorInfo, size,
			      sqlTemplateGetVectorActualSize);
}


int internalGetVectorSize(MYSQL * sqlConn, rdbVector * vectorInfo, int * size,
			  char* sqlTemplate)
{
  /* Build the sql string and execute the query */
  int length = strlen(sqlTemplate) + strlen(vectorInfo->tableName) + 1;
  char strGetSizeSQL[length];
  sprintf( strGetSizeSQL, sqlTemplate, vectorInfo->tableName );
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
     if(sqlRow[0] != NULL)
     {
        *size = atoi(sqlRow[0]);
	success = 1;
     }
  }

  while( (sqlRow = mysql_fetch_row(sqlRes)) != NULL );
  mysql_free_result(sqlRes);

  return success;
}


int setLogicalVectorSize(MYSQL * sqlConn, rdbVector * vectorInfo, int newSize)
{
  /* Build the sql string */
  int length = strlen(sqlTemplateSetVectorCurrentSize) + 2*MAX_INT_LENGTH + 1;
  char strUpdateSize[length];
  sprintf( strUpdateSize, sqlTemplateSetVectorCurrentSize,
	   newSize, vectorInfo->metadataID);

  /* Execute the query */
  int success = mysql_query(sqlConn, strUpdateSize);

  /* Update the structure*/
  vectorInfo->size = newSize;

  return (success != 0) ? 0 : 1;
}


