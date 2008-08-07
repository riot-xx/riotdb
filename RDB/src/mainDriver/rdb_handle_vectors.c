#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <mysql.h>
#include "rdb_basics.h"
#include "rdb_handle_vectors.h"
#include "rdb_handle_vector_tables.h"
#include "rdb_handle_vector_views.h"
#include "rdb_insert_vector_data.h"



/* -------------------- Delete an RDBVector ------------------------ */

int deleteRDBVector(MYSQL * sqlConn, rdbVector * vectorInfo)
{
  /* Decrement the reference counter. 
     If it reaches zero, then we can delete */
  if( !decrementRefCounter(sqlConn, vectorInfo) )
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


/* --------------- Functions to dublicate a vector table -------------- */
int duplicateVectorTable(MYSQL * sqlConn, rdbVector *  originalVector,
			 rdbVector *copyVector)
{
  /* Create the new table */
  copyRDBVector(&copyVector, originalVector, 0);
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


/* ------------- Functions for data type convertions ------------------ */

int convertNumericToComplex(MYSQL * sqlConn, rdbVector * numericVector, 
			    rdbVector * complexVector)
{
  return internalConvertVectors(sqlConn, numericVector, complexVector, 
				sqlTemplateNumericToComplex, SXP_TYPE_COMPLEX);
}


int convertDoubleToInteger(MYSQL * sqlConn, rdbVector * doubleVector, 
			   rdbVector * integerVector)
{
  return internalConvertVectors(sqlConn, doubleVector, integerVector, 
				 sqlTemplateDoubleToInteger, SXP_TYPE_INTEGER);
}


int convertIntegerToDouble(MYSQL * sqlConn, rdbVector * integerVector, 
			   rdbVector * doubleVector)
{
  return internalConvertVectors(sqlConn, integerVector, doubleVector, 
				sqlTemplateGenericConvert, SXP_TYPE_DOUBLE);
}


int convertLogicToInteger(MYSQL * sqlConn, rdbVector * logicVector, 
			  rdbVector * integerVector)
{
  return internalConvertVectors(sqlConn, logicVector, integerVector, 
				 sqlTemplateGenericConvert, SXP_TYPE_INTEGER);
}


int convertLogicToDouble(MYSQL * sqlConn, rdbVector * logicVector, 
			 rdbVector * doubleVector)
{
  return internalConvertVectors(sqlConn, logicVector, doubleVector, 
				sqlTemplateGenericConvert, SXP_TYPE_DOUBLE);
}


int internalConvertVectors(MYSQL * sqlConn, rdbVector * fromVector, 
			   rdbVector * toVector, char * sqlTemplate, int type)
{
  /* Build the sql string */
  int length = strlen(sqlTemplate) + strlen(fromVector->tableName) + 1;
  char * sqlString = (char*) malloc( length * sizeof(char*) );
  sprintf(sqlString, sqlTemplate, fromVector->tableName);

  initRDBVector(&toVector, 1, 0);
  toVector->size = fromVector->size;

  /* Create the view */
  int success = 0;
  switch( type )
  {
    case SXP_TYPE_INTEGER:
      success = createNewIntegerVectorView(sqlConn, toVector, sqlString);
      break;
    case SXP_TYPE_DOUBLE:
      success = createNewDoubleVectorView(sqlConn, toVector, sqlString);
      break;
    case SXP_TYPE_STRING:
      success = createNewStringVectorView(sqlConn, toVector, sqlString);
      break;
    case SXP_TYPE_COMPLEX:
      success = createNewComplexVectorView(sqlConn, toVector, sqlString);
      break;
    case SXP_TYPE_LOGIC:
      success = createNewLogicVectorView(sqlConn, toVector, sqlString);
      break;
  }

  if( success )
     createViewReferences(sqlConn, toVector, fromVector, fromVector);
  else
     toVector->size = 0;

  free(sqlString);
  return success;
}


/* ------------------------- Function to get size ------------------------- */
int getLogicalTableSize(MYSQL * sqlConn, rdbVector * vectorInfo, int * size)
{
  return internalGetTableSize(sqlConn,vectorInfo,size,sqlTemplateGetLogicalSize);
}


int getActualTableSize(MYSQL * sqlConn, rdbVector * vectorInfo, int * size)
{
  return internalGetTableSize(sqlConn,vectorInfo,size,sqlTemplateGetActualSize);
}


int internalGetTableSize(MYSQL * sqlConn, rdbVector * vectorInfo, int * size,
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


/* ----------------------- Function to check for NA ----------------------- */
int checkRDBVectorForNA(MYSQL * sqlConn, rdbVector * vectorInfo,
			rdbVector * resultVector)
{
  /* Create a new logic table and initialize every entry to true*/
  initRDBVector(&resultVector, 0, 0);
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
  if( !getLogicalTableSize(sqlConn, vectorInfo, &logicalSize) ||
      !getActualTableSize(sqlConn, vectorInfo, &actualSize) )
    return 0;

  *flag = (logicalSize == actualSize) ? 0 : 1;

  return 1;
}


/* ---------------  Functions to check on a vector update ----------------- */

int ensureMaterialization(MYSQL * sqlConn, rdbVector * vectorInfo)
{
  /* Build the sql string */
  int length = strlen(sqlTemplateGetReferredViews) + 2*MAX_INT_LENGTH + 1;
  char strGetRefViewsSQL[length];
  sprintf( strGetRefViewsSQL, sqlTemplateGetReferredViews,
	   vectorInfo->metadataID, vectorInfo->metadataID );

  /* Execute the query */
  int success = mysql_query(sqlConn, strGetRefViewsSQL);
  if( success != 0 )
     return 0;

  /* Get the referenced views' ids */
  MYSQL_RES *sqlRes;
  MYSQL_ROW sqlRow;
  sqlRes = mysql_store_result(sqlConn);
  unsigned long int numRes = (unsigned long int)mysql_num_rows(sqlRes);
  unsigned long int viewIDs[numRes];
  int i = 0 ;
  
  if( numRes != 0 )
  {
     /* Get the view ids from the SQl result*/
     while( (sqlRow = mysql_fetch_row(sqlRes)) != NULL )
     {
        viewIDs[i] = atoi(sqlRow[0]);
	i++;
     }
     mysql_free_result(sqlRes);
  }
  else
  {
     while( (sqlRow = mysql_fetch_row(sqlRes)) != NULL );
     mysql_free_result(sqlRes);
  }


  /* Materialize current vector if a view */
  success = 1;
  if( vectorInfo->isView )
     success *= materializeView(sqlConn, vectorInfo);

  /* Materialize all referenced views */
  for( i = 0 ; i < numRes ; i++ )
  {
     rdbVector *viewVector;
     int loadSuccess = loadRDBVector(sqlConn, &viewVector, viewIDs[i]);
      
     if( loadSuccess )
     {
        success = materializeView(sqlConn, viewVector);
	clearRDBVector(&viewVector);
     }
  }
  
  return success;
}


int materializeView(MYSQL * sqlConn, rdbVector * viewVector)
{
  if( !viewVector->isView )
     return 0;

  if( viewVector->sxp_type == SXP_TYPE_INTEGER )
     return materializeIntegerView(sqlConn, viewVector);

  if( viewVector->sxp_type == SXP_TYPE_DOUBLE )
     return materializeDoubleView(sqlConn, viewVector);

  if( viewVector->sxp_type == SXP_TYPE_COMPLEX )
     return materializeComplexView(sqlConn, viewVector);

  if( viewVector->sxp_type == SXP_TYPE_STRING )
     return materializeStringView(sqlConn, viewVector);

  if( viewVector->sxp_type == SXP_TYPE_LOGIC )
     return materializeLogicView(sqlConn, viewVector);

  return 0;
}


/* ------------ Functions to handle the reference counter ----------- */
int decrementRefCounter(MYSQL * sqlConn, rdbVector * vectorInfo)
{
  /* Get the counter */
  int refCounter = 0;
  if( !getRefCounter(sqlConn, vectorInfo, &refCounter) )
    return 0;

  /* Decrement the counter */
  refCounter = (refCounter <= 0)? 0 : (refCounter - 1);

  /* Set the new counter */
  vectorInfo->refCounter = refCounter;
  return setRefCounter(sqlConn, vectorInfo, refCounter);
}


int incrementRefCounter(MYSQL * sqlConn, rdbVector * vectorInfo)
{
  /* Get the counter */
  int refCounter = 0;
  if( !getRefCounter(sqlConn, vectorInfo, &refCounter) )
    return 0;

  /* Increment the counter */
  refCounter = (refCounter <= 0)? 1 : (refCounter + 1);

  /* Set the new counter */
  vectorInfo->refCounter = refCounter;
  return setRefCounter(sqlConn, vectorInfo, refCounter);
}


int getRefCounter(MYSQL * sqlConn, rdbVector * vectorInfo, int * refCounter)
{
  /* Build the sql string */
  int length = strlen(sqlTemplateGetRefCounter) + MAX_INT_LENGTH + 1;
  char strGetRecCounterSQL[length];
  sprintf( strGetRecCounterSQL, sqlTemplateGetRefCounter,
	   vectorInfo->metadataID );

  /* Execute the query */
  int success = mysql_query(sqlConn, strGetRecCounterSQL);
  if( success != 0 )
     return 0;

  /* Get the reference counter */
  MYSQL_RES *sqlRes;
  MYSQL_ROW sqlRow;
  sqlRes = mysql_use_result(sqlConn);
  sqlRow = mysql_fetch_row(sqlRes);
  if( sqlRow != NULL )
  {
     *refCounter = atoi(sqlRow[0]);
     success = 1;
  }

  /* C API requires to clean the result set */
  while( (sqlRow = mysql_fetch_row(sqlRes)) != NULL );
  mysql_free_result(sqlRes);

  return success;
}


int setRefCounter(MYSQL * sqlConn, rdbVector * vectorInfo, int refCounter)
{
  /* Store the new counter back into the metadata table */
  int length = strlen(sqlTemplateSetRefCounter) +2* MAX_INT_LENGTH + 1;
  char strSetRecCounterSQL[length];
  sprintf( strSetRecCounterSQL, sqlTemplateSetRefCounter,
	   refCounter, vectorInfo->metadataID );

  int success = mysql_query(sqlConn, strSetRecCounterSQL);

  return ( success != 0 )? 0 : 1;
}


/* ----- Functions to keep track of vector table naming scheme ----- */

int getNextVectorID(MYSQL * sqlConn, int * next)
{
  /* Execute the query */
  int success = mysql_query(sqlConn, sqlGetNextVectorID);
  if( success != 0 )
  {
     return 0;
  }

  /* Get the actual number */
  MYSQL_RES *sqlRes;
  MYSQL_ROW sqlRow;
  sqlRes = mysql_use_result(sqlConn);
  sqlRow = mysql_fetch_row(sqlRes);
  if( sqlRow != NULL )
  {
     *next = atoi(sqlRow[0]);
     success = 1;
  }

  /* C API requires to clean the result set */
  while( (sqlRow = mysql_fetch_row(sqlRes)) != NULL );
  mysql_free_result(sqlRes);

  return success;
}


int renameTable(MYSQL * sqlConn, char* oldName, char* newName)
{
  /* Create the sql string */
  int length = strlen(sqlTemplateRenameTable) + strlen(oldName) + 
               strlen(newName) + 1;
  char strRenameTableSQL[length];
  sprintf( strRenameTableSQL, sqlTemplateRenameTable, oldName, newName);

  int success = mysql_query(sqlConn, strRenameTableSQL);

  return ( success != 0 )? 0 : 1;
}


/* ----------------- Functions to handle Metadata ------------------ */

int insertMetadataInfo(MYSQL * sqlConn, rdbVector * vectorInfo)
{
  /* Build the sql string */
  int length = strlen(sqlTemplateInsertMetadata) + 
               strlen(vectorInfo->tableName) + 14*MAX_INT_LENGTH + 1;
  char strInsertMetadataInfo[length];
  sprintf( strInsertMetadataInfo, sqlTemplateInsertMetadata,
	   vectorInfo->tableName,
	   vectorInfo->dbSourceID, 
	   vectorInfo->size,
	   vectorInfo->isView, 
	   vectorInfo->refCounter,
	   vectorInfo->sxp_type,
	   vectorInfo->sxp_obj,
	   vectorInfo->sxp_named,
	   vectorInfo->sxp_gp,
	   vectorInfo->sxp_mark,
	   vectorInfo->sxp_debug,
	   vectorInfo->sxp_trace,
	   vectorInfo->sxp_spare,
	   vectorInfo->sxp_gcgen,
	   vectorInfo->sxp_gccls );


  /* Execute the query */
  int success = mysql_query(sqlConn, strInsertMetadataInfo);
  if( success != 0 )
     return 0;

  /* Get the metadata info index */
  vectorInfo->metadataID = (unsigned int)mysql_insert_id(sqlConn);

  return 1;
}


int deleteMetadataInfo(MYSQL * sqlConn, rdbVector * vectorInfo)
{
  /* Build the sql string */
  int length = strlen(sqlTemplateDeleteMetadata) + MAX_INT_LENGTH;
  char strDeleteMetadataInfo[length];
  sprintf( strDeleteMetadataInfo, sqlTemplateDeleteMetadata,
	   vectorInfo->metadataID);

  /* Execute the query */
  int success = mysql_query(sqlConn, strDeleteMetadataInfo);

  return (success != 0) ? 0 : 1;
}

int updateMetadataInfo(MYSQL * sqlConn, rdbVector * vectorInfo)
{
  /* Build the sql string */
  int length = strlen(sqlTemplateUpdateMetadata) + 
               strlen(vectorInfo->tableName) + 14*MAX_INT_LENGTH + 1;
  char strUpdateMetadataInfo[length];
  sprintf( strUpdateMetadataInfo, sqlTemplateUpdateMetadata,
	   vectorInfo->tableName,
	   vectorInfo->dbSourceID, 
	   vectorInfo->size,
	   vectorInfo->isView, 
	   vectorInfo->refCounter,
	   vectorInfo->sxp_type,
	   vectorInfo->sxp_obj,
	   vectorInfo->sxp_named,
	   vectorInfo->sxp_gp,
	   vectorInfo->sxp_mark,
	   vectorInfo->sxp_debug,
	   vectorInfo->sxp_trace,
	   vectorInfo->sxp_spare,
	   vectorInfo->sxp_gcgen,
	   vectorInfo->sxp_gccls,
	   vectorInfo->metadataID );

  /* Execute the query */
  int success = mysql_query(sqlConn, strUpdateMetadataInfo);
  return ( success != 0 )? 0 : 1;
}

