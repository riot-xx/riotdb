#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <mysql.h>
#include "rdb_basics.h"
#include "rdb_handle_vector_tables.h"
#include "rdb_handle_vector_views.h"
#include "rdb_handle_vectors.h"


/* --------------------- Functions to create views -------------------- */
int createNewIntegerVectorView(MYSQL * sqlConn, rdbVector * viewVector,
			       char sqlString[])
{
  viewVector->sxp_type = SXP_TYPE_INTEGER;
  return internalCreateNewVectorView(sqlConn, viewVector,
				     sqlTemplateCreateView, sqlString);
}


int createNewDoubleVectorView(MYSQL * sqlConn, rdbVector * viewVector,
			      char sqlString[])
{
  viewVector->sxp_type = SXP_TYPE_DOUBLE;
  return internalCreateNewVectorView(sqlConn, viewVector, 
				     sqlTemplateCreateView, sqlString);
}


int createNewStringVectorView(MYSQL * sqlConn, rdbVector * viewVector,
			      char sqlString[])
{
  viewVector->sxp_type = SXP_TYPE_STRING;
  return internalCreateNewVectorView(sqlConn, viewVector, 
				     sqlTemplateCreateView, sqlString);
}


int createNewComplexVectorView(MYSQL * sqlConn, rdbVector * viewVector,
			       char sqlString[])
{
  viewVector->sxp_type = SXP_TYPE_COMPLEX;
  return internalCreateNewVectorView(sqlConn, viewVector,
				     sqlTemplateCreateComplexView, sqlString);
}


int createNewLogicVectorView(MYSQL * sqlConn, rdbVector * viewVector,
			     char sqlString[])
{
  viewVector->sxp_type = SXP_TYPE_LOGIC;
  return internalCreateNewVectorView(sqlConn, viewVector,
				     sqlTemplateCreateView, sqlString);
}


int internalCreateNewVectorView(MYSQL * sqlConn, rdbVector * vectorInfo, 
				char sqlTemplate[], char sqlString[])
{
  /* Build the name of the new view */
  if( !buildUniqueViewName(sqlConn, &(vectorInfo->tableName)) )
    return 0;

  /* Build the sql string */
  int length = strlen(sqlTemplate) + strlen(vectorInfo->tableName) + 
               strlen(sqlString) + 1;
  char strCreateViewSQL[length];
  sprintf( strCreateViewSQL, sqlTemplate, vectorInfo->tableName, sqlString );

  /* Execute the query */
  int success = mysql_query(sqlConn, strCreateViewSQL);
  if( success != 0 )
     return 0;

  /* Insert info into Metadata table */
  success = insertMetadataInfo(sqlConn, vectorInfo);
 
  return success;
}


/* -------------- Functions to create view references -------------- */

int createViewReferences(MYSQL * sqlConn, rdbVector * viewVector,
			 rdbVector * leftInput, rdbVector * rightInput)
{
  /* Build the sql string and create references */
  int length = strlen(sqlTemplateCreateViewReferences) + 3*MAX_INT_LENGTH + 1;
  char strCreateRefsSQL[length];
  sprintf(strCreateRefsSQL, sqlTemplateCreateViewReferences,
	  viewVector->metadataID, leftInput->metadataID, rightInput->metadataID);

  int success = mysql_query(sqlConn, strCreateRefsSQL);
  if( success != 0 )
     return 0;

  /* Increment the references */
  leftInput->refCounter++;
  rightInput->refCounter++;

  return success;
}


int getViewReferences(MYSQL * sqlConn, rdbVector * viewVector,
		      rdbVector ** leftInput, rdbVector ** rightInput)
{
  /* Build the sql string and execute the query */
  int length = strlen(sqlTemplateGetViewReferences) + MAX_INT_LENGTH + 1;
  char strGetRefsSQL[length];
  sprintf(strGetRefsSQL, sqlTemplateGetViewReferences, viewVector->metadataID);

  int success = mysql_query(sqlConn, strGetRefsSQL);
  if( success != 0 )
     return 0;

  /* Get the references */
  unsigned long int leftID, rightID;
  MYSQL_RES *sqlRes;
  MYSQL_ROW sqlRow;
  sqlRes = mysql_use_result(sqlConn);
  sqlRow = mysql_fetch_row(sqlRes);
  if( sqlRow != NULL )
  {
     leftID = atoi(sqlRow[0]);
     rightID = atoi(sqlRow[1]);
     success = 1;
  }

  /* C API requires to clean the result set */
  while( (sqlRow = mysql_fetch_row(sqlRes)) != NULL );
  mysql_free_result(sqlRes);

  if( success != 1 )
    return 0;

  /* Load the rdbVectors from the Metadata table */
  success *= loadRDBVector(sqlConn, leftInput, leftID);
  success *= loadRDBVector(sqlConn, rightInput, rightID);

  return success;
}


int getViewRefCount(MYSQL * sqlConn, rdbVector * vectorInfo, int * count)
{
  /* Build the sql string and execute the query */
  int length = strlen(sqlTemplateGetViewRefCount) + 2*MAX_INT_LENGTH + 1;
  char strGetRefsSQL[length];
  sprintf(strGetRefsSQL, sqlTemplateGetViewRefCount, 
	  vectorInfo->metadataID, vectorInfo->metadataID);

  int success = mysql_query(sqlConn, strGetRefsSQL);
  if( success != 0 )
     return 0;

  /* Get how many times this vector is referenced in views */
  MYSQL_RES *sqlRes;
  MYSQL_ROW sqlRow;
  sqlRes = mysql_use_result(sqlConn);
  sqlRow = mysql_fetch_row(sqlRes);
  if( sqlRow != NULL )
  {
     *count = atoi(sqlRow[0]);
     success = 1;
  }

  /* C API requires to clean the result set */
  while( (sqlRow = mysql_fetch_row(sqlRes)) != NULL );
  mysql_free_result(sqlRes);

  return success;
}


int updateViewReferences(MYSQL * sqlConn, rdbVector * viewVector, 
			 rdbVector * newVector)
{
  /* Build the sql strings */
  int length = strlen(sqlTemplateUpdateViewReferences1) + 2*MAX_INT_LENGTH + 1;
  char strUpdateRefs1SQL[length];
  sprintf(strUpdateRefs1SQL, sqlTemplateUpdateViewReferences1, 
	  newVector->metadataID, viewVector->metadataID);

  length = strlen(sqlTemplateUpdateViewReferences2) + 2*MAX_INT_LENGTH + 1;
  char strUpdateRefs2SQL[length];
  sprintf(strUpdateRefs2SQL, sqlTemplateUpdateViewReferences2,
	  newVector->metadataID, viewVector->metadataID);

  /* Execute the queries */
  int success1 = mysql_query(sqlConn, strUpdateRefs1SQL);
  int success2 = mysql_query(sqlConn, strUpdateRefs2SQL);

  return ( success1 == 0 && success2 == 0 )? 1 : 0;
}


int removeViewReferences(MYSQL * sqlConn, rdbVector * viewVector)
{
  /* Get the references */
  rdbVector *leftInput, *rightInput;
  int success = getViewReferences(sqlConn, viewVector, 
				  &leftInput, &rightInput);
  if( success == 0 )
    return 0;

  /* Build the sql string and remove references from table */
  int length = strlen(sqlTemplateRemoveViewReferences) + MAX_INT_LENGTH + 1;
  char strRemoveRefsSQL[length];
  sprintf( strRemoveRefsSQL, sqlTemplateRemoveViewReferences,
	   viewVector->metadataID);

  success = mysql_query(sqlConn, strRemoveRefsSQL);
  if( success != 0 )
    return 0;
  
  /* Reccursively delete the references */
  success = deleteRDBVector(sqlConn, leftInput);

  if( leftInput->metadataID != rightInput->metadataID )
    success *= deleteRDBVector(sqlConn, rightInput);

  /* Clean up */
  clearRDBVector(&leftInput);
  clearRDBVector(&rightInput);

  return success;
}


/* ---------------- Functions to delete vector views ---------------- */

int dropVectorView(MYSQL * sqlConn, rdbVector * vectorInfo)
{
  /* Safe guard - only drop view if num references == 0 */
  int refCounter = 0;
  getViewRefCount(sqlConn, vectorInfo, &refCounter);
  if( refCounter != 0 )
  {
     vectorInfo->refCounter = refCounter;
     return setRefCounter(sqlConn, vectorInfo, refCounter);
  }

  /* Build the sql string and drop the view */
  int length = strlen(sqlTemplateDropView) + 
               strlen(vectorInfo->tableName) + 1;
  char strDropViewSQL[length];
  sprintf( strDropViewSQL, sqlTemplateDropView, vectorInfo->tableName );

  int success = mysql_query(sqlConn, strDropViewSQL);
  if( success != 0 )
     return 0;

  /* Delete corresponding metadata info */
  success = deleteMetadataInfo(sqlConn, vectorInfo);
  success *= removeViewReferences(sqlConn, vectorInfo);

  return success;
}


/* ------------------ Functions for naming views ------------------- */

int buildUniqueViewName(MYSQL * sqlConn, char ** newViewName)
{
  int next = 0;
  if( !getNextVectorID(sqlConn, &next) )
    return 0;

  /* Build the name of the new view */
  if( *newViewName != NULL )
     free(*newViewName);
  int length = strlen(sqlTemplateViewName) + MAX_INT_LENGTH;
  *newViewName = (char*) malloc(length * sizeof(char));
  sprintf( *newViewName, sqlTemplateViewName, next);

  return 1;
}


/* ----------------- Functions to materialize views ----------------- */

int materializeIntegerView(MYSQL * sqlConn, rdbVector * viewVector)
{
  /* Create the new table */
  rdbVector * newVector;
  initRDBVector(&newVector, 0, 1);
  if( !createNewIntVectorTable(sqlConn, newVector) ) 
     return 0;

  int success = internalMaterializeView(sqlConn, viewVector, newVector);

  clearRDBVector(&newVector);

  return success;
}


int materializeDoubleView(MYSQL * sqlConn, rdbVector * viewVector)
{
  /* Create the new table */
  rdbVector * newVector;
  initRDBVector(&newVector, 0, 1);
  newVector->isView = 0;
  if( !createNewDoubleVectorTable(sqlConn, newVector) ) 
     return 0;

  int success = internalMaterializeView(sqlConn, viewVector, newVector);

  clearRDBVector(&newVector);

  return success;
}


int materializeStringView(MYSQL * sqlConn, rdbVector * viewVector)
{
  /* Create the new table */
  rdbVector * newVector;
  initRDBVector(&newVector, 0, 1);
  newVector->isView = 0;
  if( !createNewStringVectorTable(sqlConn, newVector) ) 
     return 0;

  int success = internalMaterializeView(sqlConn, viewVector, newVector);

  clearRDBVector(&newVector);

  return success;
}


int materializeComplexView(MYSQL * sqlConn, rdbVector * viewVector)
{
  /* Create the new table */
  rdbVector * newVector;
  initRDBVector(&newVector, 0, 1);
  newVector->isView = 0;
  if( !createNewComplexVectorTable(sqlConn, newVector) ) 
     return 0;

  int success = internalMaterializeView(sqlConn, viewVector, newVector);

  clearRDBVector(&newVector);

  return success;
}


int materializeLogicView(MYSQL * sqlConn, rdbVector * viewVector)
{
  /* Create the new table */
  rdbVector * newVector;
  initRDBVector(&newVector, 0, 1);
  if( !createNewLogicVectorTable(sqlConn, newVector) ) 
     return 0;

  int success = internalMaterializeView(sqlConn, viewVector, newVector);

  clearRDBVector(&newVector);

  return success;
}


int internalMaterializeView(MYSQL * sqlConn, rdbVector * viewVector, 
			    rdbVector * newVector)
{
  /* Build the sql string and add the data from the view */
  int length = strlen(sqlTemplateMaterializeView) + strlen(newVector->tableName) +
               strlen(viewVector->tableName) + 1;
  char strMaterializeViewSQL[length];
  sprintf( strMaterializeViewSQL, sqlTemplateMaterializeView, 
	   newVector->tableName, viewVector->tableName );

  int success = mysql_query(sqlConn, strMaterializeViewSQL);

  if( success != 0 )
     return 0;

  /* Remove the current view */
  length = strlen(sqlTemplateDropView) + strlen(viewVector->tableName) + 1;
  char strDropViewSQL[length];
  sprintf( strDropViewSQL, sqlTemplateDropView, viewVector->tableName );

  success = mysql_query(sqlConn, strDropViewSQL);
  if( success != 0 )
     return 0;

  /* Delete corresponding references */
  success = removeViewReferences(sqlConn, viewVector);

  /* Rename new vector table to the view's name*/
  success *= renameTable(sqlConn, newVector->tableName, viewVector->tableName);

  /* Update the metadata info of the view table */
  viewVector->isView = 0;
  success *= updateMetadataInfo(sqlConn, viewVector);

  /* Delete Metadata info of new table */
  success *= deleteMetadataInfo(sqlConn, newVector);

  return success;
}

