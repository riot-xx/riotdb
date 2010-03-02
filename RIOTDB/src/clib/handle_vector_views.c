/*****************************************************************************
 * Contains functions for handling vector views (i.e create, drop, 
 * materialize views and keep track of view references)
 *
 * Author: Herodotos Herodotou
 * Date:   Sep 17, 2008
 ****************************************************************************/

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <mysql.h>
#include "basics.h"
#include "handle_vector_views.h"
#include "handle_vector_tables.h"
#include "handle_metadata.h"


/* --------------------- Functions to create views -------------------- */
int createNewIntegerVectorView(MYSQL * sqlConn, rdbVector * viewVector,
			       char sqlString[])
{
  viewVector->sxp_type = SXP_TYPE_INTEGER;
  return internalCreateNewVectorView(sqlConn, viewVector,
				     sqlTemplateCreateVectorView, sqlString);
}


int createNewDoubleVectorView(MYSQL * sqlConn, rdbVector * viewVector,
			      char sqlString[])
{
  viewVector->sxp_type = SXP_TYPE_DOUBLE;
  return internalCreateNewVectorView(sqlConn, viewVector, 
				     sqlTemplateCreateVectorView, sqlString);
}


int createNewStringVectorView(MYSQL * sqlConn, rdbVector * viewVector,
			      char sqlString[])
{
  viewVector->sxp_type = SXP_TYPE_STRING;
  return internalCreateNewVectorView(sqlConn, viewVector, 
				     sqlTemplateCreateVectorView, sqlString);
}


int createNewComplexVectorView(MYSQL * sqlConn, rdbVector * viewVector,
			       char sqlString[])
{
  viewVector->sxp_type = SXP_TYPE_COMPLEX;
  return internalCreateNewVectorView(sqlConn, viewVector,
				     sqlTemplateCreateComplexVectorView, sqlString);
}


int createNewLogicVectorView(MYSQL * sqlConn, rdbVector * viewVector,
			     char sqlString[])
{
  viewVector->sxp_type = SXP_TYPE_LOGIC;
  return internalCreateNewVectorView(sqlConn, viewVector,
				     sqlTemplateCreateVectorView, sqlString);
}


int internalCreateNewVectorView(MYSQL * sqlConn, rdbVector * vectorInfo, 
				char sqlTemplate[], char sqlString[])
{
  /* Build the name of the new view */
  if( !buildUniqueVectorViewName(sqlConn, &(vectorInfo->tableName)) )
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
  success = insertVectorMetadataInfo(sqlConn, vectorInfo);
 
  return success;
}


/* ------------------ Functions for naming views ------------------- */

int buildUniqueVectorViewName(MYSQL * sqlConn, char ** newViewName)
{
  int next = 0;
  if( !getNextTableID(sqlConn, &next) )
    return 0;

  /* Build the name of the new view */
  if( *newViewName != NULL )
     free(*newViewName);
  int length = strlen(sqlTemplateVectorViewName) + MAX_INT_LENGTH;
  *newViewName = (char*) malloc(length * sizeof(char));
  sprintf( *newViewName, sqlTemplateVectorViewName, next);

  return 1;
}


/* -------------- Functions to manage view references -------------- */

int createVectorViewReferences(MYSQL * sqlConn, rdbVector * viewVector,
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


int getVectorViewReferences(MYSQL * sqlConn, rdbVector * viewVector,
                            rdbObject *leftInput, rdbObject *rightInput)
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
  success *= loadRDBObject(sqlConn, leftInput, leftID);
  success *= loadRDBObject(sqlConn, rightInput, rightID);

  return success;
}


int getVectorViewRefCount(MYSQL * sqlConn, rdbVector * vectorInfo, int * count)
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


int updateVectorViewReferences(MYSQL * sqlConn, rdbVector * viewVector, 
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


int removeVectorViewReferences(MYSQL * sqlConn, rdbVector * viewVector)
{
  /* Get the references */
  rdbObject *leftInput = newRDBObject();
  rdbObject *rightInput = newRDBObject();
  int success = getVectorViewReferences(sqlConn, viewVector, 
                                        leftInput, rightInput);
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
  success = deleteRDBObject(sqlConn, leftInput);

  if( getMetadataIDInRDBObject(leftInput) != getMetadataIDInRDBObject(rightInput) )
    success *= deleteRDBObject(sqlConn, rightInput);

  /* Clean up */
  clearRDBObject(&leftInput);
  clearRDBObject(&rightInput);

  return success;
}


/* ---------------- Functions to delete vector views ---------------- */

int dropVectorView(MYSQL * sqlConn, rdbVector * vectorInfo)
{
  /* Safe guard - only drop view if num references == 0 */
  int refCounter = 0;
  getVectorViewRefCount(sqlConn, vectorInfo, &refCounter);
  if( refCounter != 0 )
  {
     vectorInfo->refCounter = refCounter;
     return setRefCounter(sqlConn, vectorInfo->metadataID, refCounter);
  }

  /* Build the sql string and drop the view */
  int length = strlen(sqlTemplateDropVectorView) + 
               strlen(vectorInfo->tableName) + 1;
  char strDropViewSQL[length];
  sprintf( strDropViewSQL, sqlTemplateDropVectorView, vectorInfo->tableName );

  int success = mysql_query(sqlConn, strDropViewSQL);
  if( success != 0 )
     return 0;

  /* Delete corresponding metadata info */
  success = deleteMetadataInfo(sqlConn, vectorInfo->metadataID);
  success *= removeVectorViewReferences(sqlConn, vectorInfo);

  return success;
}


/* ----------------- Functions to materialize views ----------------- */

int ensureVectorMaterialization(MYSQL * sqlConn, rdbVector * vectorInfo)
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
     success *= materializeVectorView(sqlConn, vectorInfo);

  /* Materialize all referenced views */
  for( i = 0 ; i < numRes ; i++ )
  {
     rdbVector *viewVector = newRDBVector();
     int loadSuccess = loadRDBVector(sqlConn, viewVector, viewIDs[i]);
      
     if( loadSuccess )
     {
        success = materializeVectorView(sqlConn, viewVector);
	clearRDBVector(&viewVector);
     }
  }
  return success;
}


int materializeVectorView(MYSQL * sqlConn, rdbVector * viewVector)
{
  if( !viewVector->isView )
     return 0;

  if( viewVector->sxp_type == SXP_TYPE_INTEGER )
     return materializeIntegerVectorView(sqlConn, viewVector);

  if( viewVector->sxp_type == SXP_TYPE_DOUBLE )
     return materializeDoubleVectorView(sqlConn, viewVector);

  if( viewVector->sxp_type == SXP_TYPE_COMPLEX )
     return materializeComplexVectorView(sqlConn, viewVector);

  if( viewVector->sxp_type == SXP_TYPE_STRING )
     return materializeStringVectorView(sqlConn, viewVector);

  if( viewVector->sxp_type == SXP_TYPE_LOGIC )
     return materializeLogicVectorView(sqlConn, viewVector);

  return 0;
}


int materializeIntegerVectorView(MYSQL * sqlConn, rdbVector * viewVector)
{
  /* Create the new table */
  rdbVector * newVector = newRDBVector();
  newVector->isView = 0;
  if( !createNewIntVectorTable(sqlConn, newVector) ) 
     return 0;

  int success = internalMaterializeVectorView(sqlConn, viewVector, newVector);

  clearRDBVector(&newVector);

  return success;
}


int materializeDoubleVectorView(MYSQL * sqlConn, rdbVector * viewVector)
{
  /* Create the new table */
  rdbVector * newVector = newRDBVector();
  newVector->isView = 0;
  if( !createNewDoubleVectorTable(sqlConn, newVector) ) 
     return 0;

  int success = internalMaterializeVectorView(sqlConn, viewVector, newVector);

  clearRDBVector(&newVector);

  return success;
}


int materializeStringVectorView(MYSQL * sqlConn, rdbVector * viewVector)
{
  /* Create the new table */
  rdbVector * newVector = newRDBVector();
  newVector->isView = 0;
  if( !createNewStringVectorTable(sqlConn, newVector) ) 
     return 0;

  int success = internalMaterializeVectorView(sqlConn, viewVector, newVector);

  clearRDBVector(&newVector);

  return success;
}


int materializeComplexVectorView(MYSQL * sqlConn, rdbVector * viewVector)
{
  /* Create the new table */
  rdbVector * newVector = newRDBVector();
  newVector->isView = 0;
  if( !createNewComplexVectorTable(sqlConn, newVector) ) 
     return 0;

  int success = internalMaterializeVectorView(sqlConn, viewVector, newVector);

  clearRDBVector(&newVector);

  return success;
}


int materializeLogicVectorView(MYSQL * sqlConn, rdbVector * viewVector)
{
  /* Create the new table */
  rdbVector * newVector = newRDBVector();
  newVector->isView = 0;
  if( !createNewLogicVectorTable(sqlConn, newVector) ) 
     return 0;

  int success = internalMaterializeVectorView(sqlConn, viewVector, newVector);

  clearRDBVector(&newVector);

  return success;
}


int internalMaterializeVectorView(MYSQL * sqlConn, rdbVector * viewVector, 
			    rdbVector * newVector)
{
  /* Build the sql string and add the data from the view */
  int length = strlen(sqlTemplateMaterializeVectorView) + 
	       strlen(newVector->tableName) +
               strlen(viewVector->tableName) + 1;
  char strMaterializeViewSQL[length];
  sprintf( strMaterializeViewSQL, sqlTemplateMaterializeVectorView, 
	   newVector->tableName, viewVector->tableName );

  int success = mysql_query(sqlConn, strMaterializeViewSQL);

  if( success != 0 )
     return 0;

  /* Remove the current view */
  length = strlen(sqlTemplateDropVectorView) + strlen(viewVector->tableName) + 1;
  char strDropViewSQL[length];
  sprintf( strDropViewSQL, sqlTemplateDropVectorView, viewVector->tableName );

  success = mysql_query(sqlConn, strDropViewSQL);
  if( success != 0 )
     return 0;

  /* Delete corresponding references */
  success = removeVectorViewReferences(sqlConn, viewVector);

  /* Rename new vector table to the view's name*/
  success *= renameTable(sqlConn, newVector->tableName, viewVector->tableName);

  /* Update the metadata info of the view table */
  viewVector->isView = 0;
  success *= updateVectorMetadataInfo(sqlConn, viewVector);

  /* Delete Metadata info of new table */
  success *= deleteMetadataInfo(sqlConn, newVector->metadataID);

  return success;
}

