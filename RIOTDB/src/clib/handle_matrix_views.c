/*****************************************************************************
 * Contains functions for handling matrix views (i.e create, drop, 
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
#include "handle_matrix_views.h"
#include "handle_matrix_tables.h"
#include "handle_metadata.h"


/* --------------------- Functions to create views -------------------- */
int createNewIntegerMatrixView(MYSQL * sqlConn, rdbMatrix * viewMatrix,
			       char sqlString[])
{
  viewMatrix->sxp_type = SXP_TYPE_INTEGER;
  return internalCreateNewMatrixView(sqlConn, viewMatrix,
				     sqlTemplateCreateMatrixView, sqlString);
}

int createNewDoubleMatrixView(MYSQL * sqlConn, rdbMatrix * viewMatrix,
			      char sqlString[])
{
  viewMatrix->sxp_type = SXP_TYPE_DOUBLE;
  return internalCreateNewMatrixView(sqlConn, viewMatrix, 
				     sqlTemplateCreateMatrixView, sqlString);
}

int internalCreateNewMatrixView(MYSQL * sqlConn, rdbMatrix * matrixInfo, 
				char sqlTemplate[], char sqlString[])
{
  /* Build the name of the new view */
  if( !buildUniqueMatrixViewName(sqlConn, &(matrixInfo->tableName)) )
    return 0;

  /* Build the sql string */
  int length = strlen(sqlTemplate) + strlen(matrixInfo->tableName) + 
               strlen(sqlString) + 1;
  char strCreateViewSQL[length];
  sprintf( strCreateViewSQL, sqlTemplate, matrixInfo->tableName, sqlString );

  /* Execute the query */
  int success = mysql_query(sqlConn, strCreateViewSQL);
  if( success != 0 )
     return 0;

  /* Insert info into Metadata table */
  success = insertMatrixMetadataInfo(sqlConn, matrixInfo);
 
  return success;
}


/* ------------------ Functions for naming views ------------------- */

int buildUniqueMatrixViewName(MYSQL * sqlConn, char ** newViewName)
{
  int next = 0;
  if( !getNextTableID(sqlConn, &next) )
    return 0;

  /* Build the name of the new view */
  if( *newViewName != NULL )
     free(*newViewName);
  int length = strlen(sqlTemplateMatrixViewName) + MAX_INT_LENGTH;
  *newViewName = (char*) malloc(length * sizeof(char));
  sprintf( *newViewName, sqlTemplateMatrixViewName, next);

  return 1;
}


/* -------------- Functions to manage view references -------------- */

int createMatrixViewReferences(MYSQL * sqlConn, rdbMatrix * viewMatrix,
			 rdbMatrix * leftInput, rdbMatrix * rightInput)
{
  /* Build the sql string and create references */
  int length = strlen(sqlTemplateCreateViewReferences) + 3*MAX_INT_LENGTH + 1;
  char strCreateRefsSQL[length];
  sprintf(strCreateRefsSQL, sqlTemplateCreateViewReferences,
	  viewMatrix->metadataID, leftInput->metadataID, rightInput->metadataID);

  int success = mysql_query(sqlConn, strCreateRefsSQL);
  if( success != 0 )
     return 0;

  /* Increment the references */
  leftInput->refCounter++;
  rightInput->refCounter++;

  return success;
}


int getMatrixViewReferences(MYSQL * sqlConn, rdbMatrix * viewMatrix,
	rdbMatrix ** leftInput, rdbMatrix ** rightInput, int alloc)
{
  /* Build the sql string and execute the query */
  int length = strlen(sqlTemplateGetViewReferences) + MAX_INT_LENGTH + 1;
  char strGetRefsSQL[length];
  sprintf(strGetRefsSQL, sqlTemplateGetViewReferences, viewMatrix->metadataID);

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

  /* Load the rdbMatrixs from the Metadata table */
  success *= loadRDBMatrix(sqlConn, leftInput, leftID, alloc);
  success *= loadRDBMatrix(sqlConn, rightInput, rightID, alloc);

  return success;
}


int getMatrixViewRefCount(MYSQL * sqlConn, rdbMatrix * matrixInfo, int * count)
{
  /* Build the sql string and execute the query */
  int length = strlen(sqlTemplateGetViewRefCount) + 2*MAX_INT_LENGTH + 1;
  char strGetRefsSQL[length];
  sprintf(strGetRefsSQL, sqlTemplateGetViewRefCount, 
	  matrixInfo->metadataID, matrixInfo->metadataID);

  int success = mysql_query(sqlConn, strGetRefsSQL);
  if( success != 0 )
     return 0;

  /* Get how many times this matrix is referenced in views */
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


int updateMatrixViewReferences(MYSQL * sqlConn, rdbMatrix * viewMatrix, 
			 rdbMatrix * newMatrix)
{
  /* Build the sql strings */
  int length = strlen(sqlTemplateUpdateViewReferences1) + 2*MAX_INT_LENGTH + 1;
  char strUpdateRefs1SQL[length];
  sprintf(strUpdateRefs1SQL, sqlTemplateUpdateViewReferences1, 
	  newMatrix->metadataID, viewMatrix->metadataID);

  length = strlen(sqlTemplateUpdateViewReferences2) + 2*MAX_INT_LENGTH + 1;
  char strUpdateRefs2SQL[length];
  sprintf(strUpdateRefs2SQL, sqlTemplateUpdateViewReferences2,
	  newMatrix->metadataID, viewMatrix->metadataID);

  /* Execute the queries */
  int success1 = mysql_query(sqlConn, strUpdateRefs1SQL);
  int success2 = mysql_query(sqlConn, strUpdateRefs2SQL);

  return ( success1 == 0 && success2 == 0 )? 1 : 0;
}


int removeMatrixViewReferences(MYSQL * sqlConn, rdbMatrix * viewMatrix)
{
  /* Get the references */
  rdbMatrix *leftInput, *rightInput;
  int success = getMatrixViewReferences(sqlConn, viewMatrix, 
				  &leftInput, &rightInput, 1);
  if( success == 0 )
    return 0;

  /* Build the sql string and remove references from table */
  int length = strlen(sqlTemplateRemoveViewReferences) + MAX_INT_LENGTH + 1;
  char strRemoveRefsSQL[length];
  sprintf( strRemoveRefsSQL, sqlTemplateRemoveViewReferences,
	   viewMatrix->metadataID);

  success = mysql_query(sqlConn, strRemoveRefsSQL);
  if( success != 0 )
    return 0;
  
  /* Reccursively delete the references */
  success = deleteRDBMatrix(sqlConn, leftInput);

  if( leftInput->metadataID != rightInput->metadataID )
    success *= deleteRDBMatrix(sqlConn, rightInput);

  /* Clean up */
  clearRDBMatrix(&leftInput);
  clearRDBMatrix(&rightInput);

  return success;
}


/* ---------------- Functions to delete matrix views ---------------- */

int dropMatrixView(MYSQL * sqlConn, rdbMatrix * matrixInfo)
{
  /* Safe guard - only drop view if num references == 0 */
  int refCounter = 0;
  getMatrixViewRefCount(sqlConn, matrixInfo, &refCounter);
  if( refCounter != 0 )
  {
     matrixInfo->refCounter = refCounter;
     return setRefCounter(sqlConn, matrixInfo->metadataID, refCounter);
  }

  /* Build the sql string and drop the view */
  int length = strlen(sqlTemplateDropMatrixView) + 
               strlen(matrixInfo->tableName) + 1;
  char strDropViewSQL[length];
  sprintf( strDropViewSQL, sqlTemplateDropMatrixView, matrixInfo->tableName );

  int success = mysql_query(sqlConn, strDropViewSQL);
  if( success != 0 )
     return 0;

  /* Delete corresponding metadata info */
  success = deleteMetadataInfo(sqlConn, matrixInfo->metadataID);
  success *= removeMatrixViewReferences(sqlConn, matrixInfo);

  return success;
}


/* ----------------- Functions to materialize views ----------------- */

int ensureMatrixMaterialization(MYSQL * sqlConn, rdbMatrix * matrixInfo)
{
  /* Build the sql string */
  int length = strlen(sqlTemplateGetReferredViews) + 2*MAX_INT_LENGTH + 1;
  char strGetRefViewsSQL[length];
  sprintf( strGetRefViewsSQL, sqlTemplateGetReferredViews,
	   matrixInfo->metadataID, matrixInfo->metadataID );

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

  /* Materialize current matrix if a view */
  success = 1;
  if( matrixInfo->isView )
     success *= materializeMatrixView(sqlConn, matrixInfo);

  /* Materialize all referenced views */
  for( i = 0 ; i < numRes ; i++ )
  {
     rdbMatrix *viewMatrix;
     int loadSuccess = loadRDBMatrix(sqlConn, &viewMatrix, viewIDs[i], 1);
      
     if( loadSuccess )
     {
        success = materializeMatrixView(sqlConn, viewMatrix);
	clearRDBMatrix(&viewMatrix);
     }
  }
  return success;
}


int materializeMatrixView(MYSQL * sqlConn, rdbMatrix * viewMatrix)
{
  if( !viewMatrix->isView )
     return 0;

  if( viewMatrix->sxp_type == SXP_TYPE_INTEGER )
     return materializeIntegerMatrixView(sqlConn, viewMatrix);

  if( viewMatrix->sxp_type == SXP_TYPE_DOUBLE )
     return materializeDoubleMatrixView(sqlConn, viewMatrix);

  return 0;
}


int materializeIntegerMatrixView(MYSQL * sqlConn, rdbMatrix * viewMatrix)
{
  /* Create the new table */
  rdbMatrix * newMatrix;
  initRDBMatrix(&newMatrix, 0, 1);
  if( !createNewIntMatrixTable(sqlConn, newMatrix) ) 
     return 0;

  int success = internalMaterializeMatrixView(sqlConn, viewMatrix, newMatrix);

  clearRDBMatrix(&newMatrix);

  return success;
}


int materializeDoubleMatrixView(MYSQL * sqlConn, rdbMatrix * viewMatrix)
{
  /* Create the new table */
  rdbMatrix * newMatrix;
  initRDBMatrix(&newMatrix, 0, 1);
  newMatrix->isView = 0;
  if( !createNewDoubleMatrixTable(sqlConn, newMatrix) ) 
     return 0;

  int success = internalMaterializeMatrixView(sqlConn, viewMatrix, newMatrix);

  clearRDBMatrix(&newMatrix);

  return success;
}

int internalMaterializeMatrixView(MYSQL * sqlConn, rdbMatrix * viewMatrix, 
			          rdbMatrix * newMatrix)
{
  /* Build the sql string and add the data from the view */
  int length = strlen(sqlTemplateMaterializeMatrixView) 
		+ strlen(newMatrix->tableName)
                + strlen(viewMatrix->tableName) + 1;
  char strMaterializeViewSQL[length];
  sprintf( strMaterializeViewSQL, sqlTemplateMaterializeMatrixView, 
	   newMatrix->tableName, viewMatrix->tableName );

  int success = mysql_query(sqlConn, strMaterializeViewSQL);

  if( success != 0 )
     return 0;

  /* Remove the current view */
  length = strlen(sqlTemplateDropMatrixView) + strlen(viewMatrix->tableName) + 1;
  char strDropViewSQL[length];
  sprintf( strDropViewSQL, sqlTemplateDropMatrixView, viewMatrix->tableName );

  success = mysql_query(sqlConn, strDropViewSQL);
  if( success != 0 )
     return 0;

  /* Delete corresponding references */
  success = removeMatrixViewReferences(sqlConn, viewMatrix);

  /* Rename new matrix table to the view's name*/
  success *= renameTable(sqlConn, newMatrix->tableName, viewMatrix->tableName);

  /* Update the metadata info of the view table */
  viewMatrix->isView = 0;
  success *= updateMatrixMetadataInfo(sqlConn, viewMatrix);

  /* Delete Metadata info of new table */
  success *= deleteMetadataInfo(sqlConn, newMatrix->metadataID);

  return success;
}

