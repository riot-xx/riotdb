/*****************************************************************************
 * Contains functions for managing the matadata table.
 *
 * Author: Herodotos Herodotou
 * Date:   Sep 17, 2008
 ****************************************************************************/

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <mysql.h>
#include "rdb_basics.h"
#include "rdb_handle_metadata.h"



/* ----- Functions to keep track of table naming scheme ----- */

int getNextTableID(MYSQL * sqlConn, int * next)
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


/* ------ Functions to handle the reference counter for Vectors -------- */

int decrementRefCounter(MYSQL* sqlConn, int* newRefCounter, long int metadataID)
{
  /* Get the counter */
  int refCounter = 0;
  if( !getRefCounter(sqlConn, metadataID, &refCounter) )
    return 0;

  /* Decrement the counter */
  refCounter = (refCounter <= 0)? 0 : (refCounter - 1);

  /* Set the new counter */
  *newRefCounter = refCounter;
  return setRefCounter(sqlConn, metadataID, refCounter);
}


int incrementRefCounter(MYSQL * sqlConn, int* newRefCounter, long int metadataID)
{
  /* Get the counter */
  int refCounter = 0;
  if( !getRefCounter(sqlConn, metadataID, &refCounter) )
    return 0;

  /* Increment the counter */
  refCounter = (refCounter <= 0)? 1 : (refCounter + 1);

  /* Set the new counter */
  *newRefCounter = refCounter;
  return setRefCounter(sqlConn, metadataID, refCounter);
}


int getRefCounter(MYSQL * sqlConn, long int metadataID, int * refCounter)
{
  /* Build the sql string */
  int length = strlen(sqlTemplateGetRefCounter) + MAX_INT_LENGTH + 1;
  char strGetRecCounterSQL[length];
  sprintf( strGetRecCounterSQL, sqlTemplateGetRefCounter, metadataID );

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


int setRefCounter(MYSQL * sqlConn, long int metadataID, int refCounter)
{
  /* Store the new counter back into the metadata table */
  int length = strlen(sqlTemplateSetRefCounter) +2 * MAX_INT_LENGTH + 1;
  char strSetRecCounterSQL[length];
  sprintf( strSetRecCounterSQL, sqlTemplateSetRefCounter,
	   refCounter, metadataID );

  int success = mysql_query(sqlConn, strSetRecCounterSQL);

  return ( success != 0 )? 0 : 1;
}


/* ----------------- Functions to handle Metadata ------------------ */

int insertVectorMetadataInfo(MYSQL * sqlConn, rdbVector * vectorInfo)
{
  /* Build the sql string */
  int length = strlen(sqlTemplateInsertMetadata) + 
               strlen(vectorInfo->tableName) + 14*MAX_INT_LENGTH + 1;
  char strInsertMetadataInfo[length];
  char strSize[MAX_INT_LENGTH+1];
  sprintf( strSize, "%d", vectorInfo->size );

  sprintf( strInsertMetadataInfo, sqlTemplateInsertMetadata,
	   vectorInfo->tableName,
	   vectorInfo->dbSourceID, 
	   strSize,
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
  if( success) {
	fprintf(stderr, "insert metadata failed: %u (%s)\n",
			mysql_errno(sqlConn), mysql_error(sqlConn));
     return 0;
  }

  /* Get the metadata info index */
  vectorInfo->metadataID = (unsigned int)mysql_insert_id(sqlConn);

  return 1;
}

int updateVectorMetadataInfo(MYSQL * sqlConn, rdbVector * vectorInfo)
{
  /* Build the sql string */
  int length = strlen(sqlTemplateUpdateMetadata) + 
               strlen(vectorInfo->tableName) + 14*MAX_INT_LENGTH + 1;
  char strUpdateMetadataInfo[length];
  char strSize[MAX_INT_LENGTH+1];
  sprintf( strSize, "%d", vectorInfo->size );

  sprintf( strUpdateMetadataInfo, sqlTemplateUpdateMetadata,
	   vectorInfo->tableName,
	   vectorInfo->dbSourceID, 
	   strSize,
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

int insertMatrixMetadataInfo(MYSQL * sqlConn, rdbMatrix * matrixInfo)
{
  /* Build the sql string */
  int length = strlen(sqlTemplateInsertMetadata) + 
               strlen(matrixInfo->tableName) + 14*MAX_INT_LENGTH + 1;
  char strInsertMetadataInfo[length];
  char strSize[2*MAX_INT_LENGTH+2];
  sprintf( strSize, "%d,%d", matrixInfo->numRows, matrixInfo->numCols );

  sprintf( strInsertMetadataInfo, sqlTemplateInsertMetadata,
	   matrixInfo->tableName,
	   matrixInfo->dbSourceID, 
	   strSize,
	   matrixInfo->isView, 
	   matrixInfo->refCounter,
	   matrixInfo->sxp_type,
	   matrixInfo->sxp_obj,
	   matrixInfo->sxp_named,
	   matrixInfo->sxp_gp,
	   matrixInfo->sxp_mark,
	   matrixInfo->sxp_debug,
	   matrixInfo->sxp_trace,
	   matrixInfo->sxp_spare,
	   matrixInfo->sxp_gcgen,
	   matrixInfo->sxp_gccls );


  /* Execute the query */
  int success = mysql_query(sqlConn, strInsertMetadataInfo);
  if( success) {
	fprintf(stderr, "insert metadata failed: %u (%s)\n",
			mysql_errno(sqlConn), mysql_error(sqlConn));
     return 0;
  }

  /* Get the metadata info index */
  matrixInfo->metadataID = (unsigned int)mysql_insert_id(sqlConn);

  return 1;
}

int updateMatrixMetadataInfo(MYSQL * sqlConn, rdbMatrix * matrixInfo)
{
  /* Build the sql string */
  int length = strlen(sqlTemplateUpdateMetadata) + 
               strlen(matrixInfo->tableName) + 14*MAX_INT_LENGTH + 1;
  char strUpdateMetadataInfo[length];
  char strSize[2*MAX_INT_LENGTH+2];
  sprintf( strSize, "%d,%d", matrixInfo->numRows, matrixInfo->numCols );

  sprintf( strUpdateMetadataInfo, sqlTemplateUpdateMetadata,
	   matrixInfo->tableName,
	   matrixInfo->dbSourceID, 
	   strSize,
	   matrixInfo->isView, 
	   matrixInfo->refCounter,
	   matrixInfo->sxp_type,
	   matrixInfo->sxp_obj,
	   matrixInfo->sxp_named,
	   matrixInfo->sxp_gp,
	   matrixInfo->sxp_mark,
	   matrixInfo->sxp_debug,
	   matrixInfo->sxp_trace,
	   matrixInfo->sxp_spare,
	   matrixInfo->sxp_gcgen,
	   matrixInfo->sxp_gccls,
	   matrixInfo->metadataID );

  /* Execute the query */
  int success = mysql_query(sqlConn, strUpdateMetadataInfo);
  return ( success != 0 )? 0 : 1;
}

int deleteMetadataInfo(MYSQL * sqlConn, long int metadataID)
{
  /* Build the sql string */
  int length = strlen(sqlTemplateDeleteMetadata) + MAX_INT_LENGTH;
  char strDeleteMetadataInfo[length];
  sprintf( strDeleteMetadataInfo, sqlTemplateDeleteMetadata, metadataID);

  /* Execute the query */
  int success = mysql_query(sqlConn, strDeleteMetadataInfo);

  return (success != 0) ? 0 : 1;
}

