/*****************************************************************************
 * Contains necessary constants, the RDBVector definition, the RDBMatrix
 * definition and functions for initializing, copying, clearing and loading
 * RDBVectors and RDBMatrices.
 *
 * Author: Herodotos Herodotou
 * Date:   Sep 17, 2008
 *
 * Revision 1 :
 * By Yi Zhang
 * Date:   Jan 4, 2009
 * The sxp_spare field is used for reference counting, and should be
 * initialized to 0.
 *
 ****************************************************************************/

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <mysql.h>
#include "basics.h"
#include "config.h"


int connectToLocalDB(MYSQL ** sqlConn)
{
  /* Connect to local db */
  *sqlConn = mysql_init(NULL);
  *sqlConn = mysql_real_connect(*sqlConn, DB_SERVER, DB_USERNAME, DB_PASSWORD,
		DB_NAME, DB_PORT, NULL, 0/*CLIENT_MULTI_STATEMENTS*/);

  if( sqlConn == NULL || *sqlConn == NULL )
  {
     return 0;
  }

  return 1;

}

rdbObject *newRDBObject() {
  rdbObject *info = (rdbObject *)malloc(sizeof(rdbObject));
  initRDBObject(info);
  return info;
}

void initRDBObject(rdbObject *info) {
  info->isVector = FALSE;
  initRDBMatrix(&(info->payload.matrix));
  return;
}

void copyRDBObjectFromVector(rdbObject *info, rdbVector *vector) {
  info->isVector = TRUE;
  copyRDBVector(&(info->payload.vector), vector);
  return;
}

void copyRDBObjectFromMatrix(rdbObject *info, rdbMatrix *matrix) {
  info->isVector = FALSE;
  copyRDBMatrix(&(info->payload.matrix), matrix);
  return;
}

unsigned long int getMetadataIDInRDBObject(rdbObject *info) {
  return (info->isVector ?
          info->payload.vector.metadataID :
          info->payload.matrix.metadataID);
}

void clearRDBObject(rdbObject **info) {
  char **tableName = (*info)->isVector ?
    &((*info)->payload.vector.tableName) :
    &((*info)->payload.matrix.tableName);
  if (*tableName != NULL)
    free(*tableName);
  *tableName = NULL;
  free(*info);
  info = NULL;
}

void initRDBObjectFromSQLRow(rdbObject *info, MYSQL_ROW sqlRow) {
  if (strstr(sqlRow[3], "Vector") == sqlRow[3]) {
    info->isVector = TRUE;
    initRDBVectorFromSQLRow(&(info->payload.vector), sqlRow);
  } else {
    info->isVector = FALSE;
    initRDBMatrixFromSQLRow(&(info->payload.matrix), sqlRow);
  }
  return;
}

void initRDBVectorFromSQLRow(rdbVector *info, MYSQL_ROW sqlRow) {
  int length = strlen(sqlRow[1]) + 1;
  if (info->tableName != NULL) free(info->tableName);
  info->tableName = (char *)malloc(length * sizeof(char));
  strcpy(info->tableName, sqlRow[1]);

  info->metadataID = atoi(sqlRow[0]);
  info->dbSourceID = atoi(sqlRow[2]);
  info->size       = atoi(sqlRow[3]);
  info->isView     = atoi(sqlRow[4]);
  info->refCounter = atoi(sqlRow[5]);

  info->sxp_type   = atoi(sqlRow[6]);
  info->sxp_obj    = atoi(sqlRow[7]);
  info->sxp_named  = atoi(sqlRow[8]);
  info->sxp_gp     = atoi(sqlRow[9]);
  info->sxp_mark   = atoi(sqlRow[10]);
  info->sxp_debug  = atoi(sqlRow[11]);
  info->sxp_trace  = atoi(sqlRow[12]);
  info->sxp_spare  = atoi(sqlRow[13]);
  info->sxp_gcgen  = atoi(sqlRow[14]);
  info->sxp_gccls  = atoi(sqlRow[15]);
  return;
}

void initRDBMatrixFromSQLRow(rdbMatrix *info, MYSQL_ROW sqlRow) {
  int length = strlen(sqlRow[1]) + 1;
  if (info->tableName != NULL) free(info->tableName);
  info->tableName = (char *)malloc(length * sizeof(char));
  strcpy(info->tableName, sqlRow[1]);
  getRDBMatrixDimensions(sqlRow[3], &(info->numRows), &(info->numCols));

  info->metadataID = atoi(sqlRow[0]);
  info->dbSourceID = atoi(sqlRow[2]);
  info->isView     = atoi(sqlRow[4]);
  info->refCounter = atoi(sqlRow[5]);

  info->sxp_type   = atoi(sqlRow[6]);
  info->sxp_obj    = atoi(sqlRow[7]);
  info->sxp_named  = atoi(sqlRow[8]);
  info->sxp_gp     = atoi(sqlRow[9]);
  info->sxp_mark   = atoi(sqlRow[10]);
  info->sxp_debug  = atoi(sqlRow[11]);
  info->sxp_trace  = atoi(sqlRow[12]);
  info->sxp_spare  = atoi(sqlRow[13]);
  info->sxp_gcgen  = atoi(sqlRow[14]);
  info->sxp_gccls  = atoi(sqlRow[15]);
  return;
}

int loadRDBObject(MYSQL * sqlConn, rdbObject *info, unsigned long int metadataID) {
  /* Build the sql string */
  int length = strlen(sqlGetRDBObject) + MAX_INT_LENGTH + 1;
  char strLoadMetadataInfo[length];
  sprintf( strLoadMetadataInfo, sqlGetRDBObject, metadataID );

  /* Execute the query */
  int success = mysql_query(sqlConn, strLoadMetadataInfo);
  if( success != 0 )
     return 0;

  /* Get the results from the query */
  MYSQL_RES *sqlRes;
  MYSQL_ROW sqlRow;
  sqlRes = mysql_use_result(sqlConn);
  sqlRow = mysql_fetch_row(sqlRes);
  if( sqlRow != NULL )
  {
     initRDBObjectFromSQLRow(info, sqlRow);
     success = 1;
  }

  /* C API requires to clean the result set */
  while( (sqlRow = mysql_fetch_row(sqlRes)) != NULL );
  mysql_free_result(sqlRes);

  return success;
}

/* ---------- Functions to handle RDBVector objects ---------- */
rdbVector *newRDBVector() {
  rdbVector *vectorInfo = (rdbVector *)malloc( sizeof(rdbVector) );
  initRDBVector(vectorInfo);
  return vectorInfo;
}

void initRDBVector(rdbVector *vectorInfo)
{
  vectorInfo->tableName = NULL;

  vectorInfo->metadataID = 0;
  vectorInfo->dbSourceID = 1;
  vectorInfo->size       = 0;
  vectorInfo->isView     = 0;
  vectorInfo->refCounter = 1;

  vectorInfo->sxp_type   = 0;
  vectorInfo->sxp_obj    = 1;
  vectorInfo->sxp_named  = 0;
  vectorInfo->sxp_gp     = 0;
  vectorInfo->sxp_mark   = 0;
  vectorInfo->sxp_debug  = 0;
  vectorInfo->sxp_trace  = 0;
  vectorInfo->sxp_spare  = 0;
  vectorInfo->sxp_gcgen  = 0;
  vectorInfo->sxp_gccls  = 0;

}


void copyRDBVector(rdbVector *copyVector, rdbVector *originalVector)
{
  copyVector->metadataID = originalVector->metadataID;
  if (copyVector->tableName != NULL)
    free(copyVector->tableName);
  copyVector->tableName = (char*) malloc((1+strlen(originalVector->tableName)) * sizeof(char));
  strcpy(copyVector->tableName, originalVector->tableName);
  copyVector->dbSourceID = originalVector->dbSourceID;
  copyVector->size       = originalVector->size;
  copyVector->isView     = originalVector->isView;
  copyVector->refCounter = originalVector->refCounter;

  copyVector->sxp_type   = originalVector->sxp_type;
  copyVector->sxp_obj    = originalVector->sxp_obj;
  copyVector->sxp_named  = originalVector->sxp_named;
  copyVector->sxp_gp     = originalVector->sxp_gp;
  copyVector->sxp_mark   = originalVector->sxp_mark;
  copyVector->sxp_debug  = originalVector->sxp_debug;
  copyVector->sxp_trace  = originalVector->sxp_trace;
  copyVector->sxp_spare  = originalVector->sxp_spare;
  copyVector->sxp_gcgen  = originalVector->sxp_gcgen;
  copyVector->sxp_gccls  = originalVector->sxp_gccls;
}


void clearRDBVector(rdbVector ** vectorInfo)
{
  if ((*vectorInfo)->tableName != NULL)
    free( (*vectorInfo)->tableName );
  (*vectorInfo)->tableName = NULL;

  free( *vectorInfo );
  *vectorInfo = NULL;
}


int loadAllRDBVectors(MYSQL * sqlConn, rdbVector *** arrayVectorInfoObjects,
		      unsigned long int * numObjects)
{
  /* Execute the query */
  int success = mysql_query(sqlConn, sqlGetAllRDBVectors);
  if( success != 0 )
  {
     *numObjects = 0;
     return 0;
  }

  /* Get the results from the query */
  MYSQL_RES *sqlRes;
  MYSQL_ROW sqlRow;
  sqlRes = mysql_store_result(sqlConn);

  *numObjects = (unsigned long int)mysql_num_rows(sqlRes);
  int i = 0;

  if( (*numObjects) != 0 )
  {
    /* Create all the vectorInfo objects */
    *arrayVectorInfoObjects = (rdbVector **)malloc(
				(*numObjects) * sizeof(rdbVector *) );

    sqlRow = mysql_fetch_row(sqlRes);

    while( sqlRow != NULL )
    {
       (*arrayVectorInfoObjects)[i] = newRDBVector();
       initRDBVectorFromSQLRow((*arrayVectorInfoObjects)[i], sqlRow);
       sqlRow = mysql_fetch_row(sqlRes);
       i++;
    }
  }

  mysql_free_result(sqlRes);

  return 1;
}


int loadRDBVector(MYSQL * sqlConn, rdbVector *vectorInfo,
		  unsigned long int metadataID)
{
  /* Build the sql string */
  int length = strlen(sqlGetRDBVector) + MAX_INT_LENGTH + 1;
  char strLoadMetadataInfo[length];
  sprintf( strLoadMetadataInfo, sqlGetRDBVector, metadataID );

  /* Execute the query */
  int success = mysql_query(sqlConn, strLoadMetadataInfo);
  if( success != 0 )
     return 0;

  /* Get the results from the query */
  MYSQL_RES *sqlRes;
  MYSQL_ROW sqlRow;
  sqlRes = mysql_use_result(sqlConn);
  sqlRow = mysql_fetch_row(sqlRes);
  if( sqlRow != NULL )
  {
     initRDBVectorFromSQLRow(vectorInfo, sqlRow);

     success = 1;
  }

  /* C API requires to clean the result set */
  while( (sqlRow = mysql_fetch_row(sqlRes)) != NULL );
  mysql_free_result(sqlRes);

  return success;
}


/* ---------- Functions to handle RDBMatrix objects ---------- */
/* ----------------------------------------------------------- */
rdbMatrix *newRDBMatrix() {
  rdbMatrix *matrixInfo = (rdbMatrix *)malloc( sizeof(rdbMatrix) );
  initRDBMatrix(matrixInfo);
  return matrixInfo;
}

void initRDBMatrix(rdbMatrix *matrixInfo)
{
  matrixInfo->tableName = NULL;

  matrixInfo->metadataID = 0;
  matrixInfo->dbSourceID = 1;
  matrixInfo->numRows    = 0;
  matrixInfo->numCols    = 0;
  matrixInfo->isView     = FALSE;
  matrixInfo->refCounter = 1;

  matrixInfo->sxp_type   = 0;
  matrixInfo->sxp_obj    = 1;
  matrixInfo->sxp_named  = 0;
  matrixInfo->sxp_gp     = 0;
  matrixInfo->sxp_mark   = 0;
  matrixInfo->sxp_debug  = 0;
  matrixInfo->sxp_trace  = 0;
  matrixInfo->sxp_spare  = 0;
  matrixInfo->sxp_gcgen  = 0;
  matrixInfo->sxp_gccls  = 0;

}


void copyRDBMatrix(rdbMatrix * copyMatrix, rdbMatrix * originalMatrix)
{
  copyMatrix->metadataID = originalMatrix->metadataID;
  if (copyMatrix->tableName != NULL)
    free(copyMatrix->tableName);
  copyMatrix->tableName = (char*) malloc((1+strlen(originalMatrix->tableName)) * sizeof(char));
  strcpy(copyMatrix->tableName, originalMatrix->tableName);
  copyMatrix->dbSourceID = originalMatrix->dbSourceID;
  copyMatrix->numRows    = originalMatrix->numRows;
  copyMatrix->numCols    = originalMatrix->numCols;
  copyMatrix->isView     = originalMatrix->isView;
  copyMatrix->refCounter = originalMatrix->refCounter;

  copyMatrix->sxp_type   = originalMatrix->sxp_type;
  copyMatrix->sxp_obj    = originalMatrix->sxp_obj;
  copyMatrix->sxp_named  = originalMatrix->sxp_named;
  copyMatrix->sxp_gp     = originalMatrix->sxp_gp;
  copyMatrix->sxp_mark   = originalMatrix->sxp_mark;
  copyMatrix->sxp_debug  = originalMatrix->sxp_debug;
  copyMatrix->sxp_trace  = originalMatrix->sxp_trace;
  copyMatrix->sxp_spare  = originalMatrix->sxp_spare;
  copyMatrix->sxp_gcgen  = originalMatrix->sxp_gcgen;
  copyMatrix->sxp_gccls  = originalMatrix->sxp_gccls;
}


void clearRDBMatrix(rdbMatrix ** matrixInfo)
{
  if ((*matrixInfo)->tableName != NULL)
    free( (*matrixInfo)->tableName );
  (*matrixInfo)->tableName = NULL;

  free( *matrixInfo );
  *matrixInfo = NULL;
}


int loadAllRDBMatrices(MYSQL * sqlConn, rdbMatrix *** arrayMatrixInfoObjects,
		       unsigned long int * numObjects)
{
  /* Execute the query */
  int success = mysql_query(sqlConn, sqlGetAllRDBMatrix);
  if( success != 0 )
  {
     *numObjects = 0;
     return 0;
  }

  /* Get the results from the query */
  MYSQL_RES *sqlRes;
  MYSQL_ROW sqlRow;
  sqlRes = mysql_store_result(sqlConn);

  *numObjects = (unsigned long int)mysql_num_rows(sqlRes);
  int i = 0;

  if( (*numObjects) != 0 )
  {
    /* Create all the matrixInfo objects */
    *arrayMatrixInfoObjects = (rdbMatrix **)malloc(
				(*numObjects) * sizeof(rdbMatrix *) );

    sqlRow = mysql_fetch_row(sqlRes);

    while( sqlRow != NULL )
    {
       (*arrayMatrixInfoObjects)[i] = newRDBMatrix();
       initRDBMatrixFromSQLRow((*arrayMatrixInfoObjects)[i], sqlRow);
       sqlRow = mysql_fetch_row(sqlRes);
       i++;
    }
  }

  mysql_free_result(sqlRes);

  return 1;
}


int loadRDBMatrix(MYSQL * sqlConn, rdbMatrix * matrixInfo,
		  unsigned long int metadataID)
{
  /* Build the sql string */
  int length = strlen(sqlGetRDBMatrix) + MAX_INT_LENGTH + 1;
  char strLoadMetadataInfo[length];
  sprintf( strLoadMetadataInfo, sqlGetRDBMatrix, metadataID );

  /* Execute the query */
  int success = mysql_query(sqlConn, strLoadMetadataInfo);
  if( success != 0 )
     return 0;

  /* Get the results from the query */
  MYSQL_RES *sqlRes;
  MYSQL_ROW sqlRow;
  sqlRes = mysql_use_result(sqlConn);
  sqlRow = mysql_fetch_row(sqlRes);
  if( sqlRow != NULL )
  {
     initRDBMatrixFromSQLRow(matrixInfo, sqlRow);

     success = 1;
  }

  /* C API requires to clean the result set */
  while( (sqlRow = mysql_fetch_row(sqlRes)) != NULL );
  mysql_free_result(sqlRes);

  return success;
}

void getRDBMatrixDimensions(char * strSize, int * numRows, int * numCols)
{
  char * token = strtok(strSize, ",");
  *numRows = (token != NULL) ? atoi(token) : 0;
  token = strtok(NULL, ",");
  *numCols = (token != NULL) ? atoi(token) : 0;
}

