/*****************************************************************************
 * Contains necessary constants, the RDBVector definition, the RDBMatrix
 * definition and functions for initializing, copying, clearing and loading
 * RDBVectors and RDBMatrices.
 *
 * Author: Herodotos Herodotou
 * Date:   Sep 17, 2008
 ****************************************************************************/

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <mysql.h>
#include "rdb_basics.h"
#include "config.h"


int connectToLocalDB(MYSQL ** sqlConn)
{
  /* Create parameters */
  /* Use macros defined in config.h */
  char *strServer = RDB_SERVER;
  char *strUser = RDB_USERNAME;
  char *strPwd = RDB_PASSWORD;
  char *strDatabase = RDB_DB;

  /* Connect to local db */
  *sqlConn = mysql_init(NULL);
  *sqlConn = mysql_real_connect(*sqlConn, strServer, strUser, strPwd, 
								strDatabase, 0, NULL, 0/*CLIENT_MULTI_STATEMENTS*/);

  if( sqlConn == NULL || *sqlConn == NULL )
  {
     return 0;
  }

  return 1;

}


/* ---------- Functions to handle RDBVector objects ---------- */
void initRDBVector(rdbVector ** vectorInfo, int isView, int alloc)
{
  if( alloc )
  {
    *vectorInfo = (rdbVector *)malloc( sizeof(rdbVector) );
    (*vectorInfo)->tableName = (char*) malloc( (1 + MAX_INT_LENGTH + 
			       strlen(sqlTemplateVectorTableName)) * sizeof(char) );
  }
  else
  {
     (*vectorInfo)->tableName = NULL;
  }

  (*vectorInfo)->metadataID = 0;
  (*vectorInfo)->dbSourceID = 1;
  (*vectorInfo)->size       = 0;
  (*vectorInfo)->isView     = isView;
  (*vectorInfo)->refCounter = 1;

  (*vectorInfo)->sxp_type   = 0;
  (*vectorInfo)->sxp_obj    = 1;
  (*vectorInfo)->sxp_named  = 0;
  (*vectorInfo)->sxp_gp     = 0;
  (*vectorInfo)->sxp_mark   = 0;
  (*vectorInfo)->sxp_debug  = 0;
  (*vectorInfo)->sxp_trace  = 0;
  (*vectorInfo)->sxp_spare  = 0;
  (*vectorInfo)->sxp_gcgen  = 0;
  (*vectorInfo)->sxp_gccls  = 0;

}


void copyRDBVector(rdbVector ** copyVector, rdbVector * originalVector, int alloc)
{
  if( alloc )
  {
    *copyVector = (rdbVector*) malloc( sizeof(rdbVector) );
    (*copyVector)->tableName = (char*) malloc( 
			   (1+strlen(originalVector->tableName)) * sizeof(char) );
  }

  (*copyVector)->metadataID = originalVector->metadataID;
  strcpy((*copyVector)->tableName, originalVector->tableName);
  (*copyVector)->dbSourceID = originalVector->dbSourceID;
  (*copyVector)->size       = originalVector->size;
  (*copyVector)->isView     = originalVector->isView;
  (*copyVector)->refCounter = originalVector->refCounter;

  (*copyVector)->sxp_type   = originalVector->sxp_type;
  (*copyVector)->sxp_obj    = originalVector->sxp_obj;
  (*copyVector)->sxp_named  = originalVector->sxp_named;
  (*copyVector)->sxp_gp     = originalVector->sxp_gp;
  (*copyVector)->sxp_mark   = originalVector->sxp_mark;
  (*copyVector)->sxp_debug  = originalVector->sxp_debug;
  (*copyVector)->sxp_trace  = originalVector->sxp_trace;
  (*copyVector)->sxp_spare  = originalVector->sxp_spare;
  (*copyVector)->sxp_gcgen  = originalVector->sxp_gcgen;
  (*copyVector)->sxp_gccls  = originalVector->sxp_gccls;
}


void clearRDBVector(rdbVector ** vectorInfo)
{
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
       initRDBVector(&((*arrayVectorInfoObjects)[i]), 0, 1);

       int length = strlen(sqlRow[1]) + 1;
       ((*arrayVectorInfoObjects)[i])->tableName = 
	                              (char *)malloc(length * sizeof(char));
       strcpy(((*arrayVectorInfoObjects)[i])->tableName, sqlRow[1]);

       ((*arrayVectorInfoObjects)[i])->metadataID = atoi(sqlRow[0]);
       ((*arrayVectorInfoObjects)[i])->dbSourceID = atoi(sqlRow[2]);
       ((*arrayVectorInfoObjects)[i])->size       = atoi(sqlRow[3]);
       ((*arrayVectorInfoObjects)[i])->isView     = atoi(sqlRow[4]);
       ((*arrayVectorInfoObjects)[i])->refCounter = atoi(sqlRow[5]);

       ((*arrayVectorInfoObjects)[i])->sxp_type   = atoi(sqlRow[6]);
       ((*arrayVectorInfoObjects)[i])->sxp_obj    = atoi(sqlRow[7]);
       ((*arrayVectorInfoObjects)[i])->sxp_named  = atoi(sqlRow[8]);
       ((*arrayVectorInfoObjects)[i])->sxp_gp     = atoi(sqlRow[9]);
       ((*arrayVectorInfoObjects)[i])->sxp_mark   = atoi(sqlRow[10]);
       ((*arrayVectorInfoObjects)[i])->sxp_debug  = atoi(sqlRow[11]);
       ((*arrayVectorInfoObjects)[i])->sxp_trace  = atoi(sqlRow[12]);
       ((*arrayVectorInfoObjects)[i])->sxp_spare  = atoi(sqlRow[13]);
       ((*arrayVectorInfoObjects)[i])->sxp_gcgen  = atoi(sqlRow[14]);
       ((*arrayVectorInfoObjects)[i])->sxp_gccls  = atoi(sqlRow[15]);

       sqlRow = mysql_fetch_row(sqlRes);
       i++;
    }
  }

  mysql_free_result(sqlRes);

  return 1;
}


int loadRDBVector(MYSQL * sqlConn, rdbVector ** vectorInfo,
		  unsigned long int metadataID, int alloc)
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
     initRDBVector(vectorInfo, 0, alloc);

     int length = strlen(sqlRow[1]) + 1;
     (*vectorInfo)->tableName = (char *)malloc(length * sizeof(char));
     strcpy((*vectorInfo)->tableName, sqlRow[1]);

     (*vectorInfo)->metadataID = atoi(sqlRow[0]);
     (*vectorInfo)->dbSourceID = atoi(sqlRow[2]);
     (*vectorInfo)->size       = atoi(sqlRow[3]);
     (*vectorInfo)->isView     = atoi(sqlRow[4]);
     (*vectorInfo)->refCounter = atoi(sqlRow[5]);

     (*vectorInfo)->sxp_type   = atoi(sqlRow[6]);
     (*vectorInfo)->sxp_obj    = atoi(sqlRow[7]);
     (*vectorInfo)->sxp_named  = atoi(sqlRow[8]);
     (*vectorInfo)->sxp_gp     = atoi(sqlRow[9]);
     (*vectorInfo)->sxp_mark   = atoi(sqlRow[10]);
     (*vectorInfo)->sxp_debug  = atoi(sqlRow[11]);
     (*vectorInfo)->sxp_trace  = atoi(sqlRow[12]);
     (*vectorInfo)->sxp_spare  = atoi(sqlRow[13]);
     (*vectorInfo)->sxp_gcgen  = atoi(sqlRow[14]);
     (*vectorInfo)->sxp_gccls  = atoi(sqlRow[15]);

     success = 1;
  }

  /* C API requires to clean the result set */
  while( (sqlRow = mysql_fetch_row(sqlRes)) != NULL );
  mysql_free_result(sqlRes);

  return success;
}


/* ---------- Functions to handle RDBMatrix objects ---------- */
/* ----------------------------------------------------------- */
void initRDBMatrix(rdbMatrix ** matrixInfo, int isView, int alloc)
{
  if( alloc )
  {
    *matrixInfo = (rdbMatrix *)malloc( sizeof(rdbMatrix) );
    (*matrixInfo)->tableName = (char*) malloc( (1 + MAX_INT_LENGTH + 
			       strlen(sqlTemplateMatrixTableName)) * sizeof(char) );
  }
  else
  {
     (*matrixInfo)->tableName = NULL;
  }

  (*matrixInfo)->metadataID = 0;
  (*matrixInfo)->dbSourceID = 1;
  (*matrixInfo)->numRows    = 0;
  (*matrixInfo)->numCols    = 0;
  (*matrixInfo)->isView     = isView;
  (*matrixInfo)->refCounter = 1;

  (*matrixInfo)->sxp_type   = 0;
  (*matrixInfo)->sxp_obj    = 1;
  (*matrixInfo)->sxp_named  = 0;
  (*matrixInfo)->sxp_gp     = 0;
  (*matrixInfo)->sxp_mark   = 0;
  (*matrixInfo)->sxp_debug  = 0;
  (*matrixInfo)->sxp_trace  = 0;
  (*matrixInfo)->sxp_spare  = 0;
  (*matrixInfo)->sxp_gcgen  = 0;
  (*matrixInfo)->sxp_gccls  = 0;

}


void copyRDBMatrix(rdbMatrix ** copyMatrix, rdbMatrix * originalMatrix, int alloc)
{
  if( alloc )
  {
    *copyMatrix = (rdbMatrix*) malloc( sizeof(rdbMatrix) );
    (*copyMatrix)->tableName = (char*) malloc( 
			   (1+strlen(originalMatrix->tableName)) * sizeof(char) );
  }

  (*copyMatrix)->metadataID = originalMatrix->metadataID;
  strcpy((*copyMatrix)->tableName, originalMatrix->tableName);
  (*copyMatrix)->dbSourceID = originalMatrix->dbSourceID;
  (*copyMatrix)->numRows    = originalMatrix->numRows;
  (*copyMatrix)->numCols    = originalMatrix->numCols;
  (*copyMatrix)->isView     = originalMatrix->isView;
  (*copyMatrix)->refCounter = originalMatrix->refCounter;

  (*copyMatrix)->sxp_type   = originalMatrix->sxp_type;
  (*copyMatrix)->sxp_obj    = originalMatrix->sxp_obj;
  (*copyMatrix)->sxp_named  = originalMatrix->sxp_named;
  (*copyMatrix)->sxp_gp     = originalMatrix->sxp_gp;
  (*copyMatrix)->sxp_mark   = originalMatrix->sxp_mark;
  (*copyMatrix)->sxp_debug  = originalMatrix->sxp_debug;
  (*copyMatrix)->sxp_trace  = originalMatrix->sxp_trace;
  (*copyMatrix)->sxp_spare  = originalMatrix->sxp_spare;
  (*copyMatrix)->sxp_gcgen  = originalMatrix->sxp_gcgen;
  (*copyMatrix)->sxp_gccls  = originalMatrix->sxp_gccls;
}


void clearRDBMatrix(rdbMatrix ** matrixInfo)
{
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
       initRDBMatrix(&((*arrayMatrixInfoObjects)[i]), 0, 1);

       int length = strlen(sqlRow[1]) + 1;
       ((*arrayMatrixInfoObjects)[i])->tableName = 
	                              (char *)malloc(length * sizeof(char));
       strcpy(((*arrayMatrixInfoObjects)[i])->tableName, sqlRow[1]);
       getRDBMatrixDimensions(sqlRow[3], &(((*arrayMatrixInfoObjects)[i])->numRows), 
			      &(((*arrayMatrixInfoObjects)[i])->numCols));

       ((*arrayMatrixInfoObjects)[i])->metadataID = atoi(sqlRow[0]);
       ((*arrayMatrixInfoObjects)[i])->dbSourceID = atoi(sqlRow[2]);
       ((*arrayMatrixInfoObjects)[i])->isView     = atoi(sqlRow[4]);
       ((*arrayMatrixInfoObjects)[i])->refCounter = atoi(sqlRow[5]);

       ((*arrayMatrixInfoObjects)[i])->sxp_type   = atoi(sqlRow[6]);
       ((*arrayMatrixInfoObjects)[i])->sxp_obj    = atoi(sqlRow[7]);
       ((*arrayMatrixInfoObjects)[i])->sxp_named  = atoi(sqlRow[8]);
       ((*arrayMatrixInfoObjects)[i])->sxp_gp     = atoi(sqlRow[9]);
       ((*arrayMatrixInfoObjects)[i])->sxp_mark   = atoi(sqlRow[10]);
       ((*arrayMatrixInfoObjects)[i])->sxp_debug  = atoi(sqlRow[11]);
       ((*arrayMatrixInfoObjects)[i])->sxp_trace  = atoi(sqlRow[12]);
       ((*arrayMatrixInfoObjects)[i])->sxp_spare  = atoi(sqlRow[13]);
       ((*arrayMatrixInfoObjects)[i])->sxp_gcgen  = atoi(sqlRow[14]);
       ((*arrayMatrixInfoObjects)[i])->sxp_gccls  = atoi(sqlRow[15]);

       sqlRow = mysql_fetch_row(sqlRes);
       i++;
    }
  }

  mysql_free_result(sqlRes);

  return 1;
}


int loadRDBMatrix(MYSQL * sqlConn, rdbMatrix ** matrixInfo,
		  unsigned long int metadataID, int alloc)
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
     initRDBMatrix(matrixInfo, 0, alloc);

     int length = strlen(sqlRow[1]) + 1;
     (*matrixInfo)->tableName = (char *)malloc(length * sizeof(char));
     strcpy((*matrixInfo)->tableName, sqlRow[1]);
     getRDBMatrixDimensions(sqlRow[3], &((*matrixInfo)->numRows), 
			    &((*matrixInfo)->numCols));

     (*matrixInfo)->metadataID = atoi(sqlRow[0]);
     (*matrixInfo)->dbSourceID = atoi(sqlRow[2]);
     (*matrixInfo)->isView     = atoi(sqlRow[4]);
     (*matrixInfo)->refCounter = atoi(sqlRow[5]);

     (*matrixInfo)->sxp_type   = atoi(sqlRow[6]);
     (*matrixInfo)->sxp_obj    = atoi(sqlRow[7]);
     (*matrixInfo)->sxp_named  = atoi(sqlRow[8]);
     (*matrixInfo)->sxp_gp     = atoi(sqlRow[9]);
     (*matrixInfo)->sxp_mark   = atoi(sqlRow[10]);
     (*matrixInfo)->sxp_debug  = atoi(sqlRow[11]);
     (*matrixInfo)->sxp_trace  = atoi(sqlRow[12]);
     (*matrixInfo)->sxp_spare  = atoi(sqlRow[13]);
     (*matrixInfo)->sxp_gcgen  = atoi(sqlRow[14]);
     (*matrixInfo)->sxp_gccls  = atoi(sqlRow[15]);

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

