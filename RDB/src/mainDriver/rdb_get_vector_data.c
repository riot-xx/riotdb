#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <mysql.h>
#include "Rinternals.h"
#include "rdb_basics.h"
#include "rdb_get_vector_data.h"
#include "rdb_handle_vectors.h"
#include "rdb_handle_vector_views.h"
#include "rdb_insert_vector_data.h"



/* --------- Functions to access vector tables by element -------------- */

int getIntElement(MYSQL * sqlConn, rdbVector * vectorInfo, 
		  unsigned int index, int * value, char * byte)
{
  /* Execute the query to get element */
  if( !execGetElementSQLCall(sqlConn, vectorInfo->tableName, index) )
    return 0;

  /* Get the value */
  MYSQL_RES *sqlRes;
  MYSQL_ROW sqlRow;
  sqlRes = mysql_use_result(sqlConn);

  if( (sqlRow = mysql_fetch_row(sqlRes)) != NULL )
  {
    if(sqlRow[1] != NULL)
    {
      *value = atoi(sqlRow[1]);
      *byte = 1;
    }
  }

  while( (sqlRow = mysql_fetch_row(sqlRes)) != NULL );
  mysql_free_result(sqlRes);

  return 1;
}


int getDoubleElement(MYSQL * sqlConn, rdbVector * vectorInfo, 
		     unsigned int index, double * value, char * byte)
{
  /* Execute the query to get element */
  if( !execGetElementSQLCall(sqlConn, vectorInfo->tableName, index) )
    return 0;

  /* Get the value */
  MYSQL_RES *sqlRes;
  MYSQL_ROW sqlRow;
  sqlRes = mysql_use_result(sqlConn);

  if( (sqlRow = mysql_fetch_row(sqlRes)) != NULL )
  {
    if(sqlRow[1] != NULL)
    {
      *value = strtod(sqlRow[1], NULL);
      *byte = 1;
    }
  }

  while( (sqlRow = mysql_fetch_row(sqlRes)) != NULL );
  mysql_free_result(sqlRes);

  return 1;
}


int getStringElement(MYSQL * sqlConn, rdbVector * vectorInfo, 
		     unsigned int index, char ** value, char * byte)
{
  /* Execute the query to get element */
  if( !execGetElementSQLCall(sqlConn, vectorInfo->tableName, index) )
    return 0;

  /* Get the value */
  MYSQL_RES *sqlRes;
  MYSQL_ROW sqlRow;
  sqlRes = mysql_use_result(sqlConn);

  if( (sqlRow = mysql_fetch_row(sqlRes)) != NULL )
  {
    if(sqlRow[1] != NULL)
    {
      *value = (char*) malloc( (1+strlen(sqlRow[1])) * sizeof(char) );
      strcpy(*value, sqlRow[1]);
      *byte = 1;
    }
  }

  while( (sqlRow = mysql_fetch_row(sqlRes)) != NULL );
  mysql_free_result(sqlRes);

  return 1;
}


int getComplexElement(MYSQL * sqlConn, rdbVector * vectorInfo, 
		      unsigned int index, Rcomplex * value, char * byte)
{
  /* Execute the query to get element */
  if( !execGetElementSQLCall(sqlConn, vectorInfo->tableName, index) )
    return 0;

  /* Get the value */
  MYSQL_RES *sqlRes;
  MYSQL_ROW sqlRow;
  sqlRes = mysql_use_result(sqlConn);

  if( (sqlRow = mysql_fetch_row(sqlRes)) != NULL )
  {
    if(sqlRow[1] != NULL && sqlRow[2] != NULL)
    {
      value->r = strtod(sqlRow[1], NULL);
      value->i = strtod(sqlRow[2], NULL);
      *byte = 1;
    }
  }

  while( (sqlRow = mysql_fetch_row(sqlRes)) != NULL );
  mysql_free_result(sqlRes);

  return 1;
}


int getLogicElement(MYSQL * sqlConn, rdbVector * vectorInfo, 
		    unsigned int index, int * value, char * byte)
{
  /* Execute the query to get element */
  if( !execGetElementSQLCall(sqlConn, vectorInfo->tableName, index) )
    return 0;

  /* Get the value */
  MYSQL_RES *sqlRes;
  MYSQL_ROW sqlRow;
  sqlRes = mysql_use_result(sqlConn);

  if( (sqlRow = mysql_fetch_row(sqlRes)) != NULL )
  {
    if(sqlRow[1] != NULL)
    {
      *value = atoi(sqlRow[1]);
      *byte = 1;
    }
  }

  while( (sqlRow = mysql_fetch_row(sqlRes)) != NULL );
  mysql_free_result(sqlRes);

  return 1;
}


int execGetElementSQLCall(MYSQL * sqlConn, char* tableName, unsigned int index)
{
  /* Build the sql string */
  int length = strlen(sqlTemplateGetSingleValue) + strlen(tableName) + 
               MAX_INT_LENGTH + 1;
  char strGetElem[length];
  sprintf( strGetElem, sqlTemplateGetSingleValue, tableName, index );

  /* Execute the query */
  int success = mysql_query(sqlConn, strGetElem);
  return (success != 0) ? 0 : 1;
}


/* ------------------------------------------------------------------ */
/* ----- Functions to access vector tables and get all elements ----- */

int getAllIntElements(MYSQL * sqlConn, rdbVector * vectorInfo, 
		      int values[], char byteArray[])
{
  /* Execute the query to get element */
  if( !execGetAllElementsSQLCall(sqlConn, vectorInfo->tableName) )
    return 0;

  /* Get the query results */
  MYSQL_RES *sqlRes;
  MYSQL_ROW sqlRow;
  sqlRes = mysql_use_result(sqlConn);
  int index;

  while( (sqlRow = mysql_fetch_row(sqlRes)) != NULL )
  {
    if(sqlRow[1] != NULL)
    {
      index = atoi(sqlRow[0])-1;
      values[index] = atoi(sqlRow[1]);
      byteArray[index] = 1;
    }
  }

  mysql_free_result(sqlRes);

  return 1;
}


int getAllDoubleElements(MYSQL * sqlConn, rdbVector * vectorInfo,
			 double values[], char byteArray[])
{
  /* Execute the query to get element */
  if( !execGetAllElementsSQLCall(sqlConn, vectorInfo->tableName) )
    return 0;

  /* Get the query results */
  MYSQL_RES *sqlRes;
  MYSQL_ROW sqlRow;
  sqlRes = mysql_use_result(sqlConn);
  int index;

  while( (sqlRow = mysql_fetch_row(sqlRes)) != NULL )
  {
    if(sqlRow[1] != NULL)
    {
      index = atoi(sqlRow[0])-1;
      values[index] = strtod(sqlRow[1], NULL);
      byteArray[index] = 1;
    }
  }

  mysql_free_result(sqlRes);

  return 1;
}


int getAllStringElements(MYSQL * sqlConn, rdbVector * vectorInfo,
			 char* values[], char byteArray[])
{
  /* Execute the query to get element */
  if( !execGetAllElementsSQLCall(sqlConn, vectorInfo->tableName) )
    return 0;

  /* Get the query results */
  MYSQL_RES *sqlRes;
  MYSQL_ROW sqlRow;
  sqlRes = mysql_use_result(sqlConn);
  int index;

  while( (sqlRow = mysql_fetch_row(sqlRes)) != NULL )
  {
    if(sqlRow[1] != NULL)
    {
      index = atoi(sqlRow[0]) - 1;
      values[index] = (char*) malloc( (1+strlen(sqlRow[1])) * sizeof(char) );
      strcpy(values[index], sqlRow[1]);
      byteArray[index] = 1;
    }
  }

  mysql_free_result(sqlRes);

  return 1;
}


int getAllComplexElements(MYSQL * sqlConn, rdbVector * vectorInfo,
			  Rcomplex values[], char byteArray[])
{
  /* Execute the query to get element */
  if( !execGetAllElementsSQLCall(sqlConn, vectorInfo->tableName) )
    return 0;

  /* Get the query results */
  MYSQL_RES *sqlRes;
  MYSQL_ROW sqlRow;
  sqlRes = mysql_use_result(sqlConn);
  int index;

  while( (sqlRow = mysql_fetch_row(sqlRes)) != NULL )
  {
    if(sqlRow[1] != NULL && sqlRow[2] != NULL)
    {
      index = atoi(sqlRow[0])-1;
      values[index].r = strtod(sqlRow[1], NULL);
      values[index].i = strtod(sqlRow[2], NULL);
      byteArray[index] = 1;
    }
  }

  mysql_free_result(sqlRes);

  return 1;
}


int getAllLogicElements(MYSQL * sqlConn, rdbVector * vectorInfo, 
			int values[], char byteArray[])
{
  /* Execute the query to get element */
  if( !execGetAllElementsSQLCall(sqlConn, vectorInfo->tableName) )
    return 0;

  /* Get the query results */
  MYSQL_RES *sqlRes;
  MYSQL_ROW sqlRow;
  sqlRes = mysql_use_result(sqlConn);
  int index;

  while( (sqlRow = mysql_fetch_row(sqlRes)) != NULL )
  {
    if(sqlRow[1] != NULL)
    {
      index = atoi(sqlRow[0])-1;
      values[index] = atoi(sqlRow[1]);
      byteArray[index] = 1;
    }
  }

  mysql_free_result(sqlRes);

  return 1;
}


int execGetAllElementsSQLCall(MYSQL * sqlConn, char * tableName)
{
  /* Build the sql string */
  int length = strlen(sqlTemplateGetAllValues) + strlen(tableName) + 1;
  char strGetElems[length];
  sprintf( strGetElems, sqlTemplateGetAllValues, tableName);

  /* Execute the query */
  int success = mysql_query(sqlConn, strGetElems);
  return (success != 0) ? 0 : 1;
}


/* ------------------------------------------------------------------ */
/* -------- Functions to access vector tables with ranges -------- */

int getRangeIntElements(MYSQL * sqlConn, rdbVector * vectorInfo, 
			unsigned int greaterThan, unsigned int lessThan, 
			int values[], char byteArray[])
{
  /* Execute the query to get element */
  int success = execGetRangeElementsSQLCall(sqlConn, vectorInfo->tableName,
					    greaterThan, lessThan);
  if( !success )
    return 0;

  /* Get the query results */
  MYSQL_RES *sqlRes;
  MYSQL_ROW sqlRow;
  sqlRes = mysql_use_result(sqlConn);
  int index;

  while( (sqlRow = mysql_fetch_row(sqlRes)) != NULL )
  {
    if(sqlRow[1] != NULL)
    {
      index = atoi(sqlRow[0]) - greaterThan;
      values[index] = atoi(sqlRow[1]);
      byteArray[index] = 1;
    }
  }

  mysql_free_result(sqlRes);

  return 1;
}

int getRangeDoubleElements(MYSQL * sqlConn, rdbVector * vectorInfo, 
			   unsigned int greaterThan, unsigned int lessThan, 
			   double values[], char byteArray[])
{
  /* Execute the query to get element */
  int success = execGetRangeElementsSQLCall(sqlConn, vectorInfo->tableName,
					    greaterThan, lessThan);
  if( !success )
    return 0;

  /* Get the query results */
  MYSQL_RES *sqlRes;
  MYSQL_ROW sqlRow;
  sqlRes = mysql_use_result(sqlConn);
  int index;

  while( (sqlRow = mysql_fetch_row(sqlRes)) != NULL )
  {
    if(sqlRow[1] != NULL)
    {
      index = atoi(sqlRow[0]) - greaterThan;
      values[index] = strtod(sqlRow[1], NULL);
      byteArray[index] = 1;
    }
  }

  mysql_free_result(sqlRes);

  return 1;
}


int getRangeStringElements(MYSQL * sqlConn, rdbVector * vectorInfo, 
			   unsigned int greaterThan, unsigned int lessThan, 
			   char * values[], char byteArray[])
{
  /* Execute the query to get element */
  int success = execGetRangeElementsSQLCall(sqlConn, vectorInfo->tableName,
					    greaterThan, lessThan);
  if( !success )
    return 0;

  /* Get the query results */
  MYSQL_RES *sqlRes;
  MYSQL_ROW sqlRow;
  sqlRes = mysql_use_result(sqlConn);
  int index;

  while( (sqlRow = mysql_fetch_row(sqlRes)) != NULL )
  {
    if(sqlRow[1] != NULL)
    {
      index = atoi(sqlRow[0]) - greaterThan;
      values[index] = (char*) malloc( (1+strlen(sqlRow[1])) * sizeof(char) );
      strcpy(values[index], sqlRow[1]);
      byteArray[index] = 1;
    }
  }

  mysql_free_result(sqlRes);

  return 1;
}


int getRangeComplexElements(MYSQL * sqlConn, rdbVector * vectorInfo, 
			    unsigned int greaterThan, unsigned int lessThan, 
			    Rcomplex values[], char byteArray[])
{
  /* Execute the query to get element */
  int success = execGetRangeElementsSQLCall(sqlConn, vectorInfo->tableName,
					    greaterThan, lessThan);
  if( !success )
    return 0;

  /* Get the query results */
  MYSQL_RES *sqlRes;
  MYSQL_ROW sqlRow;
  sqlRes = mysql_use_result(sqlConn);
  int index;

  while( (sqlRow = mysql_fetch_row(sqlRes)) != NULL )
  {
    if(sqlRow[1] != NULL && sqlRow[2] != NULL)
    {
      index = atoi(sqlRow[0]) - greaterThan;
      values[index].r = strtod(sqlRow[1], NULL);
      values[index].i = strtod(sqlRow[2], NULL);
      byteArray[index] = 1;
    }
  }

  mysql_free_result(sqlRes);

  return 1;
}


int getRangeLogicElements(MYSQL * sqlConn, rdbVector * vectorInfo, 
			  unsigned int greaterThan, unsigned int lessThan, 
			  int values[], char byteArray[])
{
  /* Execute the query to get element */
  int success = execGetRangeElementsSQLCall(sqlConn, vectorInfo->tableName,
					    greaterThan, lessThan);
  if( !success )
    return 0;

  /* Get the query results */
  MYSQL_RES *sqlRes;
  MYSQL_ROW sqlRow;
  sqlRes = mysql_use_result(sqlConn);
  int index;

  while( (sqlRow = mysql_fetch_row(sqlRes)) != NULL )
  {
    if(sqlRow[1] != NULL)
    {
      index = atoi(sqlRow[0]) - greaterThan;
      values[index] = atoi(sqlRow[1]);
      byteArray[index] = 1;
    }
  }

  mysql_free_result(sqlRes);

  return 1;
}


int execGetRangeElementsSQLCall(MYSQL * sqlConn, char * tableName,
			     unsigned int greaterThan, unsigned int lessThan)
{
  /* Build the sql string */
  int length = strlen(sqlTemplateGetRangeValues) + strlen(tableName) + 
               2*MAX_INT_LENGTH + 1;
  char strGetElems[length];
  sprintf( strGetElems, sqlTemplateGetRangeValues, tableName,
	   greaterThan, lessThan );

  /* Execute the query */
  int success = mysql_query(sqlConn, strGetElems);
  return (success != 0) ? 0 : 1;
}



/* ------------------------------------------------------------------ */
/* ---- Functions to access vector tables and get some elements ----- */

int getSparseIntElements(MYSQL * sqlConn, rdbVector * vectorInfo,
			 unsigned int indexes[], int size, 
			 int values[], char byteArray[])
{
  /* Some error checking */
  if( size < 1 )
     return 0;

  /* Execute the sql query */
  int success = execGetSparseElementsSQLCall(sqlConn, vectorInfo->tableName,
					     indexes, size);
  if( !success )
    return 0;

  /* Get the query results */
  MYSQL_RES *sqlRes;
  MYSQL_ROW sqlRow;
  sqlRes = mysql_use_result(sqlConn);
  int vIndex, i;

  while( (sqlRow = mysql_fetch_row(sqlRes)) != NULL )
  {
     vIndex = atoi(sqlRow[0]);
     for( i = 0 ; i < size ; i++ )
     {
        if( vIndex == indexes[i] && sqlRow[1] != NULL )
        {
	   values[i] = atoi(sqlRow[1]);
	   byteArray[i] = 1;
	}
     }
  }

  mysql_free_result(sqlRes);

  return 1;
}


int getSparseDoubleElements(MYSQL * sqlConn, rdbVector * vectorInfo,
			    unsigned int indexes[], int size,
			    double values[], char byteArray[])
{
  /* Some error checking */
  if( size < 1 )
     return 0;

  /* Execute the sql query */
  int success = execGetSparseElementsSQLCall(sqlConn, vectorInfo->tableName,
					     indexes, size);
  if( !success )
    return 0;

  /* Get the query results */
  MYSQL_RES *sqlRes;
  MYSQL_ROW sqlRow;
  sqlRes = mysql_use_result(sqlConn);
  int vIndex, i;

  while( (sqlRow = mysql_fetch_row(sqlRes)) != NULL )
  {
     vIndex = atoi(sqlRow[0]);
     for( i = 0 ; i < size ; i++ )
     {
        if( vIndex == indexes[i] && sqlRow[1] != NULL )
        {
	   values[i] =  strtod(sqlRow[1], NULL);
	   byteArray[i] = 1;
	}
     }
  }

  mysql_free_result(sqlRes);

  return 1;
}


int getSparseStringElements(MYSQL * sqlConn, rdbVector * vectorInfo, 
			    unsigned int indexes[], int size,
			    char * values[], char byteArray[])
{
  /* Some error checking */
  if( size < 1 )
     return 0;

  /* Execute the sql query */
  int success = execGetSparseElementsSQLCall(sqlConn, vectorInfo->tableName,
					     indexes, size);
  if( !success )
    return 0;

  /* Get the query results */
  MYSQL_RES *sqlRes;
  MYSQL_ROW sqlRow;
  sqlRes = mysql_use_result(sqlConn);
  int vIndex, i;

  while( (sqlRow = mysql_fetch_row(sqlRes)) != NULL )
  {
     vIndex = atoi(sqlRow[0]);
     for( i = 0 ; i < size ; i++ )
     {
        if( vIndex == indexes[i] && sqlRow[1] != NULL )
        {
 	   values[i] = (char*) malloc( (1+strlen(sqlRow[1])) * sizeof(char) );
	   strcpy(values[i], sqlRow[1]);
	   byteArray[i] = 1;
	}
     }
  }

  mysql_free_result(sqlRes);

  return 1;
}


int getSparseComplexElements(MYSQL * sqlConn, rdbVector * vectorInfo, 
			     unsigned int indexes[], int size, 
			     Rcomplex values[], char byteArray[])
{
  /* Some error checking */
  if( size < 1 )
     return 0;

  /* Execute the sql query */
  int success = execGetSparseElementsSQLCall(sqlConn, vectorInfo->tableName,
					     indexes, size);
  if( !success )
    return 0;

  /* Get the query results */
  MYSQL_RES *sqlRes;
  MYSQL_ROW sqlRow;
  sqlRes = mysql_use_result(sqlConn);
  int vIndex, i;

  while( (sqlRow = mysql_fetch_row(sqlRes)) != NULL )
  {
     vIndex = atoi(sqlRow[0]);
     for( i = 0 ; i < size ; i++ )
     {
        if( vIndex == indexes[i] && sqlRow[1] != NULL && sqlRow[2] != NULL )
        {
	   values[i].r = strtod(sqlRow[1], NULL);
	   values[i].i = strtod(sqlRow[2], NULL);
	   byteArray[i] = 1;
	}
     }
  }

  mysql_free_result(sqlRes);

  return 1;
}


int getSparseLogicElements(MYSQL * sqlConn, rdbVector * vectorInfo,
			   unsigned int indexes[], int size, 
			   int values[], char byteArray[])
{
  /* Some error checking */
  if( size < 1 )
     return 0;

  /* Execute the sql query */
  int success = execGetSparseElementsSQLCall(sqlConn, vectorInfo->tableName,
					     indexes, size);
  if( !success )
    return 0;

  /* Get the query results */
  MYSQL_RES *sqlRes;
  MYSQL_ROW sqlRow;
  sqlRes = mysql_use_result(sqlConn);
  int vIndex, i;

  while( (sqlRow = mysql_fetch_row(sqlRes)) != NULL )
  {
     vIndex = atoi(sqlRow[0]);
     for( i = 0 ; i < size ; i++ )
     {
        if( vIndex == indexes[i] && sqlRow[1] != NULL )
        {
	   values[i] = atoi(sqlRow[1]);
	   byteArray[i] = 1;
	}
     }
  }

  mysql_free_result(sqlRes);

  return 1;
}


int execGetSparseElementsSQLCall(MYSQL * sqlConn, char * tableName, 
				 unsigned int indexes[], int size)
{
  /* Initialize necessary strings */
  int i;
  int stringSize = size * (MAX_INT_LENGTH + strlen(sqlTemplateVIndexOR)) + 1;
  char * strIndexes = (char *)malloc( stringSize * sizeof(char) );
  char temp[MAX_INT_LENGTH + strlen(sqlTemplateVIndexOR)];
  strIndexes[0] = '\0';

  /* Build string of the form: "vIndex = n OR vIndex = n ..." */
  for( i = 0 ; i < size - 1 ; i++ )
  {
     sprintf(temp, sqlTemplateVIndexOR, indexes[i]);
     strcat(strIndexes, temp);
  }
  
  sprintf(temp, sqlTemplateVIndex, indexes[i]);
  strcat(strIndexes, temp);

  /* Build the sql string */
  int length = strlen(sqlTemplateGetSparseValues) + strlen(tableName) + 
               strlen(strIndexes) + 1;
  char strGetElems[length];
  sprintf( strGetElems, sqlTemplateGetSparseValues, tableName, strIndexes);
  free(strIndexes);

  /* Execute the query */
  int success = mysql_query(sqlConn, strGetElems);
  return (success != 0) ? 0 : 1;
}


/* ---------- Functions to access vector tables by DBVectors ------------ */
int getIntElementsWithDBVector(MYSQL * sqlConn, rdbVector * dataVector, 
			       rdbVector * indexVector, rdbVector * resultVector)
{
  if( indexVector->sxp_type != SXP_TYPE_INTEGER &&
      indexVector->sxp_type != SXP_TYPE_DOUBLE )
    return 0;

  /* Create the sql string for the results view */
  char * strGetElems;
  buildGetElemWithDBVectorSQL(&strGetElems, sqlTemplateGetValuesWithDBVec,
			     dataVector, indexVector);

  /* Create the results view */
  initRDBVector(&resultVector, 1, 0);
  resultVector->size = indexVector->size;
  int success = createNewIntegerVectorView(sqlConn, resultVector, strGetElems);

  if( success )
     createViewReferences(sqlConn, resultVector, dataVector, indexVector);
  else
     resultVector->size = 0;

  free(strGetElems);

  return success;
}


int getDoubleElementsWithDBVector(MYSQL * sqlConn, rdbVector * dataVector, 
				  rdbVector * indexVector,
				  rdbVector * resultVector)
{
  if( indexVector->sxp_type != SXP_TYPE_INTEGER &&
      indexVector->sxp_type != SXP_TYPE_DOUBLE )
    return 0;

  /* Create the sql string for the results view */
  char * strGetElems;
  buildGetElemWithDBVectorSQL(&strGetElems, sqlTemplateGetValuesWithDBVec,
			     dataVector, indexVector);

  /* Create the results view */
  initRDBVector(&resultVector, 1, 0);
  resultVector->size = indexVector->size;
  int success = createNewDoubleVectorView(sqlConn, resultVector, strGetElems);

  if( success )
     createViewReferences(sqlConn, resultVector, dataVector, indexVector);
  else
     resultVector->size = 0;

  free(strGetElems);

  return success;
}


int getStringElementsWithDBVector(MYSQL * sqlConn, rdbVector * dataVector, 
				  rdbVector * indexVector, 
				  rdbVector * resultVector)
{
  if( indexVector->sxp_type != SXP_TYPE_INTEGER &&
      indexVector->sxp_type != SXP_TYPE_DOUBLE )
    return 0;

  /* Create the sql string for the results view */
  char * strGetElems;
  buildGetElemWithDBVectorSQL(&strGetElems, sqlTemplateGetValuesWithDBVec,
			     dataVector, indexVector);

  /* Create the results view */
  initRDBVector(&resultVector, 1, 0);
  resultVector->size = indexVector->size;
  int success = createNewStringVectorView(sqlConn, resultVector, strGetElems);

  if( success )
     createViewReferences(sqlConn, resultVector, dataVector, indexVector);
  else
     resultVector->size = 0;

  free(strGetElems);

  return success;
}


int getComplexElementsWithDBVector(MYSQL * sqlConn, rdbVector * dataVector,
				   rdbVector * indexVector, 
				   rdbVector * resultVector)
{
  if( indexVector->sxp_type != SXP_TYPE_INTEGER &&
      indexVector->sxp_type != SXP_TYPE_DOUBLE )
    return 0;

  /* Create the sql string for the results view */
  char * strGetElems;
  buildGetElemWithDBVectorSQL(&strGetElems, sqlTemplateGetCplxValuesWithDBVec,
			     dataVector, indexVector);

  /* Create the results view */
  initRDBVector(&resultVector, 1, 0);
  resultVector->size = indexVector->size;
  int success = createNewComplexVectorView(sqlConn, resultVector, strGetElems);

  if( success )
     createViewReferences(sqlConn, resultVector, dataVector, indexVector);
  else
     resultVector->size = 0;

  free(strGetElems);

  return success;
}


int getLogicElementsWithDBVector(MYSQL * sqlConn, rdbVector * dataVector, 
				 rdbVector * indexVector, 
				 rdbVector * resultVector)
{
  if( indexVector->sxp_type != SXP_TYPE_INTEGER &&
      indexVector->sxp_type != SXP_TYPE_DOUBLE )
    return 0;

  /* Create the sql string for the results view */
  char * strGetElems;
  buildGetElemWithDBVectorSQL(&strGetElems, sqlTemplateGetValuesWithDBVec,
			     dataVector, indexVector);

  /* Create the results view */
  initRDBVector(&resultVector, 1, 0);
  resultVector->size = indexVector->size;
  int success = createNewLogicVectorView(sqlConn, resultVector, strGetElems);

  if( success )
     createViewReferences(sqlConn, resultVector, dataVector, indexVector);
  else
     resultVector->size = 0;

  free(strGetElems);

  return success;
}


void buildGetElemWithDBVectorSQL(char ** strGetElems, char * sqlTemplate,
				rdbVector* dataVector, rdbVector* indexVector)
{
  int length = strlen(sqlTemplate) + strlen(dataVector->tableName) + 
               strlen(indexVector->tableName) + 1;
  *strGetElems = (char*) malloc( length * sizeof(char) );
  sprintf( *strGetElems, sqlTemplate, dataVector->tableName,
	   indexVector->tableName );
}


/* ------ Functions to access vector tables by Logic DBVectors -------- */
int getIntElementsWithLogic(MYSQL * sqlConn, rdbVector * dataVector, 
			    rdbVector * logicVector, rdbVector * resultVector)
{
  return internalGetElementsWithLogic(sqlConn, dataVector, 
				      logicVector, resultVector, 
				      sqlTemplateGetValuesWithLogic,
				      sqlTemplateTransformResults);
}


int getDoubleElementsWithLogic(MYSQL * sqlConn, rdbVector * dataVector, 
			       rdbVector * logicVector, rdbVector * resultVector)
{
  return internalGetElementsWithLogic(sqlConn, dataVector, 
				      logicVector, resultVector, 
				      sqlTemplateGetValuesWithLogic,
				      sqlTemplateTransformResults);
}


int getStringElementsWithLogic(MYSQL * sqlConn, rdbVector * dataVector, 
			       rdbVector * logicVector, rdbVector * resultVector)
{
  return internalGetElementsWithLogic(sqlConn, dataVector, 
				      logicVector, resultVector, 
				      sqlTemplateGetValuesWithLogic,
				      sqlTemplateTransformResults);
}


int getComplexElementsWithLogic(MYSQL * sqlConn, rdbVector * dataVector,
				rdbVector * logicVector, rdbVector * resultVector)
{
  return internalGetElementsWithLogic(sqlConn, dataVector, 
				      logicVector, resultVector, 
				      sqlTemplateGetCplxValuesWithLogic,
				      sqlTemplateTransformCplxResults);
}


int getLogicElementsWithLogic(MYSQL * sqlConn, rdbVector * dataVector, 
			      rdbVector * logicVector, rdbVector * resultVector)
{
  return internalGetElementsWithLogic(sqlConn, dataVector, 
				      logicVector, resultVector, 
				      sqlTemplateGetValuesWithLogic,
				      sqlTemplateTransformResults);
}


int internalGetElementsWithLogic(MYSQL * sqlConn, rdbVector * dataVector, 
			       rdbVector * logicVector, rdbVector * resultVector,
			       char * sqlTemplateTemp, char * sqlTemplateFinal)
{
  if( logicVector->sxp_type != SXP_TYPE_LOGIC )
    return 0;

  /* Create a view to hold the intermediate results */
  char * strResults;
  rdbVector * tempView;
  initRDBVector(&tempView, 1, 1);
  tempView->size = dataVector->size;
  buildGetElemWithLogicSQL(&strResults, sqlTemplateTemp, dataVector, logicVector);

  int success = 0;
  switch( dataVector->sxp_type )
  {
    case SXP_TYPE_INTEGER: 
      success = createNewIntegerVectorView(sqlConn, tempView, strResults);
      break;
    case SXP_TYPE_DOUBLE: 
      success = createNewDoubleVectorView(sqlConn, tempView, strResults);
      break;
    case SXP_TYPE_STRING: 
      success = createNewStringVectorView(sqlConn, tempView, strResults);
      break;
    case SXP_TYPE_COMPLEX: 
      success = createNewComplexVectorView(sqlConn, tempView, strResults);
      break;
    case SXP_TYPE_LOGIC: 
      success = createNewLogicVectorView(sqlConn, tempView, strResults);
      break;
  }

  if( success )
     createViewReferences(sqlConn, tempView, dataVector, logicVector);
  else {
     clearRDBVector(&tempView);
     free(strResults);
     return 0;
  }

  /* Create a view to hold the final results */
  char * strFinal;
  initRDBVector(&resultVector, 1, 0);
  buildTranformResultsSQL(&strFinal, sqlTemplateFinal, tempView, logicVector);

  success = 0;
  switch( dataVector->sxp_type )
  {
    case SXP_TYPE_INTEGER: 
      success = createNewIntegerVectorView(sqlConn, resultVector, strFinal);
      break;
    case SXP_TYPE_DOUBLE: 
      success = createNewDoubleVectorView(sqlConn, resultVector, strFinal);
      break;
    case SXP_TYPE_STRING: 
      success = createNewStringVectorView(sqlConn, resultVector, strFinal);
      break;
    case SXP_TYPE_COMPLEX: 
      success = createNewComplexVectorView(sqlConn, resultVector, strFinal);
      break;
    case SXP_TYPE_LOGIC: 
      success = createNewLogicVectorView(sqlConn, resultVector, strFinal);
      break;
  }
  
  /* Get the results from the view */
  if( success )
  {
     createViewReferences(sqlConn, resultVector, tempView, logicVector);

     int numResults = 0;
     success *= getLogicalTableSize(sqlConn, resultVector, &numResults);
     if( success )
       success *= updateSizeVectorTable(sqlConn, resultVector, numResults);
  }

  /* Clean up */
  deleteRDBVector(sqlConn, tempView);
  clearRDBVector(&tempView);
  free(strResults);
  free(strFinal);

  return success;

}

void buildGetElemWithLogicSQL(char ** strGetElems, char * sqlTemplate,
			     rdbVector* dataVector, rdbVector* logicVector)
{
  int size = logicVector->size;
  int length = strlen(sqlTemplate) + strlen(dataVector->tableName) + 
               strlen(logicVector->tableName) + 2*MAX_INT_LENGTH + 1;
  *strGetElems = (char*) malloc( length * sizeof(char) );

  sprintf( *strGetElems, sqlTemplate, dataVector->tableName,
	   logicVector->tableName, size, size );
}


void buildTranformResultsSQL(char ** strResults, char * sqlTemplate,
			     rdbVector* resultsVector, rdbVector* logicVector)
{
  int size = logicVector->size;
  int length = strlen(sqlTemplate) + strlen(resultsVector->tableName) + 
               2*strlen(logicVector->tableName) + 2*MAX_INT_LENGTH + 1;
  *strResults = (char*) malloc( length * sizeof(char) );

  sprintf( *strResults, sqlTemplate, size, logicVector->tableName,
	   logicVector->tableName, size, resultsVector->tableName);
}


