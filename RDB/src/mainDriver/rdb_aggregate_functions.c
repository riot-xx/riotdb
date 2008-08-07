#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <mysql.h>
#include "Rinternals.h"
#include "rdb_basics.h"
#include "rdb_aggregate_functions.h"


/* ------------------------- Functions for SUM --------------------------- */
int getIntegerVectorSum(MYSQL * sqlConn, rdbVector * dataVector, int * sum)
{
  if( internalExecQuery(sqlConn, sqlTemplateNumericSum, dataVector->tableName) )
  {
     return internalGetIntegerResult(sqlConn, sum);
  }
  
  return 0;
}


int getDoubleVectorSum (MYSQL * sqlConn, rdbVector * dataVector, double * sum)
{
  if( internalExecQuery(sqlConn, sqlTemplateNumericSum, dataVector->tableName) )
  {
     return internalGetDoubleResult(sqlConn, sum);
  }
  
  return 0;
}


int getComplexVectorSum(MYSQL * sqlConn, rdbVector * dataVector, Rcomplex * sum)
{
  if( internalExecQuery(sqlConn, sqlTemplateComplexSum, dataVector->tableName) )
  {
     return internalGetComplexResult(sqlConn, sum);
  }
  
  return 0;
}


/* -------------------------- Functions for AVERAGE ------------------------ */
int getIntegerVectorAvg(MYSQL * sqlConn, rdbVector * dataVector, double * avg)
{
  if( internalExecQuery(sqlConn, sqlTemplateNumericAvg, dataVector->tableName) )
  {
     return internalGetDoubleResult(sqlConn, avg);
  }
  
  return 0;
}


int getDoubleVectorAvg (MYSQL * sqlConn, rdbVector * dataVector, double * avg)
{
  if( internalExecQuery(sqlConn, sqlTemplateNumericAvg, dataVector->tableName) )
  {
     return internalGetDoubleResult(sqlConn, avg);
  }
  
  return 0;
}

int getComplexVectorAvg(MYSQL * sqlConn, rdbVector * dataVector, Rcomplex * avg)
{
  if( internalExecQuery(sqlConn, sqlTemplateComplexAvg, dataVector->tableName) )
  {
     return internalGetComplexResult(sqlConn, avg);
  }
  
  return 0;
}


/* --------------------------- Functions for MAX --------------------------- */
int getIntegerVectorMax(MYSQL * sqlConn, rdbVector * dataVector, int * max)
{
  if( internalExecQuery(sqlConn, sqlTemplateNumericMax, dataVector->tableName) )
  {
     return internalGetIntegerResult(sqlConn, max);
  }
  
  return 0;
}

int getDoubleVectorMax (MYSQL * sqlConn, rdbVector * dataVector, double * max)
{
  if( internalExecQuery(sqlConn, sqlTemplateNumericMax, dataVector->tableName) )
  {
     return internalGetDoubleResult(sqlConn, max);
  }
  
  return 0;
}

int getStringVectorMax (MYSQL * sqlConn, rdbVector * dataVector, char ** max)
{
  if( internalExecQuery(sqlConn, sqlTemplateNumericMax, dataVector->tableName) )
  {
     return internalGetStringResult(sqlConn, max);
  }
  
  return 0;
}


/* --------------------------- Functions for MIN --------------------------- */
int getIntegerVectorMin(MYSQL * sqlConn, rdbVector * dataVector, int * min)
{
  if( internalExecQuery(sqlConn, sqlTemplateNumericMin, dataVector->tableName) )
  {
     return internalGetIntegerResult(sqlConn, min);
  }
  
  return 0;
}

int getDoubleVectorMin (MYSQL * sqlConn, rdbVector * dataVector, double * min)
{
  if( internalExecQuery(sqlConn, sqlTemplateNumericMin, dataVector->tableName) )
  {
     return internalGetDoubleResult(sqlConn, min);
  }
  
  return 0;
}

int getStringVectorMin (MYSQL * sqlConn, rdbVector * dataVector, char ** min)
{
  if( internalExecQuery(sqlConn, sqlTemplateNumericMin, dataVector->tableName) )
  {
     return internalGetStringResult(sqlConn, min);
  }
  
  return 0;
}



/* -------------- Internal Method to execute the SQL queries --------------- */
int internalExecQuery(MYSQL * sqlConn, char * sqlTemplate, char * tableName)
{
  /* Build the sql string */
  int length = strlen(sqlTemplate) + strlen(tableName) + 1;
  char strExecSQL[length];
  sprintf( strExecSQL, sqlTemplate, tableName );

  /* Execute the query */
  int success = mysql_query(sqlConn, strExecSQL);
  return (success != 0) ? 0 : 1;
}


/* -------------- Internal Methods to get SQL results ---------------- */
int internalGetIntegerResult(MYSQL * sqlConn, int * result)
{
  int success = 0;
  MYSQL_RES *sqlRes;
  MYSQL_ROW sqlRow;
  sqlRes = mysql_use_result(sqlConn);

  if( (sqlRow = mysql_fetch_row(sqlRes)) != NULL )
  {
     if(sqlRow[0] != NULL)
     {
        *result = atoi(sqlRow[0]);
        success = 1;
     }
  }

  while( (sqlRow = mysql_fetch_row(sqlRes)) != NULL );
  mysql_free_result(sqlRes);

  return success;
}


int internalGetDoubleResult (MYSQL * sqlConn, double * result)
{
  int success = 0;
  MYSQL_RES *sqlRes;
  MYSQL_ROW sqlRow;
  sqlRes = mysql_use_result(sqlConn);

  if( (sqlRow = mysql_fetch_row(sqlRes)) != NULL )
  {
     if(sqlRow[0] != NULL)
     {
        *result = strtod(sqlRow[0], NULL);
        success = 1;
     }
  }

  while( (sqlRow = mysql_fetch_row(sqlRes)) != NULL );
  mysql_free_result(sqlRes);

  return success;
}


int internalGetComplexResult(MYSQL * sqlConn, Rcomplex * result)
{
  int success = 0;
  MYSQL_RES *sqlRes;
  MYSQL_ROW sqlRow;
  sqlRes = mysql_use_result(sqlConn);

  if( (sqlRow = mysql_fetch_row(sqlRes)) != NULL )
  {
     if(sqlRow[0] != NULL && sqlRow[1] != NULL)
     {
        result->r = strtod(sqlRow[0], NULL);
        result->i = strtod(sqlRow[1], NULL);
        success = 1;
     }
  }

  while( (sqlRow = mysql_fetch_row(sqlRes)) != NULL );
  mysql_free_result(sqlRes);

  return success;
}


int internalGetStringResult (MYSQL * sqlConn, char ** result)
{
  int success = 0;
  MYSQL_RES *sqlRes;
  MYSQL_ROW sqlRow;
  sqlRes = mysql_use_result(sqlConn);

  if( (sqlRow = mysql_fetch_row(sqlRes)) != NULL )
  {
     if(sqlRow[0] != NULL)
     {
        *result = (char*) malloc( (1+strlen(sqlRow[0])) * sizeof(char) );
	strcpy(*result, sqlRow[0]);
        success = 1;
     }
  }

  while( (sqlRow = mysql_fetch_row(sqlRes)) != NULL );
  mysql_free_result(sqlRes);

  return success;
}

