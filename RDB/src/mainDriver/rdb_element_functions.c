#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <mysql.h>
#include "Rinternals.h"
#include "rdb_basics.h"
#include "rdb_element_functions.h"
#include "rdb_handle_vector_views.h"


/* ----------- Functions for Trigonometric functions --------------- */
int performNumericSin(MYSQL * sqlConn,rdbVector * dataVector,
		      rdbVector * resultVector)
{
  return internalPerformNumericTrig(sqlConn, dataVector, resultVector,
				    sqlTemplateNumericSIN);
}


int performNumericCos(MYSQL * sqlConn,rdbVector * dataVector,
		      rdbVector * resultVector)
{
  return internalPerformNumericTrig(sqlConn, dataVector, resultVector,
				    sqlTemplateNumericCOS);
}


int performComplexSin(MYSQL * sqlConn,rdbVector * dataVector,
		      rdbVector * resultVector)
{
  return internalPerformComplexTrig(sqlConn, dataVector, resultVector,
				    sqlTemplateComplexSIN);
}


int performComplexCos(MYSQL * sqlConn,rdbVector * dataVector,
		      rdbVector * resultVector)
{
  return internalPerformComplexTrig(sqlConn, dataVector, resultVector,
				    sqlTemplateComplexCOS);
}


/* ------------ Internal Method for all trig functions ------------- */
int internalPerformNumericTrig(MYSQL * sqlConn,rdbVector * dataVector,
			       rdbVector * resultVector, char * sqlTemplate)
{
  if(dataVector->sxp_type == SXP_TYPE_COMPLEX ||
     dataVector->sxp_type == SXP_TYPE_STRING)
    return 0;

  /* Build the sql string */
  int length = strlen(sqlTemplate) + strlen(dataVector->tableName) + 1;
  char strFunctionSQL[length];
  sprintf( strFunctionSQL, sqlTemplate, dataVector->tableName );

  /* Create the results (logic) view */
  initRDBVector(&resultVector, 1, 0);
  resultVector->size = dataVector->size;
  int success = createNewDoubleVectorView(sqlConn, resultVector, strFunctionSQL);

  if( success )
     createViewReferences(sqlConn, resultVector, dataVector, dataVector);
  else
     resultVector->size = 0;

  return success;
}


int internalPerformComplexTrig(MYSQL * sqlConn,rdbVector * dataVector,
			       rdbVector * resultVector, char * sqlTemplate)
{
  if(dataVector->sxp_type != SXP_TYPE_COMPLEX)
    return 0;

  /* Build the sql string */
  int length = strlen(sqlTemplate) + strlen(dataVector->tableName) + 1;
  char strFunctionSQL[length];
  sprintf( strFunctionSQL, sqlTemplate, dataVector->tableName );

  /* Create the results (logic) view */
  initRDBVector(&resultVector, 1, 0);
  resultVector->size = dataVector->size;
  int success = createNewComplexVectorView(sqlConn,resultVector,strFunctionSQL);

  if( success )
     createViewReferences(sqlConn, resultVector, dataVector, dataVector);
  else
     resultVector->size = 0;

  return success;
}


