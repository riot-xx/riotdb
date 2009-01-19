/*****************************************************************************
 * Contains element-wise functions for vectors (sin, cos, pow, sqrt)
 *
 * Author: Herodotos Herodotou
 * Date:   Sep 17, 2008
 ****************************************************************************/

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <mysql.h>
#include "Rinternals.h"
#include "basics.h"
#include "vector_element_functions.h"
#include "handle_vector_views.h"


/* General functions */
int performNumericPow(MYSQL * sqlConn, rdbVector * result, 
		      rdbVector * input, double exponent)
{
  /* Both inputs must either be integers or doubles or logic */
  if( input->sxp_type != SXP_TYPE_INTEGER &&
      input->sxp_type != SXP_TYPE_DOUBLE &&
      input->sxp_type != SXP_TYPE_LOGIC )
    return 0;

  /* Build the sql string */
  int length = strlen(sqlTemplatePowFunction) + strlen(input->tableName) + 
	       MAX_DOUBLE_LENGTH + 1;
  char* sqlString = malloc(length * sizeof(char));
  sprintf(sqlString, sqlTemplatePowFunction, exponent, input->tableName);

 /* Create the results (logic) view */
  initRDBVector(&result, 1, 0);
  result->size = input->size;
  int success = 0;
  success = createNewDoubleVectorView(sqlConn, result, sqlString);
  free(sqlString);

  if( success )
     createVectorViewReferences(sqlConn, result, input, input);
  else
     result->size = 0;

  return success;
}

int performNumericSqrt(MYSQL * sqlConn, rdbVector * resultVector, 
		       rdbVector * dataVector)
{
  return internalPerformNumericFunction(sqlConn, dataVector, resultVector, SQRT_OP);
}

int performNumericSin(MYSQL * sqlConn,rdbVector * dataVector,
		      rdbVector * resultVector)
{
  return internalPerformNumericFunction(sqlConn, dataVector, resultVector, SIN_OP);
}


int performNumericCos(MYSQL * sqlConn,rdbVector * dataVector,
		      rdbVector * resultVector)
{
  return internalPerformNumericFunction(sqlConn, dataVector, resultVector, COS_OP);
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


/* ------------ Internal Method for numeric functions ------------- */
int internalPerformNumericFunction(MYSQL * sqlConn, rdbVector * dataVector,
			           rdbVector * resultVector, char * function)
{
  if(dataVector->sxp_type == SXP_TYPE_COMPLEX ||
     dataVector->sxp_type == SXP_TYPE_STRING)
    return 0;

  /* Build the sql string */
  int length = strlen(sqlTemplateUnaryFunction) + strlen(dataVector->tableName) + 1;
  char strFunctionSQL[length];
  sprintf( strFunctionSQL, sqlTemplateUnaryFunction, function, dataVector->tableName );

  /* Create the results (logic) view */
  initRDBVector(&resultVector, 1, 0);
  resultVector->size = dataVector->size;
  int success = createNewDoubleVectorView(sqlConn, resultVector, strFunctionSQL);

  if( success )
     createVectorViewReferences(sqlConn, resultVector, dataVector, dataVector);
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
     createVectorViewReferences(sqlConn, resultVector, dataVector, dataVector);
  else
     resultVector->size = 0;

  return success;
}


