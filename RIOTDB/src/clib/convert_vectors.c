/*****************************************************************************
 * Contains functions for performing data type conversions for vectors
 *
 * Author: Herodotos Herodotou
 * Date:   Sep 17, 2008
 ****************************************************************************/

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <mysql.h>
#include "basics.h"
#include "convert_vectors.h"
#include "handle_vector_views.h"


/* ------------- Functions for data type convertions ------------------ */

int convertNumericToComplex(MYSQL * sqlConn, rdbVector * numericVector,
			    rdbVector * complexVector)
{
  return internalConvertVectors(sqlConn, numericVector, complexVector,
				sqlTemplateNumericToComplex, SXP_TYPE_COMPLEX);
}


int convertDoubleToInteger(MYSQL * sqlConn, rdbVector * doubleVector,
			   rdbVector * integerVector)
{
  return internalConvertVectors(sqlConn, doubleVector, integerVector,
				 sqlTemplateDoubleToInteger, SXP_TYPE_INTEGER);
}


int convertIntegerToDouble(MYSQL * sqlConn, rdbVector * integerVector,
			   rdbVector * doubleVector)
{
  return internalConvertVectors(sqlConn, integerVector, doubleVector,
				sqlTemplateGenericConvert, SXP_TYPE_DOUBLE);
}


int convertLogicToInteger(MYSQL * sqlConn, rdbVector * logicVector,
			  rdbVector * integerVector)
{
  return internalConvertVectors(sqlConn, logicVector, integerVector,
				 sqlTemplateGenericConvert, SXP_TYPE_INTEGER);
}


int convertLogicToDouble(MYSQL * sqlConn, rdbVector * logicVector,
			 rdbVector * doubleVector)
{
  return internalConvertVectors(sqlConn, logicVector, doubleVector,
				sqlTemplateGenericConvert, SXP_TYPE_DOUBLE);
}

/* ---------- Internal Functions for data type convertions ---------------- */

int internalConvertVectors(MYSQL * sqlConn, rdbVector * fromVector,
			   rdbVector * toVector, char * sqlTemplate, int type)
{
  /* Build the sql string */
  int length = strlen(sqlTemplate) + strlen(fromVector->tableName) + 1;
  char * sqlString = (char*) malloc( length * sizeof(char) );
  sprintf(sqlString, sqlTemplate, fromVector->tableName);

  toVector->sxp_type = type;
  toVector->isView = TRUE;
  toVector->size = fromVector->size;

  /* Create the view */
  int success = 0;
  switch( type )
  {
    case SXP_TYPE_INTEGER:
      success = createNewIntegerVectorView(sqlConn, toVector, sqlString);
      break;
    case SXP_TYPE_DOUBLE:
      success = createNewDoubleVectorView(sqlConn, toVector, sqlString);
      break;
    case SXP_TYPE_STRING:
      success = createNewStringVectorView(sqlConn, toVector, sqlString);
      break;
    case SXP_TYPE_COMPLEX:
      success = createNewComplexVectorView(sqlConn, toVector, sqlString);
      break;
    case SXP_TYPE_LOGIC:
      success = createNewLogicVectorView(sqlConn, toVector, sqlString);
      break;
  }

  if( success )
     createVectorViewReferences(sqlConn, toVector, fromVector, fromVector);
  else
     toVector->size = 0;

  free(sqlString);
  return success;
}


