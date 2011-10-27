/*****************************************************************************
 * Contains functions for vector comparisons with other vectors or values.
 *
 * Author: Herodotos Herodotou
 * Date:   Sep 17, 2008
 ****************************************************************************/
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <mysql.h>
#include "basics.h"
#include "compare_vectors.h"
#include "handle_vector_views.h"


/* --------- Functions to compare with values from a DBVector ---------- */
int compareWithDBVector(MYSQL * sqlConn, rdbVector * resultVector,
			rdbVector * dataVector, rdbVector * compareVector,
			char * comparisonOp)
{
  if(dataVector->sxp_type == SXP_TYPE_COMPLEX ||
     compareVector->sxp_type == SXP_TYPE_COMPLEX)
    return 0;

  /* Build the sql string */
  int size = compareVector->size;
  int length = strlen(sqlTemplateCompareWDBVector) +
               strlen(dataVector->tableName) + strlen(compareVector->tableName) +
               strlen(comparisonOp) + 4*MAX_INT_LENGTH + 1;
  char strCompareSQL[length];
  sprintf( strCompareSQL, sqlTemplateCompareWDBVector, size, size, comparisonOp,
	   dataVector->tableName, compareVector->tableName, size, size );

  /* Create the results (logic) view */
  resultVector->sxp_type = SXP_TYPE_LOGIC;
  resultVector->isView = TRUE;
  resultVector->size = dataVector->size;
  int success = createNewLogicVectorView(sqlConn, resultVector, strCompareSQL);

  if( success )
     createVectorViewReferences(sqlConn, resultVector, dataVector, compareVector);
  else
     resultVector->size = 0;

  return success;
}


int compareWithIntegerValues(MYSQL * sqlConn, rdbVector * resultVector,
			     rdbVector * dataVector, int compareValues[],
			     int size, char * comparisonOp)
{
  if( size <= 0 )
    return 0;

  char* strComparison;
  buildIntegerComparisonString(&strComparison,compareValues,size,comparisonOp);

  int success = internalCompareWithValues(sqlConn, resultVector, dataVector,
					  strComparison);

  free(strComparison);

  return success;
}

int compareWithDoubleValues(MYSQL * sqlConn, rdbVector * resultVector,
			    rdbVector * dataVector, double compareValues[],
			    int size, char * comparisonOp)
{
  if( size <= 0 )
    return 0;

  char* strComparison;
  buildDoubleComparisonString(&strComparison,compareValues,size,comparisonOp);

  int success = internalCompareWithValues(sqlConn, resultVector, dataVector,
					  strComparison);

  free(strComparison);

  return success;
}

int compareWithStringValues(MYSQL * sqlConn, rdbVector * resultVector,
			    rdbVector * dataVector, char * compareValues[],
			    int size, char * comparisonOp)
{
  if( size <= 0 )
    return 0;

  char* strComparison;
  buildStringComparisonString(&strComparison,compareValues,size,comparisonOp);

  int success = internalCompareWithValues(sqlConn, resultVector, dataVector,
					  strComparison);

  free(strComparison);

  return success;
}


int internalCompareWithValues(MYSQL * sqlConn, rdbVector * resultVector,
			      rdbVector * dataVector, char * strComparison)
{
  /* Build the sql string */
  int length = strlen(sqlTemplateCompareWithValues) +
               strlen(strComparison) + strlen(dataVector->tableName) + 1;
  char strCompareSQL[length];
  sprintf( strCompareSQL, sqlTemplateCompareWithValues, strComparison,
	   dataVector->tableName );

  /* Create the results (logic) view */
  resultVector->sxp_type = SXP_TYPE_LOGIC;
  resultVector->isView = TRUE;
  resultVector->size = dataVector->size;
  int success = createNewLogicVectorView(sqlConn, resultVector, strCompareSQL);

  if( success )
     createVectorViewReferences(sqlConn, resultVector, dataVector, dataVector);
  else
     resultVector->size = 0;

  return success;
}



/* ------------- Functions to build strings for comparisons -------------- */

void buildIntegerComparisonString(char ** strComparison, int compareValues[],
				  int size, char * comparisonOp)
{
  int length = strlen(sqlTemplateIntegerComparisonOR) +
               3*MAX_INT_LENGTH + strlen(comparisonOp) + 1;
  (*strComparison) = (char*) malloc( length * size * sizeof(char) );
  (*strComparison)[0] = '\0';

  int i;
  char strPiece[length];
  for( i = 0 ; i < size - 1 ; i++ )
  {
     sprintf(strPiece, sqlTemplateIntegerComparisonOR, size, i+1, comparisonOp,
	     compareValues[i]);
     strcat(*strComparison, strPiece);
  }

  sprintf(strPiece, sqlTemplateIntegerComparison, size, 0, comparisonOp,
	  compareValues[i]);
  strcat(*strComparison, strPiece);
}


void buildDoubleComparisonString(char ** strComparison, double compareValues[],
				 int size, char * comparisonOp)
{
  int length = strlen(sqlTemplateDoubleComparisonOR) +
               2*MAX_INT_LENGTH + MAX_DOUBLE_LENGTH + strlen(comparisonOp) + 1;
  (*strComparison) = (char*) malloc( length * size * sizeof(char) );
  (*strComparison)[0] = '\0';

  int i;
  char strPiece[length];
  for( i = 0 ; i < size - 1 ; i++ )
  {
     sprintf(strPiece, sqlTemplateDoubleComparisonOR, size, i+1, comparisonOp,
	     compareValues[i]);
     strcat(*strComparison, strPiece);
  }

  sprintf(strPiece, sqlTemplateDoubleComparison, size, 0, comparisonOp,
	  compareValues[i]);
  strcat(*strComparison, strPiece);
}


void buildStringComparisonString(char ** strComparison, char* compareValues[],
				 int size, char * comparisonOp)
{
  int i, pieceLength = strlen(sqlTemplateStringComparisonOR) +
                       2*MAX_INT_LENGTH + strlen(comparisonOp) + 1;
  int totalLength = size * pieceLength;
  for( i = 0 ; i < size ; i++ )
    totalLength += strlen(compareValues[i]);

  (*strComparison) = (char*) malloc( totalLength * sizeof(char) );
  (*strComparison)[0] = '\0';

  for( i = 0 ; i < size - 1 ; i++ )
  {
     int length = pieceLength + strlen(compareValues[i]);
     char * strPiece = (char*) malloc( length * sizeof(char) );
     sprintf(strPiece, sqlTemplateStringComparisonOR, size, i+1, comparisonOp,
	     compareValues[i]);
     strcat(*strComparison, strPiece);
     free(strPiece);
  }

  int length = pieceLength + strlen(compareValues[i]);
  char * strPiece = (char*) malloc( length * sizeof(char) );
  sprintf(strPiece, sqlTemplateStringComparison, size, 0, comparisonOp,
	  compareValues[i]);
  strcat(*strComparison, strPiece);
  free(strPiece);
}

