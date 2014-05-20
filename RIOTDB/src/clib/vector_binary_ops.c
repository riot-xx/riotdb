/*****************************************************************************
 * Contains functions for binary operators for vectors (+, -, *, /, %)
 *
 * Author: Herodotos Herodotou
 * Date:   Sep 17, 2008
 ****************************************************************************/
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <mysql.h>
#include "basics.h"
#include "vector_binary_ops.h"
#include "handle_vector_views.h"
#include "convert_vectors.h"


/* --- Functions to execute binary operations on Ints and Doubles --- */

int addNumericVectors(MYSQL * sqlConn, rdbVector * result,
		      rdbVector * input1, rdbVector * input2)
{
  return internalHandleNumericBinOp(sqlConn, result, input1,
				    input2, PLUS_SIGN, 0);
}

int subtractNumericVectors(MYSQL * sqlConn, rdbVector * result,
			   rdbVector * input1, rdbVector * input2)
{
  return internalHandleNumericBinOp(sqlConn, result, input1,
				    input2, MINUS_SIGN, 0);
}

int multiplyNumericVectors(MYSQL * sqlConn, rdbVector * result,
			   rdbVector * input1, rdbVector * input2)
{
  return internalHandleNumericBinOp(sqlConn, result, input1,
				    input2, MULT_SIGN, 0);
}

int divideNumericVectors(MYSQL * sqlConn, rdbVector * result,
			 rdbVector * input1, rdbVector * input2)
{
  return internalHandleNumericBinOp(sqlConn, result, input1,
				    input2, DIV_SIGN, 1);
}

int modNumericVectors(MYSQL * sqlConn, rdbVector * result,
		      rdbVector * input1, rdbVector * input2)
{
  if( input1->sxp_type != SXP_TYPE_INTEGER ||
      input2->sxp_type != SXP_TYPE_INTEGER )
    return 0;

  return internalHandleNumericBinOp(sqlConn, result, input1,
				    input2, MOD_SIGN, 0);
}

/* --- Helper Functions for binary operations on Ints and Doubles --- */

int internalHandleNumericBinOp(MYSQL * sqlConn, rdbVector * result,
			       rdbVector * input1, rdbVector * input2,
			       char * sign, int forceDouble)
{
  /* Both inputs must either be integers or doubles or logic */
  if( input1->sxp_type != SXP_TYPE_INTEGER &&
      input1->sxp_type != SXP_TYPE_DOUBLE &&
      input1->sxp_type != SXP_TYPE_LOGIC )
    return 0;

  if( input2->sxp_type != SXP_TYPE_INTEGER &&
      input2->sxp_type != SXP_TYPE_DOUBLE &&
      input2->sxp_type != SXP_TYPE_LOGIC )
    return 0;

  /* Build the sql string and create the view */
  char * sqlString;
  buildNumericBinaryOpsSQL(input1, input2, sign, &sqlString);

  result->isView = TRUE;
  result->size = (input1->size > input2->size)? input1->size : input2->size;

  int success = 0;
  if( (input1->sxp_type == SXP_TYPE_INTEGER || input1->sxp_type == SXP_TYPE_LOGIC ) &&
      (input2->sxp_type == SXP_TYPE_INTEGER || input2->sxp_type == SXP_TYPE_LOGIC ) &&
      !forceDouble)
  {
     result->sxp_type = SXP_TYPE_INTEGER;
     success = createNewIntegerVectorView(sqlConn, result, sqlString);
  }
  else
  {
     result->sxp_type = SXP_TYPE_DOUBLE;
     success = createNewDoubleVectorView(sqlConn, result, sqlString);
  }
  free(sqlString);

  if( success )
     createVectorViewReferences(sqlConn, result, input1, input2);
  else
     result->size = 0;

  return success;
}


void buildNumericBinaryOpsSQL(rdbVector * input1, rdbVector * input2,
			      char sign[], char ** sqlStr)
{
  int size1 = input1->size;
  int size2 = input2->size;

  if( size1 > size2 )
  {
    /* Input1 is larger than input2 */
    int length = strlen(sqlTemplateNumericBinaryOps_NE) + strlen(TABLE_1) +
                 strlen(sign) + strlen(input1->tableName) +
                 strlen(input2->tableName) + 2*MAX_INT_LENGTH + 1;
    *sqlStr = (char*) malloc( length * sizeof(char) );
    sprintf(*sqlStr, sqlTemplateNumericBinaryOps_NE, TABLE_1, sign,
	    input1->tableName, input2->tableName, size2, size2);
  }
  else if( size1 < size2 )
  {
    /* Input2 is larger than input1 */
    int length = strlen(sqlTemplateNumericBinaryOps_NE) + strlen(TABLE_2) +
                 strlen(sign) + strlen(input1->tableName) +
                 strlen(input2->tableName) + 2*MAX_INT_LENGTH + 1;
    *sqlStr = (char*) malloc( length * sizeof(char) );
    sprintf(*sqlStr, sqlTemplateNumericBinaryOps_NE, TABLE_2, sign,
	    input1->tableName, input2->tableName, size1, size1);
  }
  else
  {
    /* Input1 is equal to input2 */
    int length = strlen(sqlTemplateNumericBinaryOps_EQ) + strlen(sign) +
                 strlen(input1->tableName) + strlen(input2->tableName) + 1;
    *sqlStr = (char*) malloc( length * sizeof(char) );
    sprintf(*sqlStr, sqlTemplateNumericBinaryOps_EQ, sign,
	    input1->tableName, input2->tableName);
  }
}


/* ---- Functions to execute binary operations on Complex Vectors ---- */

int addComplexVectors(MYSQL * sqlConn, rdbVector * result,
		      rdbVector * input1, rdbVector * input2)
{
  return internalHandleComplexBinOp(sqlConn, result, input1, input2, PLUS_OP);
}


int subtractComplexVectors(MYSQL * sqlConn, rdbVector * result,
			   rdbVector * input1, rdbVector * input2)
{
  return internalHandleComplexBinOp(sqlConn, result, input1, input2, MINUS_OP);
}


int multiplyComplexVectors(MYSQL * sqlConn, rdbVector * result,
			   rdbVector * input1, rdbVector * input2)
{
  return internalHandleComplexBinOp(sqlConn, result, input1, input2, MULT_OP);
}


int divideComplexVectors(MYSQL * sqlConn, rdbVector * result,
			 rdbVector * input1, rdbVector * input2)
{
  return internalHandleComplexBinOp(sqlConn, result, input1, input2, DIV_OP);
}


/* ---- Helper Functions for binary operations on Complex Vectors ---- */

int internalHandleComplexBinOp(MYSQL * sqlConn, rdbVector * result,
			  rdbVector * input1, rdbVector * input2, int op)
{
  /* Create some temporary rdbVector objects */
  rdbVector * cInput1 = newRDBVector();
  rdbVector * cInput2 = newRDBVector();
  if( input1->sxp_type == SXP_TYPE_INTEGER ||
      input1->sxp_type == SXP_TYPE_DOUBLE ||
      input1->sxp_type == SXP_TYPE_LOGIC )
  {
     cInput1->sxp_type = SXP_TYPE_COMPLEX;
     cInput1->isView = TRUE;
     cInput1->size = input1->size;
     if( !convertNumericToComplex(sqlConn, input1, cInput1) )
     {
       clearRDBVector(&cInput1);
       return 0;
     }
  }
  else if( input1->sxp_type == SXP_TYPE_COMPLEX )
  {
     copyRDBVector(cInput1, input1);
  }
  else
  {
    return 0;
  }

  if( input2->sxp_type == SXP_TYPE_INTEGER ||
      input2->sxp_type == SXP_TYPE_DOUBLE ||
      input2->sxp_type == SXP_TYPE_LOGIC )
  {
     cInput2->sxp_type = SXP_TYPE_COMPLEX;
     cInput2->isView = TRUE;
     cInput2->size = input2->size;
     if( !convertNumericToComplex(sqlConn, input2, cInput2) )
     {
       clearRDBVector(&cInput1);
       clearRDBVector(&cInput2);
       return 0;
     }
  }
  else if( input2->sxp_type == SXP_TYPE_COMPLEX )
  {
     copyRDBVector(cInput2, input2);
  }
  else
  {
    clearRDBVector(&cInput1);
    return 0;
  }


  /* Build the sql string */
  char * sqlString;
  switch( op )
  {
  case PLUS_OP:
    buildComplexAddSubSQL(cInput1, cInput2, PLUS_SIGN, &sqlString);
    break;
  case MINUS_OP:
    buildComplexAddSubSQL(cInput1, cInput2, MINUS_SIGN, &sqlString);
    break;
  case MULT_OP:
    buildComplexMultDivSQL(cInput1, cInput2, sqlTemplateComplexMultiply_EQ,
			   sqlTemplateComplexMultiply_NE, &sqlString);
    break;
  case DIV_OP:
    buildComplexMultDivSQL(cInput1, cInput2, sqlTemplateComplexDivide_EQ,
			   sqlTemplateComplexDivide_NE, &sqlString);
    break;
  default:
    clearRDBVector(&cInput1);
    clearRDBVector(&cInput2);
    return 0;
  }

  result->sxp_type = SXP_TYPE_COMPLEX;
  result->isView = TRUE;
  result->size = (cInput1->size > cInput2->size)? cInput1->size : cInput2->size;

  /* Create the view */
  int success = createNewComplexVectorView(sqlConn, result, sqlString);
  if( success )
     createVectorViewReferences(sqlConn, result, cInput1, cInput2);
  else
    result->size = 0;

  /* Clean up */
  free(sqlString);
  clearRDBVector(&cInput1);
  clearRDBVector(&cInput2);

  return success;
}


void buildComplexAddSubSQL(rdbVector * input1, rdbVector * input2,
			   char sign[], char ** sqlStr)
{
  int size1 = input1->size;
  int size2 = input2->size;

  if( size1 > size2 )
  {
    /* Input1 is larger than input2 */
    int length = strlen(sqlTemplateComplexAddSub_NE) + strlen(TABLE_1) +
                 2*strlen(sign) + strlen(input1->tableName) +
                 strlen(input2->tableName) + 2*MAX_INT_LENGTH + 1;
    *sqlStr = (char*) malloc( length * sizeof(char) );
    sprintf(*sqlStr, sqlTemplateComplexAddSub_NE, TABLE_1, sign, sign,
	    input1->tableName, input2->tableName, size2, size2);
  }
  else if( size1 < size2 )
  {
    /* Input2 is larger than input1 */
    int length = strlen(sqlTemplateComplexAddSub_NE) + strlen(TABLE_2) +
                 2*strlen(sign) + strlen(input1->tableName) +
                 strlen(input2->tableName) + 2*MAX_INT_LENGTH + 1;
    *sqlStr = (char*) malloc( length * sizeof(char) );
    sprintf(*sqlStr, sqlTemplateComplexAddSub_NE, TABLE_2, sign, sign,
	    input1->tableName, input2->tableName, size1, size1);
  }
  else
  {
    /* Input1 is equal to input2 */
    int length = strlen(sqlTemplateComplexAddSub_EQ) + 2*strlen(sign) +
                 strlen(input1->tableName) + strlen(input2->tableName) + 1;
    *sqlStr = (char*) malloc( length * sizeof(char) );
    sprintf(*sqlStr, sqlTemplateComplexAddSub_EQ, sign, sign,
	    input1->tableName, input2->tableName);
  }
}


void buildComplexMultDivSQL(rdbVector * input1, rdbVector * input2,
			    char * sqlTemplateEQ, char * sqlTemplateNE,
			    char ** sqlStr)
{
  int size1 = input1->size;
  int size2 = input2->size;

  if( size1 > size2 )
  {
    /* Input1 is larger than input2 */
    int length = strlen(sqlTemplateNE) + strlen(TABLE_1) +
                 strlen(input1->tableName) + strlen(input2->tableName) +
                 2*MAX_INT_LENGTH + 1;
    *sqlStr = (char*) malloc( length * sizeof(char) );
    sprintf(*sqlStr, sqlTemplateNE, TABLE_1,
	    input1->tableName, input2->tableName, size2, size2);
  }
  else if( size1 < size2 )
  {
    /* Input2 is larger than input1 */
    int length = strlen(sqlTemplateNE) + strlen(TABLE_2) +
                 strlen(input1->tableName) + strlen(input2->tableName) +
                 2*MAX_INT_LENGTH + 1;
    *sqlStr = (char*) malloc( length * sizeof(char) );
    sprintf(*sqlStr, sqlTemplateNE, TABLE_2,
	    input1->tableName, input2->tableName, size1, size1);
  }
  else
  {
    /* Input1 is equal to input2 */
    int length = strlen(sqlTemplateEQ) +
                 strlen(input1->tableName) + strlen(input2->tableName) + 1;
    *sqlStr = (char*) malloc( length * sizeof(char) );
    sprintf(*sqlStr, sqlTemplateEQ,
	    input1->tableName, input2->tableName);
  }
}


/* -------------------------- Other functions ----------------------------------- */

int subtractDoubleFromNumericVector(MYSQL * sqlConn, rdbVector * result,
				    rdbVector *input1, double y)
{
  /* Both inputs must either be integers or doubles or logic */
  if( input1->sxp_type != SXP_TYPE_INTEGER &&
      input1->sxp_type != SXP_TYPE_DOUBLE &&
      input1->sxp_type != SXP_TYPE_LOGIC )
    return 0;

  /* Build the sql string */
  int length = strlen(sqlTemplateSimpleNumericBinary) + strlen(MINUS_SIGN) +
	       strlen(input1->tableName) + MAX_DOUBLE_LENGTH + 1;
  char *sqlString = malloc( length * sizeof(char) );
  sprintf(sqlString, sqlTemplateSimpleNumericBinary, MINUS_SIGN, y, input1->tableName);

  /* Create the results view */
  result->sxp_type = SXP_TYPE_DOUBLE;
  result->isView = TRUE;
  result->size = input1->size;

  int success = 0;
  success = createNewDoubleVectorView(sqlConn, result, sqlString);
  free(sqlString);

  /* Create the view references */
  if( success )
     createVectorViewReferences(sqlConn, result, input1, input1);
  else
     result->size = 0;

  return success;
}


