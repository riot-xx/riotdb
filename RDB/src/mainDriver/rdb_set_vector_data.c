#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <mysql.h>
#include "Rinternals.h"
#include "rdb_basics.h"
#include "rdb_handle_vectors.h"
#include "rdb_set_vector_data.h"
#include "rdb_get_vector_data.h"
#include "rdb_insert_vector_data.h"


/* ---------- Functions to update vector tables by element ----------- */
int setIntElement(MYSQL * sqlConn, rdbVector * vectorInfo, 
		  unsigned int index, int value)
{
  if( !ensureMaterialization(sqlConn, vectorInfo) )
    return 0;

  return internalSetIntElement(sqlConn, vectorInfo, index, value);
}


int setDoubleElement(MYSQL * sqlConn, rdbVector * vectorInfo, 
		     unsigned  int index, double value)
{
  if( !ensureMaterialization(sqlConn, vectorInfo) )
    return 0;

  return internalSetDoubleElement(sqlConn, vectorInfo, index, value);
}


int setStringElement(MYSQL * sqlConn, rdbVector * vectorInfo, 
		     unsigned int index, char * value)
{
  if( !ensureMaterialization(sqlConn, vectorInfo) )
    return 0;

  return internalSetStringElement(sqlConn, vectorInfo, index, value);
}


int setComplexElement(MYSQL * sqlConn, rdbVector * vectorInfo,
		      unsigned int index, Rcomplex value)
{
  if( !ensureMaterialization(sqlConn, vectorInfo) )
    return 0;

  return internalSetComplexElement(sqlConn, vectorInfo, index, value);
}


int setLogicElement(MYSQL * sqlConn, rdbVector * vectorInfo, 
		    unsigned int index, int value)
{
  if( !ensureMaterialization(sqlConn, vectorInfo) )
    return 0;

  return internalSetLogicElement(sqlConn, vectorInfo, index, value);
}


/* ---------- Functions to update vector tables with ranges ----------- */

int setRangeIntElements(MYSQL * sqlConn, rdbVector * vectorInfo, 
		        unsigned int greaterThan,  unsigned int lessThan, 
			int values[], int numValues)
{
  if( !ensureMaterialization(sqlConn, vectorInfo) )
    return 0;

  int i, success = 1, j = 0;
  for( i = greaterThan ; i <= lessThan ; i++, j++ )
  {
    success *= internalSetIntElement(sqlConn, vectorInfo, i, values[j%numValues]);
  }

  return success;
}

int setRangeDoubleElements(MYSQL * sqlConn, rdbVector * vectorInfo, 
			   unsigned int greaterThan,  unsigned int lessThan, 
			   double values[], int numValues)
{
  if( !ensureMaterialization(sqlConn, vectorInfo) )
    return 0;

  int i, success = 1, j = 0;
  for( i = greaterThan ; i <= lessThan ; i++, j++ )
  {
    success *= internalSetDoubleElement(sqlConn, vectorInfo, i, 
					values[j%numValues]);
  }

  return success;
}


int setRangeStringElements(MYSQL * sqlConn, rdbVector * vectorInfo, 
			   unsigned int greaterThan,  unsigned int lessThan, 
			   char* values[], int numValues)
{
  if( !ensureMaterialization(sqlConn, vectorInfo) )
    return 0;

  int i, success = 1, j = 0;
  for( i = greaterThan ; i <= lessThan ; i++, j++ )
  {
    success *= internalSetStringElement(sqlConn, vectorInfo, i,
					values[j%numValues]);
  }

  return success;
}


int setRangeComplexElements(MYSQL * sqlConn, rdbVector * vectorInfo, 
			    unsigned int greaterThan,  unsigned int lessThan, 
			    Rcomplex values[], int numValues)
{
  if( !ensureMaterialization(sqlConn, vectorInfo) )
    return 0;

  int i, success = 1, j = 0;
  for( i = greaterThan ; i <= lessThan ; i++, j++ )
  {
    success *= internalSetComplexElement(sqlConn, vectorInfo, i, 
					 values[j%numValues]);
  }

  return success;
}


int setRangeLogicElements(MYSQL * sqlConn, rdbVector * vectorInfo, 
			  unsigned int greaterThan,  unsigned int lessThan, 
			  int values[], int numValues)
{
  if( !ensureMaterialization(sqlConn, vectorInfo) )
    return 0;

  int i, success = 1, j = 0;
  for( i = greaterThan ; i <= lessThan ; i++, j++ )
  {
    success *= internalSetLogicElement(sqlConn,vectorInfo,i,values[j%numValues]);
  }

  return success;
}



/* ----- Functions to update some elements in the vector tables ------ */
int setSparseIntElements(MYSQL * sqlConn, rdbVector * vectorInfo,
			 unsigned int indexes[], int numIndexes, 
			 int values[], int numValues)
{
  if( !ensureMaterialization(sqlConn, vectorInfo) )
    return 0;

  int i, success = 1;
  for( i = 0 ; i < numIndexes ; i++ )
  {
    success *= internalSetIntElement(sqlConn, vectorInfo, 
			     indexes[i], values[i%numValues]);
  }

  return success;
}


int setSparseDoubleElements(MYSQL * sqlConn, rdbVector * vectorInfo,
			    unsigned int indexes[], int numIndexes, 
			    double values[], int numValues)
{
  if( !ensureMaterialization(sqlConn, vectorInfo) )
    return 0;

  int i, success = 1;
  for( i = 0 ; i < numIndexes ; i++ )
  {
    success *= internalSetDoubleElement(sqlConn, vectorInfo, 
				indexes[i], values[i%numValues]);
  }

  return success;
}


int setSparseStringElements(MYSQL * sqlConn, rdbVector * vectorInfo, 
			    unsigned int indexes[], int numIndexes, 
			    char * values[], int numValues)
{
  if( !ensureMaterialization(sqlConn, vectorInfo) )
    return 0;

  int i, success = 1;
  for( i = 0 ; i < numIndexes ; i++ )
  {
    success *= internalSetStringElement(sqlConn, vectorInfo, 
				indexes[i], values[i%numValues]);
  }

  return success;
}


int setSparseComplexElements(MYSQL * sqlConn, rdbVector * vectorInfo, 
			     unsigned int indexes[], int numIndexes, 
			     Rcomplex values[], int numValues)
{
  if( !ensureMaterialization(sqlConn, vectorInfo) )
    return 0;

  int i, success = 1;
  for( i = 0 ; i < numIndexes ; i++ )
  {
    success *= internalSetComplexElement(sqlConn, vectorInfo, 
				 indexes[i], values[i%numValues]);
  }

  return success;
}


int setSparseLogicElements(MYSQL * sqlConn, rdbVector * vectorInfo,
			   unsigned int indexes[], int numIndexes, 
			   int values[], int numValues)
{
  if( !ensureMaterialization(sqlConn, vectorInfo) )
    return 0;

  int i, success = 1;
  for( i = 0 ; i < numIndexes ; i++ )
  {
    success *= internalSetLogicElement(sqlConn, vectorInfo, 
				       indexes[i], values[i%numValues]);
  }

  return success;
}


/* -- Functions to update some elements using a DBVector with indexes -- */
int setIntElementsWDBVector(MYSQL * sqlConn, rdbVector * vectorInfo,
			    rdbVector * indexVector, 
			    int values[], int numValues)
{
  if( indexVector->sxp_type != SXP_TYPE_INTEGER &&
      indexVector->sxp_type != SXP_TYPE_DOUBLE )
    return 0;

  if( !ensureMaterialization(sqlConn, vectorInfo) )
    return 0;

  int i, success = 1;
  int dbIndex = 0;
  int next = 0;
  char flag;
  for( i = 1 ; i <= indexVector->size ; i++ )
  {
     flag = 0;
     if( getIntElement(sqlConn, indexVector, i, &dbIndex, &flag) )
     {
       if( flag != 0 )
       {
	 success *= setIntElement(sqlConn, vectorInfo, dbIndex, 
				  values[next%numValues]);
	 next++;
       }
     }
  }

  return success;
}


int setDoubleElementsWDBVector(MYSQL * sqlConn, rdbVector * vectorInfo,
			       rdbVector * indexVector, 
			       double values[], int numValues)
{
  if( indexVector->sxp_type != SXP_TYPE_INTEGER &&
      indexVector->sxp_type != SXP_TYPE_DOUBLE )
    return 0;

  if( !ensureMaterialization(sqlConn, vectorInfo) )
    return 0;

  int i, success = 1;
  int dbIndex = 0;
  int next = 0;
  char flag;
  for( i = 1 ; i <= indexVector->size ; i++ )
  {
     flag = 0;
     if( getIntElement(sqlConn, indexVector, i, &dbIndex, &flag) )
     {
       if( flag != 0 )
       {
	 success *= setDoubleElement(sqlConn, vectorInfo, dbIndex, 
				     values[next%numValues]);
	 next++;
       }
     }
  }

  return success;
}


int setStringElementsWDBVector(MYSQL * sqlConn, rdbVector * vectorInfo, 
			       rdbVector * indexVector, 
			       char * values[], int numValues)
{
  if( indexVector->sxp_type != SXP_TYPE_INTEGER &&
      indexVector->sxp_type != SXP_TYPE_DOUBLE )
    return 0;

  if( !ensureMaterialization(sqlConn, vectorInfo) )
    return 0;

  int i, success = 1;
  int dbIndex = 0;
  int next = 0;
  char flag;
  for( i = 1 ; i <= indexVector->size ; i++ )
  {
     flag = 0;
     if( getIntElement(sqlConn, indexVector, i, &dbIndex, &flag) )
     {
       if( flag != 0 )
       {
	 success *= setStringElement(sqlConn, vectorInfo, dbIndex, 
				     values[next%numValues]);
	 next++;
       }
     }
  }

  return success;
}


int setComplexElementsWDBVector(MYSQL * sqlConn, rdbVector * vectorInfo, 
				rdbVector * indexVector, 
				Rcomplex values[], int numValues)
{
  if( indexVector->sxp_type != SXP_TYPE_INTEGER &&
      indexVector->sxp_type != SXP_TYPE_DOUBLE )
    return 0;

  if( !ensureMaterialization(sqlConn, vectorInfo) )
    return 0;

  int i, success = 1;
  int dbIndex = 0;
  int next = 0;
  char flag;
  for( i = 1 ; i <= indexVector->size ; i++ )
  {
     flag = 0;
     if( getIntElement(sqlConn, indexVector, i, &dbIndex, &flag) )
     {
       if( flag != 0 )
       {
	 success *= setComplexElement(sqlConn, vectorInfo, dbIndex, 
				      values[next%numValues]);
	 next++;
       }
     }
  }

  return success;
}


int setLogicElementsWDBVector(MYSQL * sqlConn, rdbVector * vectorInfo,
			      rdbVector * indexVector, 
			      int values[], int numValues)
{
  if( indexVector->sxp_type != SXP_TYPE_INTEGER &&
      indexVector->sxp_type != SXP_TYPE_DOUBLE )
    return 0;

  if( !ensureMaterialization(sqlConn, vectorInfo) )
    return 0;

  int i, success = 1;
  int dbIndex = 0;
  int next = 0;
  char flag;
  for( i = 1 ; i <= indexVector->size ; i++ )
  {
     flag = 0;
     if( getIntElement(sqlConn, indexVector, i, &dbIndex, &flag) )
     {
       if( flag != 0 )
       {
	 success *= setLogicElement(sqlConn, vectorInfo, dbIndex, 
				    values[next%numValues]);
	 next++;
       }
     }
  }

  return success;
}


/* -- Functions to update some elements using a Logic Vector as indexes -- */
int setIntElementsWithLogic(MYSQL * sqlConn, rdbVector * vectorInfo,
			    rdbVector * logicVector, 
			    int values[], int numValues)
{
  if( logicVector->sxp_type != SXP_TYPE_LOGIC || logicVector->size <= 0 )
    return 0;

  if( !ensureMaterialization(sqlConn, vectorInfo) )
    return 0;

  int lIndex, lSize = logicVector->size;
  int i, success = 1;
  int logic = 0;
  int next = 0;
  char flag;
  for( i = 1 ; i <= vectorInfo->size ; i++ )
  {
     flag = 0;
     lIndex = (i % lSize == 0) ? lSize : i % lSize;
     if( getLogicElement(sqlConn, logicVector, lIndex, &logic, &flag) )
     {
       if( flag != 0 && logic != 0 )
       {
	 success *= setIntElement(sqlConn, vectorInfo, i,
				  values[next%numValues]);
	 next++;
       }
     }
  }

  return success;
}

int setDoubleElementsWithLogic(MYSQL * sqlConn, rdbVector * vectorInfo,
			       rdbVector * logicVector, 
			       double values[], int numValues)
{
  if( logicVector->sxp_type != SXP_TYPE_LOGIC || logicVector->size <= 0 )
    return 0;

  if( !ensureMaterialization(sqlConn, vectorInfo) )
    return 0;

  int lIndex, lSize = logicVector->size;
  int i, success = 1;
  int logic = 0;
  int next = 0;
  char flag;
  for( i = 1 ; i <= vectorInfo->size ; i++ )
  {
     flag = 0;
     lIndex = (i % lSize == 0) ? lSize : i % lSize;
     if( getLogicElement(sqlConn, logicVector, lIndex, &logic, &flag) )
     {
       if( flag != 0 && logic != 0 )
       {
	 success *= setDoubleElement(sqlConn, vectorInfo, i,
				     values[next%numValues]);
	 next++;
       }
     }
  }

  return success;
}


int setStringElementsWithLogic(MYSQL * sqlConn, rdbVector * vectorInfo, 
			       rdbVector * logicVector, 
			       char * values[], int numValues)
{
  if( logicVector->sxp_type != SXP_TYPE_LOGIC || logicVector->size <= 0 )
    return 0;

  if( !ensureMaterialization(sqlConn, vectorInfo) )
    return 0;

  int lIndex, lSize = logicVector->size;
  int i, success = 1;
  int logic = 0;
  int next = 0;
  char flag;
  for( i = 1 ; i <= vectorInfo->size ; i++ )
  {
     flag = 0;
     lIndex = (i % lSize == 0) ? lSize : i % lSize;
     if( getLogicElement(sqlConn, logicVector, lIndex, &logic, &flag) )
     {
       if( flag != 0 && logic != 0 )
       {
	 success *= setStringElement(sqlConn, vectorInfo, i,
				     values[next%numValues]);
	 next++;
       }
     }
  }

  return success;
}


int setComplexElementsWithLogic(MYSQL * sqlConn, rdbVector * vectorInfo, 
				rdbVector * logicVector, 
				Rcomplex values[], int numValues)
{
  if( logicVector->sxp_type != SXP_TYPE_LOGIC || logicVector->size <= 0 )
    return 0;

  if( !ensureMaterialization(sqlConn, vectorInfo) )
    return 0;

  int lIndex, lSize = logicVector->size;
  int i, success = 1;
  int logic = 0;
  int next = 0;
  char flag;
  for( i = 1 ; i <= vectorInfo->size ; i++ )
  {
     flag = 0;
     lIndex = (i % lSize == 0) ? lSize : i % lSize;
     if( getLogicElement(sqlConn, logicVector, lIndex, &logic, &flag) )
     {
       if( flag != 0 && logic != 0 )
       {
	 success *= setComplexElement(sqlConn, vectorInfo, i,
				      values[next%numValues]);
	 next++;
       }
     }
  }

  return success;
}


int setLogicElementsWithLogic(MYSQL * sqlConn, rdbVector * vectorInfo,
			      rdbVector * logicVector, 
			      int values[], int numValues)
{
  if( logicVector->sxp_type != SXP_TYPE_LOGIC || logicVector->size <= 0 )
    return 0;

  if( !ensureMaterialization(sqlConn, vectorInfo) )
    return 0;

  int lIndex, lSize = logicVector->size;
  int i, success = 1;
  int logic = 0;
  int next = 0;
  char flag;
  for( i = 1 ; i <= vectorInfo->size ; i++ )
  {
     flag = 0;
     lIndex = (i % lSize == 0) ? lSize : i % lSize;
     if( getLogicElement(sqlConn, logicVector, lIndex, &logic, &flag) )
     {
       if( flag != 0 && logic != 0 )
       {
	 success *= setLogicElement(sqlConn, vectorInfo, i,
				    values[next%numValues]);
	 next++;
       }
     }
  }

  return success;
}



/* ---------------- Internal functions for the setters --------------- */
int internalSetIntElement(MYSQL * sqlConn, rdbVector * vectorInfo, 
		  unsigned int index, int value)
{
  /* If index is greater then size, then "extend" the vector */
  if( index > vectorInfo->size )
  {
     vectorInfo->size = index;
     updateSizeVectorTable(sqlConn, vectorInfo, index);
     return internalInsertIntElement(sqlConn, vectorInfo, index, value);
  }

  /* Need to check the the element is already in the vector or not */
  int current;
  char byte = 0;
  getIntElement(sqlConn, vectorInfo, index, &current, &byte);

  if( byte == 1 && current != value )
     return internalUpdateIntElement(sqlConn, vectorInfo, index, value);
  else if( byte == 0 )
     return internalInsertIntElement(sqlConn, vectorInfo, index, value);

  return 1;
}


int internalSetDoubleElement(MYSQL * sqlConn, rdbVector * vectorInfo, 
		     unsigned  int index, double value)
{
  /* If index is greater then size, then "extend" the vector */
  if( index > vectorInfo->size )
  {
     vectorInfo->size = index;
     updateSizeVectorTable(sqlConn, vectorInfo, index);
     return internalInsertDoubleElement(sqlConn, vectorInfo, index, value);
  }

  /* Need to check the the element is already in the vector or not */
  double current;
  char byte = 0;
  getDoubleElement(sqlConn, vectorInfo, index, &current, &byte);

  if( byte == 1 && current != value )
     return internalUpdateDoubleElement(sqlConn, vectorInfo, index, value);
  else if( byte == 0 )
     return internalInsertDoubleElement(sqlConn, vectorInfo, index, value);

  return 1;
}


int internalSetStringElement(MYSQL * sqlConn, rdbVector * vectorInfo, 
		     unsigned int index, char * value)
{
  /* If index is greater then size, then "extend" the vector */
  if( index > vectorInfo->size )
  {
     vectorInfo->size = index;
     updateSizeVectorTable(sqlConn, vectorInfo, index);
     return internalInsertStringElement(sqlConn, vectorInfo, index, value);
  }

  /* Need to check the the element is already in the vector or not */
  char* current;
  char byte = 0;
  getStringElement(sqlConn, vectorInfo, index, &current, &byte);

  if( byte == 1 && strcmp(current, value) != 0 )
     return internalUpdateStringElement(sqlConn, vectorInfo, index, value);
  else if( byte == 0 )
     return internalInsertStringElement(sqlConn, vectorInfo, index, value);

  return 1;
}


int internalSetComplexElement(MYSQL * sqlConn, rdbVector * vectorInfo,
		      unsigned int index, Rcomplex value)
{
  /* If index is greater then size, then "extend" the vector */
  if( index > vectorInfo->size )
  {
     vectorInfo->size = index;
     updateSizeVectorTable(sqlConn, vectorInfo, index);
     return internalInsertComplexElement(sqlConn, vectorInfo, index, value);
  }

  /* Need to check the the element is already in the vector or not */
  Rcomplex current;
  char byte = 0;
  getComplexElement(sqlConn, vectorInfo, index, &current, &byte);

  if( byte == 1 && (current.r != value.r || current.i != value.i) )
     return internalUpdateComplexElement(sqlConn, vectorInfo, index, value);
  else if( byte == 0 )
     return internalInsertComplexElement(sqlConn, vectorInfo, index, value);

  return 1;
}


int internalSetLogicElement(MYSQL * sqlConn, rdbVector * vectorInfo, 
			    unsigned int index, int value)
{
  /* If index is greater then size, then "extend" the vector */
  if( index > vectorInfo->size )
  {
     vectorInfo->size = index;
     updateSizeVectorTable(sqlConn, vectorInfo, index);
     return internalInsertLogicElement(sqlConn, vectorInfo, index, value);
  }

  /* Need to check the the element is already in the vector or not */
  int current;
  char byte = 0;
  getLogicElement(sqlConn, vectorInfo, index, &current, &byte);

  if( byte == 1 && current != value )
     return internalUpdateLogicElement(sqlConn, vectorInfo, index, value);
  else if( byte == 0 )
     return internalInsertLogicElement(sqlConn, vectorInfo, index, value);

  return 1;
}


/* ------- Internal functions to perform insertions of new values ------- */
int internalInsertIntElement(MYSQL * sqlConn, rdbVector * vectorInfo, 
			     unsigned int index, int value)
{
  /* Build the sql string */
  int length = strlen(sqlTemplateInsertIntElement) + 
               strlen(vectorInfo->tableName) + 
               2*MAX_INT_LENGTH + 1;
  char strInsertElem[length];
  sprintf( strInsertElem, sqlTemplateInsertIntElement, vectorInfo->tableName, 
	   index, value );

  /* Execute the query */
  int success = mysql_query(sqlConn, strInsertElem);
  return (success != 0) ? 0 : 1;
}


int internalInsertDoubleElement(MYSQL * sqlConn, rdbVector * vectorInfo, 
				unsigned int index, double value)
{
  /* Build the sql string */
  int length = strlen(sqlTemplateInsertDoubleElement) + 
               strlen(vectorInfo->tableName) + 
               MAX_INT_LENGTH + MAX_DOUBLE_LENGTH + 1;
  char strInsertElem[length];
  sprintf( strInsertElem, sqlTemplateInsertDoubleElement, vectorInfo->tableName, 
	   index, value );

  /* Execute the query */
  int success = mysql_query(sqlConn, strInsertElem);
  return (success != 0) ? 0 : 1;
}


int internalInsertStringElement(MYSQL * sqlConn, rdbVector * vectorInfo, 
				unsigned int index, char* value)
{
  /* Build the sql string */
  int length = strlen(sqlTemplateInsertStringElement) + 
               strlen(vectorInfo->tableName) + 
               strlen(value) + MAX_INT_LENGTH + 1;
  char strInsertElem[length];
  sprintf( strInsertElem, sqlTemplateInsertStringElement, vectorInfo->tableName, 
	   index, value );

  /* Execute the query */
  int success = mysql_query(sqlConn, strInsertElem);
  return (success != 0) ? 0 : 1;
}


int internalInsertComplexElement(MYSQL * sqlConn, rdbVector * vectorInfo, 
				 unsigned int index, Rcomplex value)

{
  /* Build the sql string */
  int length = strlen(sqlTemplateInsertComplexElement) + 
               strlen(vectorInfo->tableName) + 
               2*MAX_DOUBLE_LENGTH + MAX_INT_LENGTH + 1;
  char strInsertElem[length];
  sprintf( strInsertElem, sqlTemplateInsertComplexElement, vectorInfo->tableName, 
	   index, value.r, value.i );

  /* Execute the query */
  int success = mysql_query(sqlConn, strInsertElem);
  return (success != 0) ? 0 : 1;
}


int internalInsertLogicElement(MYSQL * sqlConn, rdbVector * vectorInfo, 
			       unsigned int index, int value)
{
  /* Build the sql string */
  int length = strlen(sqlTemplateInsertLogicElement) + 
               strlen(vectorInfo->tableName) + 
               MAX_INT_LENGTH + MAX_LOGIC_LENGTH + 1;
  char strInsertElem[length];
  sprintf( strInsertElem, sqlTemplateInsertLogicElement, vectorInfo->tableName, 
	   index, (value == 0) ?  '0' : '1' );

  /* Execute the query */
  int success = mysql_query(sqlConn, strInsertElem);
  return (success != 0) ? 0 : 1;
}


/* ------------ Internal functions to perform the updates ------------ */
int internalUpdateIntElement(MYSQL * sqlConn, rdbVector * vectorInfo, 
			     unsigned int index, int value)
{
  /* Build the sql string */
  int length = strlen(sqlTemplateUpdateIntElement) + 
               strlen(vectorInfo->tableName) + 
               2*MAX_INT_LENGTH + 1;
  char strUpdateElem[length];
  sprintf( strUpdateElem, sqlTemplateUpdateIntElement, vectorInfo->tableName, 
	   value, index );

  /* Execute the query */
  int success = mysql_query(sqlConn, strUpdateElem);
  return (success != 0) ? 0 : 1;
}


int internalUpdateDoubleElement(MYSQL * sqlConn, rdbVector * vectorInfo, 
				unsigned int index, double value)
{
  /* Build the sql string */
  int length = strlen(sqlTemplateUpdateDoubleElement) + 
               strlen(vectorInfo->tableName) + 
               MAX_DOUBLE_LENGTH + MAX_INT_LENGTH + 1;
  char strUpdateElem[length];
  sprintf( strUpdateElem, sqlTemplateUpdateDoubleElement, vectorInfo->tableName, 
	   value, index );

  /* Execute the query */
  int success = mysql_query(sqlConn, strUpdateElem);
  return (success != 0) ? 0 : 1;
}


int internalUpdateStringElement(MYSQL * sqlConn, rdbVector * vectorInfo, 
				unsigned int index, char* value)
{
  /* Build the sql string */
  int length = strlen(sqlTemplateUpdateStringElement) + 
               strlen(vectorInfo->tableName) + 
               strlen(value) + MAX_INT_LENGTH + 1;
  char strUpdateElem[length];
  sprintf( strUpdateElem, sqlTemplateUpdateStringElement, vectorInfo->tableName, 
	   value, index );

  /* Execute the query */
  int success = mysql_query(sqlConn, strUpdateElem);
  return (success != 0) ? 0 : 1;
}


int internalUpdateComplexElement(MYSQL * sqlConn, rdbVector * vectorInfo, 
				 unsigned int index, Rcomplex value)
{
  /* Build the sql string */
  int length = strlen(sqlTemplateUpdateComplexElement) + 
               strlen(vectorInfo->tableName) + 
               2*MAX_DOUBLE_LENGTH + MAX_INT_LENGTH + 1;
  char strUpdateElem[length];
  sprintf( strUpdateElem, sqlTemplateUpdateComplexElement,
	   vectorInfo->tableName, value.r, value.i, index );

  /* Execute the query */
  int success = mysql_query(sqlConn, strUpdateElem);
  return (success != 0) ? 0 : 1;
}


int internalUpdateLogicElement(MYSQL * sqlConn, rdbVector * vectorInfo, 
			       unsigned int index, int value)
{
  /* Build the sql string */
  int length = strlen(sqlTemplateUpdateLogicElement) + 
               strlen(vectorInfo->tableName) + 
               MAX_INT_LENGTH + MAX_LOGIC_LENGTH + 1;
  char strUpdateElem[length];
  sprintf( strUpdateElem, sqlTemplateUpdateLogicElement, vectorInfo->tableName, 
	   (value == 0) ? '0' : '1', index );

  /* Execute the query */
  int success = mysql_query(sqlConn, strUpdateElem);
  return (success != 0) ? 0 : 1;
}

