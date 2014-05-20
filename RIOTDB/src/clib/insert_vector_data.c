/*****************************************************************************
 * Contains functions for inserting data to a vector either explicitely
 * or load from a file
 *
 * Author: Herodotos Herodotou
 * Date:   Sep 17, 2008
 ****************************************************************************/
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <mysql.h>
#include "basics.h"
#include "insert_vector_data.h"
#include "handle_metadata.h"
#include "handle_vector_tables.h"


/* ---------- Functions to load data from a local file ------------ */

int loadIntoVectorTable(MYSQL * sqlConn, rdbVector * vectorInfo,
			char * filename)
{
  return internalLoadIntoAnyVectorTable(sqlConn, vectorInfo,
					sqlTemplateLoadIntoVector, filename);
}


int loadIntoComplexVectorTable(MYSQL * sqlConn, rdbVector * vectorInfo,
			       char * filename)
{
  return internalLoadIntoAnyVectorTable(sqlConn, vectorInfo,
					sqlTemplateLoadComplexVector, filename);
}


int internalLoadIntoAnyVectorTable(MYSQL * sqlConn, rdbVector * vectorInfo,
				   char * sqlTemplate, char * filename)
{
  /* Build the sql string */
  int length = strlen(sqlTemplate) + strlen(vectorInfo->tableName) +
               strlen(filename) + 1;
  char strLoadVector[length];
  sprintf( strLoadVector, sqlTemplate, filename, vectorInfo->tableName );

  /* Execute the query */
  int success = mysql_query(sqlConn, strLoadVector);
  if( success != 0 )
     return 0;

  int numElems =  mysql_affected_rows(sqlConn);
  if( numElems >= 0 )
  {
     int newSize = numElems + vectorInfo->size;
     success = setLogicalVectorSize(sqlConn, vectorInfo, newSize);
  }

  return success;
}


/* ------- Functions to insert values into vector tables ----------- */

int insertIntoIntVectorTable(MYSQL * sqlConn, rdbVector * vectorInfo,
			     int * values, int size )
{
  /* Some error checking */
  if( size < 1 )
      return 0;

  /* Perform the insertions in parts */
  int remain = size;
  int iter = 0;
  int left, right, success = 1;
  while( remain > MAX_NUM_INTEGERS )
  {
    left = iter * MAX_NUM_INTEGERS;
    right = left + MAX_NUM_INTEGERS;
    success *= insertPartIntVectorTable(sqlConn, vectorInfo, values, left, right);
    remain -= MAX_NUM_INTEGERS;
    iter++;
  }

  left = iter * MAX_NUM_INTEGERS;
  right = left + remain;
  success *= insertPartIntVectorTable(sqlConn, vectorInfo, values, left, right);

  /* Update the size */
  int newSize = size + vectorInfo->size;
  success *= setLogicalVectorSize(sqlConn, vectorInfo, newSize);

  return success;
}


int insertIntoDoubleVectorTable(MYSQL * sqlConn, rdbVector * vectorInfo,
				double * values, int size )
{
  /* Some error checking */
  if( size < 1 )
      return 0;

  /* Perform the insertions in parts */
  int remain = size;
  int iter = 0;
  int left, right, success = 1;
  while( remain > MAX_NUM_INTEGERS )
  {
    left = iter * MAX_NUM_DOUBLES;
    right = left + MAX_NUM_DOUBLES;
    success *= insertPartDoubleVectorTable(sqlConn, vectorInfo,
					   values, left, right);
    remain -= MAX_NUM_DOUBLES;
    iter++;
  }

  left = iter * MAX_NUM_DOUBLES;
  right = left + remain;
  success *= insertPartDoubleVectorTable(sqlConn, vectorInfo,
					 values, left, right);

  /* Update the size */
  int newSize = size + vectorInfo->size;
  success *= setLogicalVectorSize(sqlConn, vectorInfo, newSize);

  return success;
}


int insertIntoStringVectorTable(MYSQL * sqlConn, rdbVector * vectorInfo,
				char ** values, int size )
{
  /* Some error checking */
  if( size < 1 )
      return 0;

  /* Perform the insertions in parts */
  int remain = size;
  int iter = 0;
  int left, right, success = 1;
  while( remain > MAX_NUM_STRINGS )
  {
    left = iter * MAX_NUM_STRINGS;
    right = left + MAX_NUM_STRINGS;
    success *= insertPartStringVectorTable(sqlConn, vectorInfo,
					   values, left, right);
    remain -= MAX_NUM_STRINGS;
    iter++;
  }

  left = iter * MAX_NUM_STRINGS;
  right = left + remain;
  success *= insertPartStringVectorTable(sqlConn, vectorInfo,
					 values, left, right);

  /* Update the size */
  int newSize = size + vectorInfo->size;
  success *= setLogicalVectorSize(sqlConn, vectorInfo, newSize);

  return success;
}


int insertIntoComplexVectorTable(MYSQL * sqlConn, rdbVector * vectorInfo,
				 double * realValues, int numReals,
				 double *imagValues, int numImags,
				 int desiredSize)
{
  /* Some error checking */
  if( desiredSize < 1 )
      return 0;

  /* Perform the insertions in parts */
  int remain = desiredSize;
  int iter = 0;
  int left, right, success = 1;
  while( remain > MAX_NUM_COMPLEX )
  {
    left = iter * MAX_NUM_COMPLEX;
    right = left + MAX_NUM_COMPLEX;
    success *= insertPartComplexVectorTable(sqlConn, vectorInfo,
					    realValues, numReals,
					    imagValues, numImags,
					    left, right);
    remain -= MAX_NUM_COMPLEX;
    iter++;
  }

  left = iter * MAX_NUM_COMPLEX;
  right = left + remain;
  success *= insertPartComplexVectorTable(sqlConn, vectorInfo,
					  realValues, numReals,
					  imagValues, numImags,
					  left, right);

  /* Update the size */
  int newSize = desiredSize + vectorInfo->size;
  success *= setLogicalVectorSize(sqlConn, vectorInfo, newSize);

  return success;
}


int insertIntoLogicVectorTable(MYSQL * sqlConn, rdbVector * vectorInfo,
			       int * values, int size )
{
  /* Some error checking */
  if( size < 1 )
      return 0;

  /* Perform the insertions in parts */
  int remain = size;
  int iter = 0;
  int left, right, success = 1;
  while( remain > MAX_NUM_LOGIC )
  {
    left = iter * MAX_NUM_LOGIC;
    right = left + MAX_NUM_LOGIC;
    success *= insertPartLogicVectorTable(sqlConn, vectorInfo, values,
					  left, right);
    remain -= MAX_NUM_LOGIC;
    iter++;
  }

  left = iter * MAX_NUM_LOGIC;
  right = left + remain;
  success *= insertPartLogicVectorTable(sqlConn, vectorInfo,values,left,right);

  /* Update the size */
  int newSize = size + vectorInfo->size;
  success *= setLogicalVectorSize(sqlConn, vectorInfo, newSize);

  return success;
}


/* ------- Functions to insert sequences into vector tables ------- */
int insertSeqIntVectorTable(MYSQL * sqlConn, rdbVector * vectorInfo,
			    int start, int end, int step)
{
  if( step == 0 || (start < end && step < 0) || (start > end && step > 0) )
  {
    return 0;
  }

  /* Initialize necessary strings */
  int stringSize = MAX_NUM_INTEGERS * (MAX_INT_LENGTH + 3) + 1;
  char * strValues = (char *)malloc( stringSize * sizeof(char) );
  char strTemp[MAX_INT_LENGTH + 3];

  int numElems = 0;
  int success = 1;
  int counter, current = start;

  while( (step > 0) ? current <= end : current >= end )
  {
    /* Build string with the values of the form "(n),(n),...,(n)" */
    strValues[0] = '\0';
    counter = 0;

    while( counter < MAX_NUM_INTEGERS - 1 &&
	   ((step > 0) ? (current <= end - step):(current >= end - step)) )
    {
       sprintf(strTemp, "(%d),", current);
       strcat(strValues, strTemp);
       current += step;
       counter++;
    }

    sprintf(strTemp, "(%d)", current);
    strcat(strValues, strTemp);
    current += step;
    numElems += counter + 1;

    /* Execute the sql query */
    success *= insertIntoVectorTable(sqlConn, sqlTemplateInsertVector,
				     vectorInfo->tableName, strValues);
  }

  free(strValues);

  /* Update the size */
  int newSize = numElems + vectorInfo->size;
  success *= setLogicalVectorSize(sqlConn, vectorInfo, newSize);

  return success;
}


int insertSeqDoubleVectorTable(MYSQL * sqlConn, rdbVector * vectorInfo,
			       double start, double end, double step)
{
  if( step == 0 || (start < end && step < 0) || (start > end && step > 0) )
  {
    return 0;
  }

  /* Initialize necessary strings */
  int stringSize = MAX_NUM_DOUBLES * (MAX_DOUBLE_LENGTH + 3) + 1;
  char * strValues = (char *)malloc( stringSize * sizeof(char) );
  char strTemp[MAX_DOUBLE_LENGTH + 3];

  int numElems = 0;
  int success = 1;
  int counter;
  double current = start;

  while( (step > 0) ? current <= end : current >= end )
  {
    /* Build string with the values of the form "(n),(n),...,(n)" */
    strValues[0] = '\0';
    counter = 0;

    while( counter < MAX_NUM_DOUBLES - 1 &&
	   ((step > 0) ? (current <= end - step):(current >= end - step)) )
    {
       sprintf(strTemp, "(%lf),", current);
       strcat(strValues, strTemp);
       current += step;
       counter++;
    }

    sprintf(strTemp, "(%lf)", current);
    strcat(strValues, strTemp);
    current += step;
    numElems += counter + 1;

    /* Execute the sql query */
    success *= insertIntoVectorTable(sqlConn, sqlTemplateInsertVector,
				     vectorInfo->tableName, strValues);
  }

  free(strValues);

  /* Update the size */
  int newSize = numElems + vectorInfo->size;
  success *= setLogicalVectorSize(sqlConn, vectorInfo, newSize);

  return success;
}


int insertSeqLogicVectorTable(MYSQL * sqlConn, rdbVector * vectorInfo,
			      int value, int repeats)
{
  /* Initialize necessary strings */
  int stringSize = MAX_NUM_LOGIC * 4 + 1;
  char * strValues = (char *)malloc( stringSize * sizeof(char) );
  char strTemp[5];

  int success = 1;
  int innerCounter, remaining = repeats;
  char ch = (value == 0) ? '0' : '1';

  while( remaining > 0 )
  {
    /* Build string with the values of the form "(n),(n),...,(n)" */
    strValues[0] = '\0';
    innerCounter = 0;

    while( innerCounter < MAX_NUM_LOGIC - 1 && remaining > 1 )
    {
       sprintf(strTemp, "(%c),", ch);
       strcat(strValues, strTemp);
       innerCounter++;
       remaining--;
    }

    sprintf(strTemp, "(%c)", ch);
    strcat(strValues, strTemp);
    remaining--;

    /* Execute the sql query */
    success *= insertIntoVectorTable(sqlConn, sqlTemplateInsertVector,
				     vectorInfo->tableName, strValues);
  }

  free(strValues);

  /* Update the size */
  int newSize = repeats + vectorInfo->size;
  success *= setLogicalVectorSize(sqlConn, vectorInfo, newSize);

  return success;
}



/* ----------------------------------------------------------------- */
/* ----- Helper Functions to insert values into vector tables ------ */
int insertPartIntVectorTable(MYSQL * sqlConn, rdbVector * vectorInfo,
			     int * values, int left, int right)
{
  /* Initialize necessary strings */
  int stringSize = (right - left) * (MAX_INT_LENGTH + 3) + 1;
  char * strValues = (char *)malloc( stringSize * sizeof(char) );
  char value[MAX_INT_LENGTH + 4];
  strValues[0] = '\0';

  /* Build string with the values of the form "(n),(n),...,(n)" */
  int i;
  for( i = left ; i < right - 1 ; i++ )
  {
     sprintf(value, "(%d),", values[i]);
     strcat(strValues, value);
  }

  sprintf(value, "(%d)", values[i]);
  strcat(strValues, value);

  /* Execute the sql query */
  int success = insertIntoVectorTable(sqlConn, sqlTemplateInsertVector,
				      vectorInfo->tableName, strValues);

  free(strValues);

  return success;
}


int insertPartDoubleVectorTable(MYSQL * sqlConn, rdbVector * vectorInfo,
				double * values, int left, int right)
{
  /* Initialize necessary strings */
  int stringSize = (right - left) * (MAX_DOUBLE_LENGTH + 3) + 1;
  char * strValues = (char *)malloc( stringSize * sizeof(char) );
  char value[MAX_DOUBLE_LENGTH + 4];
  strValues[0] = '\0';

  /* Build string with the values of the form "(n),(n),...,(n)" */
  int i;
  for( i = left ; i < right - 1 ; i++ )
  {
     sprintf(value, "(%lf),", values[i]);
     strcat(strValues, value);
  }

  sprintf(value, "(%lf)", values[i]);
  strcat(strValues, value);

  /* Execute the sql query */
  int success = insertIntoVectorTable(sqlConn, sqlTemplateInsertVector,
				      vectorInfo->tableName, strValues);

  free(strValues);

  return success;
}


int insertPartStringVectorTable(MYSQL * sqlConn, rdbVector * vectorInfo,
				char ** values, int left, int right)
{

  /* Encode the strings */
  int size = right - left;
  char ** encodedValues = (char **) malloc( size * sizeof(char *) );
  unsigned long int totalSize = 0;
  int iVal, bufSize, i = 0;
  for( iVal = left ; iVal < right ; iVal++, i++ )
  {
     bufSize = 2 * strlen(values[iVal]) + 1;
     encodedValues[i] = (char*) malloc( bufSize * sizeof(char) );

     totalSize += 5 + mysql_real_escape_string(sqlConn, encodedValues[i],
				     values[iVal], strlen(values[iVal]));
  }

  /* Build string with the values of the form "('s'),('s'),...,('s')" */
  char * strValues = (char *) malloc( totalSize * sizeof(char) );
  strValues[0] = '\0';

  for( i = 0 ; i < size - 1 ; i++ )
  {
     char * value = (char *)malloc( (6+strlen(encodedValues[i]))*sizeof(char) );
     sprintf(value, "('%s'),", encodedValues[i]);
     strcat(strValues, value);
     free(value);
  }

  char * value = (char *)malloc( (6+strlen(encodedValues[i]))*sizeof(char) );
  sprintf(value, "('%s')", encodedValues[i]);
  strcat(strValues, value);
  free(value);

  /* Execute the sql query */
  int success = insertIntoVectorTable(sqlConn, sqlTemplateInsertVector,
				      vectorInfo->tableName, strValues);

  /* Clean up */
  for( i = 0 ; i < size ; i++ )
  {
     free(encodedValues[i]);
  }
  free(encodedValues);
  free(strValues);

  return success;
}


int insertPartComplexVectorTable(MYSQL * sqlConn, rdbVector * vectorInfo,
				 double * realValues, int numReals,
				 double *imagValues, int numImags,
				 int left, int right)
{
  /* Initialize necessary strings */
  int stringSize = (right-left) * (2*MAX_DOUBLE_LENGTH + 4) + 1;
  char * strValues = (char *)malloc( stringSize * sizeof(char) );
  char value[2*MAX_DOUBLE_LENGTH + 5];
  strValues[0] = '\0';

  /* Build string with the values of the form "(r,i),(r,i),...,(r,i)" */
  int i;
  for( i = left ; i < right - 1 ; i++ )
  {
     sprintf(value, "(%lf,%lf),",realValues[i%numReals],imagValues[i%numImags]);
     strcat(strValues, value);
  }

  sprintf(value, "(%lf,%lf)", realValues[i%numReals], imagValues[i%numImags]);
  strcat(strValues, value);

  /* Execute the sql query */
  int success = insertIntoVectorTable(sqlConn, sqlTemplateInsertComplexVector,
				      vectorInfo->tableName, strValues);

  free(strValues);

  return success;
}


int insertPartLogicVectorTable(MYSQL * sqlConn, rdbVector * vectorInfo,
			       int * values, int left, int right)
{
  /* Initialize necessary strings */
  int stringSize = (right - left) * (MAX_LOGIC_LENGTH + 3) + 1;
  char * strValues = (char *)malloc( stringSize * sizeof(char) );
  char value[MAX_LOGIC_LENGTH + 4];
  strValues[0] = '\0';

  /* Build string with the values of the form "(n),(n),...,(n)" */
  int i;
  for( i = left ; i < right - 1 ; i++ )
  {
     sprintf(value, "(%c),", (values[i] == 0) ? '0' : '1');
     strcat(strValues, value);
  }

  sprintf(value, "(%c)", (values[i] == 0) ? '0' : '1');
  strcat(strValues, value);

  /* Execute the sql query */
  int success = insertIntoVectorTable(sqlConn, sqlTemplateInsertVector,
				      vectorInfo->tableName, strValues);

  free(strValues);

  return success;
}


/* -------------------- Other Helper functions ---------------------- */

int insertIntoVectorTable(MYSQL * sqlConn, char * sqlTemplate,
			  char * tableName, char * strValues)
{
  /* Build the sql string */
  int length = strlen(sqlTemplate) + strlen(tableName) + strlen(strValues) + 1;
  char strInsertValues[length];
  sprintf( strInsertValues, sqlTemplate, tableName, strValues);

  /* Execute the query */
  int success = mysql_query(sqlConn, strInsertValues);

  return (success != 0) ? 0 : 1;
}


