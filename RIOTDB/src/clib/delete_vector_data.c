/*****************************************************************************
 * Contains functions for deleting elements from vectors (delete all, 
 * single values, range of values or sparse values)
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
#include "delete_vector_data.h"
#include "handle_metadata.h"
#include "handle_vector_tables.h"


/* ---------- Functions to delete elements from vector tables ----------- */
int deleteAllElements(MYSQL * sqlConn, rdbVector * vectorInfo)
{
   /* Build the sql string */
   int length = strlen(sqlTemplateDeleteAllValues) + 
                strlen(vectorInfo->tableName) + 1;
   char strDeleteSQL[length];
   sprintf( strDeleteSQL, sqlTemplateDeleteAllValues, vectorInfo->tableName);

   /* Execute the query */
   int success = mysql_query(sqlConn, strDeleteSQL);
   if(success != 0)
     return 0;

   setLogicalVectorSize(sqlConn, vectorInfo, 0);

   return 1;
}


int deleteSingleElement(MYSQL * sqlConn, rdbVector * vectorInfo, 
			unsigned int index)
{
   /* Build the sql string */
   int length = strlen(sqlTemplateDeleteSingleValue) + 
                strlen(vectorInfo->tableName) + MAX_INT_LENGTH + 1;
   char strDeleteSQL[length];
   sprintf( strDeleteSQL, sqlTemplateDeleteSingleValue, 
	    vectorInfo->tableName, index );

   /* Execute the query */
   int success = mysql_query(sqlConn, strDeleteSQL);
   if(success != 0)
     return 0;

   if( index == vectorInfo->size )
      setLogicalVectorSize(sqlConn, vectorInfo, vectorInfo->size - 1);

   return 1;
}


int deleteRangeElements(MYSQL * sqlConn, rdbVector * vectorInfo, 
		        unsigned int greaterThan,  unsigned int lessThan)
{
   /* Build the sql string */
   int length = strlen(sqlTemplateDeleteRangeValues) + 
                strlen(vectorInfo->tableName) + 2*MAX_INT_LENGTH + 1;
   char strDeleteSQL[length];
   sprintf( strDeleteSQL, sqlTemplateDeleteRangeValues, 
	    vectorInfo->tableName, greaterThan, lessThan);

   /* Execute the query */
   int success = mysql_query(sqlConn, strDeleteSQL);
   if(success != 0)
     return 0;

   if( vectorInfo->size >= greaterThan && vectorInfo->size <= lessThan ) {
     if( greaterThan > 0 ) {
       setLogicalVectorSize(sqlConn, vectorInfo, greaterThan - 1);
     }
     else {
       setLogicalVectorSize(sqlConn, vectorInfo, 0);
     }
   }

   return 1;
}


int deleteSparseElements(MYSQL * sqlConn, rdbVector * vectorInfo,
			 unsigned int indexes[], int size)
{
  /* Initialize necessary strings */
  int i;
  int stringSize = size * (MAX_INT_LENGTH + strlen(sqlTemplateSparseVIndexOR)) + 1;
  char * strIndexes = (char *)malloc( stringSize * sizeof(char) );
  char temp[MAX_INT_LENGTH + strlen(sqlTemplateSparseVIndexOR)];
  strIndexes[0] = '\0';

  /* Build string of the form: "vIndex = n OR vIndex = n ..." */
  for( i = 0 ; i < size - 1 ; i++ )
  {
     sprintf(temp, sqlTemplateSparseVIndexOR, indexes[i]);
     strcat(strIndexes, temp);
  }
  
  sprintf(temp, sqlTemplateSparseVIndex, indexes[i]);
  strcat(strIndexes, temp);

  /* Build the sql string */
  int length = strlen(sqlTemplateDeleteSparseValues) + 
               strlen(vectorInfo->tableName) + strlen(strIndexes) + 1;
  char strDeleteSQL[length];
  sprintf( strDeleteSQL, sqlTemplateDeleteSparseValues, 
	   vectorInfo->tableName, strIndexes);
  free(strIndexes);

  /* Execute the query */
  int success = mysql_query(sqlConn, strDeleteSQL);
  if(success != 0)
    return 0;

  int newSize;
  if( getLogicalVectorSize(sqlConn, vectorInfo, &newSize) && 
      newSize != vectorInfo->size )
       setLogicalVectorSize(sqlConn, vectorInfo, newSize);

  return 1;
}

