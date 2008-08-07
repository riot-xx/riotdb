#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <mysql.h>
#include "rdb_basics.h"
#include "rdb_handle_vector_tables.h"
#include "rdb_handle_vector_views.h"
#include "rdb_handle_vectors.h"


/*---------------  Functions to create vector tables ---------------- */

int createNewIntVectorTable(MYSQL * sqlConn, rdbVector * vectorInfo)
{
  vectorInfo->sxp_type = SXP_TYPE_INTEGER;
  return internalCreateNewVectorTable(sqlConn, vectorInfo, 
				      sqlTemplateCreateIntVector);
}


int createNewDoubleVectorTable(MYSQL * sqlConn, rdbVector * vectorInfo)
{
  vectorInfo->sxp_type = SXP_TYPE_DOUBLE;
  return internalCreateNewVectorTable(sqlConn, vectorInfo, 
				      sqlTemplateCreateDoubleVector);
}


int createNewComplexVectorTable(MYSQL * sqlConn, rdbVector * vectorInfo)
{
  vectorInfo->sxp_type = SXP_TYPE_COMPLEX;
  return internalCreateNewVectorTable(sqlConn, vectorInfo, 
				      sqlTemplateCreateComplexVector);
}


int createNewStringVectorTable(MYSQL * sqlConn, rdbVector * vectorInfo)
{
  vectorInfo->sxp_type = SXP_TYPE_STRING;
  return internalCreateNewVectorTable(sqlConn, vectorInfo, 
				      sqlTemplateCreateStringVector);
}


int createNewLogicVectorTable(MYSQL * sqlConn, rdbVector * vectorInfo)
{
  vectorInfo->sxp_type = SXP_TYPE_LOGIC;
  return internalCreateNewVectorTable(sqlConn, vectorInfo, 
				      sqlTemplateCreateLogicVector);
}


int internalCreateNewVectorTable(MYSQL * sqlConn, rdbVector * vectorInfo, 
				 char * sqlTemplate)
{
  /* Build the name of the new table */
  if( !buildUniqueTableName(sqlConn, &(vectorInfo->tableName)) )
    return 0;

  /* Build the sql string */
  int length = strlen(sqlTemplate) + strlen(vectorInfo->tableName) + 1;
  char strCreateVectorSQL[length];
  sprintf( strCreateVectorSQL, sqlTemplate, vectorInfo->tableName );

  /* Execute the query to create the table*/
  int success = mysql_query(sqlConn, strCreateVectorSQL);
  if( success != 0 )
     return 0;

  /* Insert info into Metadata table */
  success = insertMetadataInfo(sqlConn, vectorInfo);

  return success;
}


/* ---------------- Functions to drop vector tables ---------------- */

int dropVectorTable(MYSQL * sqlConn, rdbVector * vectorInfo)
{
  /* Safe guard - only drop table if num references == 0 */
  int refCounter = 0;
  getViewRefCount(sqlConn, vectorInfo, &refCounter);
  if( refCounter != 0 )
  {
     vectorInfo->refCounter = refCounter;
     return setRefCounter(sqlConn, vectorInfo, refCounter);
  }

  /* Build the sql string and drop the table */
  int length = strlen(sqlTemplateDropTable) + 
               strlen(vectorInfo->tableName) + 1;
  char strDropTableSQL[length];
  sprintf( strDropTableSQL, sqlTemplateDropTable, vectorInfo->tableName );

  int success = mysql_query(sqlConn, strDropTableSQL);
  if( success != 0 )
     return 0;

  /* Delete corresponding metadata info */
  success = deleteMetadataInfo(sqlConn, vectorInfo);

  return success;
}


/* ------------------ Functions for naming tables ------------------- */

int buildUniqueTableName(MYSQL * sqlConn, char ** newTableName)
{
  int next = 0;
  if( !getNextVectorID(sqlConn, &next) )
    return 0;

  /* Build the name of the new table */
   if( *newTableName != NULL )
      free(*newTableName);
   int length = strlen(sqlTemplateTableName) + MAX_INT_LENGTH;
  *newTableName = (char*) malloc(length * sizeof(char));
  sprintf( *newTableName, sqlTemplateTableName, next);

  return 1;
}

