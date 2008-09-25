#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include "rdb_sort_vector.h"
#include "rdb_handle_metadata.h"

int sortVector(MYSQL *conn, rdbVector *aInfo, rdbVector *bInfo, int desc)
{
	if(!buildUniqueVectorTableName(conn, &(bInfo->tableName)))
		return 0;

	bInfo->sxp_type = aInfo->sxp_type;
	bInfo->size = aInfo->size;
	
	int length;
	int success = 0;
	switch(aInfo->sxp_type)
	{
	case SXP_TYPE_DOUBLE:
	  length = strlen(sqlTemplateCreateSortVector)+strlen("double")+strlen(bInfo->tableName);
	  
		char str[length];
		
		sprintf(str, sqlTemplateCreateSortVector,bInfo->tableName, "double");

		if (mysql_query(conn, str)) {
		  fprintf(stderr, "create sorted results failed: %u (%s)\n", mysql_errno(conn), mysql_error(conn));
		  success = 1;
		}		  
		length = strlen(sqlTemplateSortVector)+strlen(bInfo->tableName)+strlen(aInfo->tableName)+4+1;
		char str1[length];
		sprintf(str1, sqlTemplateSortVector, bInfo->tableName, aInfo->tableName, desc?"desc":"asc");
		if(mysql_query(conn, str1)) {
		  fprintf(stderr, "insert sorted results failed: %u (%s)\n", mysql_errno(conn), mysql_error(conn));
		  success = 1;
		}
		/*mysql_affected_rows(conn);*/
		if (success)
			return 0;
		success = insertVectorMetadataInfo(conn, bInfo);
		return success;
		break;
	}
	return 0;
}
	
