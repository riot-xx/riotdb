#ifndef _RDB_DELETE_VECTOR_DATA_H
#define _RDB_DELETE_VECTOR_DATA_H


/* Templates to delete elements from vector tables */
#define sqlTemplateDeleteAllValues      "DELETE FROM %s"

#define sqlTemplateDeleteSingleValue    "DELETE FROM %s WHERE vIndex = %d"

#define sqlTemplateDeleteRangeValues    "DELETE FROM %s \
                                         WHERE vIndex >= %d AND vIndex <= %d"

#define sqlTemplateDeleteSparseValues   "DELETE FROM %s WHERE %s"

/* Functions to delete elements from vector tables */
int deleteAllElements(MYSQL * sqlConn, rdbVector * vectorInfo);

int deleteSingleElement(MYSQL * sqlConn, rdbVector * vectorInfo, 
			unsigned int index);

int deleteRangeElements(MYSQL * sqlConn, rdbVector * vectorInfo, 
		        unsigned int greaterThan,  unsigned int lessThan); 

int deleteSparseElements(MYSQL * sqlConn, rdbVector * vectorInfo,
			 unsigned int indexes[], int size);


#endif

