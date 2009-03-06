#ifndef _DELETE_VECTOR_DATA_H
#define _DELETE_VECTOR_DATA_H

/*****************************************************************************
 * Contains functions for deleting elements from vectors (delete all, 
 * single values, range of values or sparse values)
 *
 * Author: Herodotos Herodotou
 * Date:   Sep 17, 2008
 ****************************************************************************/

/* Templates to delete elements from vector tables */
#define sqlTemplateDeleteAllValues      "DELETE FROM %s"

#define sqlTemplateDeleteSingleValue    "DELETE FROM %s WHERE vIndex = %d"

#define sqlTemplateDeleteRangeValues    "DELETE FROM %s \
                                         WHERE vIndex >= %d AND vIndex <= %d"

#define sqlTemplateDeleteSparseValues   "DELETE FROM %s WHERE %s"

#define sqlTemplateSparseVIndex         "vIndex = %d"
#define sqlTemplateSparseVIndexOR       "vIndex = %d OR "

/* Functions to delete elements from vector tables */
int deleteAllElements(MYSQL * sqlConn, rdbVector * vectorInfo);

int deleteSingleElement(MYSQL * sqlConn, rdbVector * vectorInfo, 
			unsigned int index);

int deleteRangeElements(MYSQL * sqlConn, rdbVector * vectorInfo, 
		        unsigned int greaterThan,  unsigned int lessThan); 

int deleteSparseElements(MYSQL * sqlConn, rdbVector * vectorInfo,
			 unsigned int indexes[], int size);


#endif

