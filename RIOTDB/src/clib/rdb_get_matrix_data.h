#ifndef _RDB_GET_MATRIX_DATA_H
#define _RDB_GET_MATRIX_DATA_H

/*****************************************************************************
 * Contains functions for accessing elements in matrices
 *
 * Author: Herodotos Herodotou
 * Date:   Sep 23, 2008
 ****************************************************************************/

/* Templates to access matrix tables */
#define sqlTemplateGetSingleMatrixValue  "SELECT mValue FROM %s \
					  WHERE mRow = %d AND mCol = %d"

#define sqlTemplateGetAllMatrixValues    "SELECT * FROM %s ORDER BY mCol, mRow"


/* Functions to access matrix tables by element */
int getIntMatrixElement(MYSQL * sqlConn, rdbMatrix * matrixInfo, 
		  	unsigned int row, unsigned int col, 
			int * value, char * byte);

int getDoubleMatrixElement(MYSQL * sqlConn, rdbMatrix * matrixInfo, 
		    	   unsigned int row, unsigned int col, 
			   double * value, char * byte);

int execGetMatrixElementSQLCall(MYSQL * sqlConn, char* tableName, 
			  	unsigned int row, unsigned int col);


/* Functions to access matrix tables and get all elements */
int getAllIntMatrixElements(MYSQL * sqlConn, rdbMatrix * matrixInfo,
		      	    int values[], char byteArray[]);

int getAllDoubleMatrixElements(MYSQL * sqlConn, rdbMatrix * matrixInfo,
		      	       double values[], char byteArray[]);

int execGetAllMatrixElementsSQLCall(MYSQL * sqlConn, char * tableName);


#endif

