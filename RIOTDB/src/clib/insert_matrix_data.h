#ifndef _INSERT_MATRIX_DATA_H
#define _INSERT_MATRIX_DATA_H

/*****************************************************************************
 * Contains functions for inserting data to a matrix either explicitely 
 * or load from a file
 *
 * Author: Herodotos Herodotou
 * Date:   Sep 22, 2008
 ****************************************************************************/

/* Useful Constant */
#define MAX_NUM_INT_STRING     80
#define MAX_NUM_DOUBLE_STRING  40

/* Template to load data from a local file */
#define sqlTemplateLoadIntoMatrix        "LOAD DATA LOCAL INFILE '%s' \
                                          INTO TABLE %s;"

/* Template to insert values into matrix tables */
#define sqlTemplateInsertIntoMatrix         "INSERT INTO %s VALUES %s"

#define sqlTemplateInsertIntMatrixValue       "(%d, %d, %d),"
#define sqlTemplateInsertDoubleMatrixValue    "(%d, %d, %f),"

/* Functions to load data from a local file */
int loadIntoMatrixTable(MYSQL * sqlConn, rdbMatrix * matrixInfo,
			char * filename);


/* Functions to insert values into matrix tables */
int insertIntoIntMatrixTable(MYSQL * sqlConn, rdbMatrix * matrixInfo, 
			     int * values, int size, int nRows, int nCols);

int insertIntoDoubleMatrixTable(MYSQL * sqlConn, rdbMatrix * matrixInfo, 
			     double * values, int size, int nRows, int nCols);


/* Functions to insert sequences into matrix tables */
int insertSeqIntMatrixTable(MYSQL * sqlConn, rdbMatrix * matrixInfo, 
			    int start, int end, int step, 
			    int nRows, int nCols);

int insertSeqDoubleMatrixTable(MYSQL * sqlConn, rdbMatrix * matrixInfo, 
			    double start, double end, double step, 
			    int nRows, int nCols);

/* ---------------------------------------------------- */
/* Helper Functions to insert values into matrix tables */
int insertPartIntMatrixTable(MYSQL * sqlConn, rdbMatrix * matrixInfo, 
			     int * values, int size, int numElems,
			     int startRow, int startCol, int nRows, int nCols);

int insertPartDoubleMatrixTable(MYSQL * sqlConn, rdbMatrix * matrixInfo, 
			     double * values, int size, int numElems,
			     int startRow, int startCol, int nRows, int nCols);

/* Other Helper functions */
int internalInsertIntoMatrixTable(MYSQL * sqlConn, char * sqlTemplate, 
				  char * tableName, char * strValues);



#endif

