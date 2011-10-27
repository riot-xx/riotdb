#ifndef _INSERT_VECTOR_DATA_H
#define _INSERT_VECTOR_DATA_H

/*****************************************************************************
 * Contains functions for inserting data to a vector either explicitely
 * or load from a file
 *
 * Author: Herodotos Herodotou
 * Date:   Sep 17, 2008
 ****************************************************************************/

/* Useful Constant */
#define MAX_NUM_INTEGERS 200
#define MAX_NUM_DOUBLES  100
#define MAX_NUM_STRINGS  50
#define MAX_NUM_COMPLEX  100
#define MAX_NUM_LOGIC    800


/* Template to load data from a local file */
#define sqlTemplateLoadIntoVector        "LOAD DATA LOCAL INFILE '%s' \
                                         INTO TABLE %s (vValue);"

#define sqlTemplateLoadComplexVector     "LOAD DATA LOCAL INFILE '%s' \
                                         INTO TABLE %s (vReal, vImag);"


/* Template to insert values into vector tables */
#define sqlTemplateInsertVector         "INSERT INTO %s (vValue) VALUES %s"

#define sqlTemplateInsertComplexVector  "INSERT INTO %s (vReal, vImag) VALUES %s"



/* Functions to load data from a local file */
int loadIntoVectorTable(MYSQL * sqlConn, rdbVector * vectorInfo,
			char * filename);

int loadIntoComplexVectorTable(MYSQL * sqlConn, rdbVector * vectorInfo,
			       char * filename);

int internalLoadIntoAnyVectorTable(MYSQL * sqlConn, rdbVector * vectorInfo,
				   char * sqlTemplate, char * filename);


/* Functions to insert values into vector tables */
int insertIntoIntVectorTable(MYSQL * sqlConn, rdbVector * vectorInfo,
			     int * values, int size);

int insertIntoDoubleVectorTable(MYSQL * sqlConn, rdbVector * vectorInfo,
				double * values, int size);

int insertIntoStringVectorTable(MYSQL * sqlConn, rdbVector * vectorInfo,
				char ** values, int size);

int insertIntoComplexVectorTable(MYSQL * sqlConn, rdbVector * vectorInfo,
				 double * realValues, int numReals,
				 double *imagValues, int numImags,
				 int desiredSize);

int insertIntoLogicVectorTable(MYSQL * sqlConn, rdbVector * vectorInfo,
			       int * values, int size);


/* Functions to insert sequences into vector tables */
int insertSeqIntVectorTable(MYSQL * sqlConn, rdbVector * vectorInfo,
			    int start, int end, int step);

int insertSeqDoubleVectorTable(MYSQL * sqlConn, rdbVector * vectorInfo,
			       double start, double end, double step);

int insertSeqLogicVectorTable(MYSQL * sqlConn, rdbVector * vectorInfo,
			      int value, int repeats);

/* ---------------------------------------------------- */
/* Helper Functions to insert values into vector tables */
int insertPartIntVectorTable(MYSQL * sqlConn, rdbVector * vectorInfo,
			     int * values, int left, int right);

int insertPartDoubleVectorTable(MYSQL * sqlConn, rdbVector * vectorInfo,
				double * values, int left, int right);

int insertPartStringVectorTable(MYSQL * sqlConn, rdbVector * vectorInfo,
				char ** values, int left, int right);

int insertPartComplexVectorTable(MYSQL * sqlConn, rdbVector * vectorInfo,
				 double * realValues, int numReals,
				 double *imagValues, int numImags,
				 int left, int right);

int insertPartLogicVectorTable(MYSQL * sqlConn, rdbVector * vectorInfo,
			       int * values, int left, int right);


/* Other Helper functions */
int insertIntoVectorTable(MYSQL * sqlConn, char * sqlTemplate,
			  char * tableName, char * strValues);



#endif

