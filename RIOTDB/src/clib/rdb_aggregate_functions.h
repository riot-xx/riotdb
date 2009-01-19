#ifndef _RDB_AGGREGATE_FUNCTIONS_H
#define _RDB_AGGREGATE_FUNCTIONS_H

/*****************************************************************************
 * Contains aggregate functions for vectors (sum, avg, max, min)
 *
 * Author: Herodotos Herodotou
 * Date:   Sep 17, 2008
 ****************************************************************************/

/* Templates for SUM */
#define sqlTemplateNumericSum    "SELECT SUM(vValue) from %s"

#define sqlTemplateComplexSum    "SELECT SUM(vReal), SUM(vImag) from %s"


/* Templates for AVG */
#define sqlTemplateNumericAvg    "SELECT AVG(vValue) from %s"

#define sqlTemplateComplexAvg    "SELECT AVG(vReal), AVG(vImag) from %s"


/* Templates for MAX/MIN*/
#define sqlTemplateNumericMax    "SELECT MAX(vValue) from %s"

#define sqlTemplateNumericMin    "SELECT MIN(vValue) from %s"


/* Functions for SUM */
int getIntegerVectorSum(MYSQL * sqlConn, rdbVector * dataVector, int * sum);

int getDoubleVectorSum (MYSQL * sqlConn, rdbVector * dataVector, double * sum);

int getComplexVectorSum(MYSQL * sqlConn, rdbVector * dataVector, Rcomplex * sum);


/* Functions for AVERAGE */
int getIntegerVectorAvg(MYSQL * sqlConn, rdbVector * dataVector, double * avg);

int getDoubleVectorAvg (MYSQL * sqlConn, rdbVector * dataVector, double * avg);

int getComplexVectorAvg(MYSQL * sqlConn, rdbVector * dataVector, Rcomplex * avg);


/* Functions for MAX */
int getIntegerVectorMax(MYSQL * sqlConn, rdbVector * dataVector, int * max);

int getDoubleVectorMax (MYSQL * sqlConn, rdbVector * dataVector, double * max);

int getStringVectorMax (MYSQL * sqlConn, rdbVector * dataVector, char ** max);


/* Functions for MIN */
int getIntegerVectorMin(MYSQL * sqlConn, rdbVector * dataVector, int * min);

int getDoubleVectorMin (MYSQL * sqlConn, rdbVector * dataVector, double * min);

int getStringVectorMin (MYSQL * sqlConn, rdbVector * dataVector, char ** min);


/* Internal Method to execute the SQL queries */
int internalExecQuery(MYSQL * sqlConn, char * sqlTemplate, char * tableName);


/* Internal Methods to get SQL results */
int internalGetIntegerResult(MYSQL * sqlConn, int * result);

int internalGetDoubleResult (MYSQL * sqlConn, double * result);

int internalGetComplexResult(MYSQL * sqlConn, Rcomplex * result);

int internalGetStringResult (MYSQL * sqlConn, char ** result);


#endif

