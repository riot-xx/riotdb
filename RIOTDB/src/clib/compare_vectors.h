#ifndef _COMPARE_VECTORS_H
#define _COMPARE_VECTORS_H

/*****************************************************************************
 * Contains functions for vector comparisons with other vectors or values.
 *
 * Author: Herodotos Herodotou
 * Date:   Sep 17, 2008
 ****************************************************************************/

/* Templates to perform comparisons */
#define sqlTemplateCompareWDBVector "SELECT T1.vIndex, \
                                     IF( (T1.vIndex %% %d = T2.vIndex %% %d \
                                          AND T1.vValue %s T2.vValue), \
                                          TRUE, FALSE ) \
                                     FROM %s AS T1, %s AS T2 \
                                     WHERE T1.vIndex %% %d = T2.vIndex %% %d;"

#define sqlTemplateCompareWithValues "SELECT vIndex, IF( %s, TRUE, FALSE ) \
                                      FROM %s"


/* Templates to build strings for comparisons */
#define sqlTemplateIntegerComparisonOR "(vIndex %% %d = %d AND vValue %s %d) OR "
#define sqlTemplateDoubleComparisonOR  "(vIndex %% %d = %d AND vValue %s %lf) OR "
#define sqlTemplateStringComparisonOR  "(vIndex %% %d = %d AND vValue %s '%s') OR "

#define sqlTemplateIntegerComparison   "(vIndex %% %d = %d AND vValue %s %d)"
#define sqlTemplateDoubleComparison    "(vIndex %% %d = %d AND vValue %s %lf)"
#define sqlTemplateStringComparison    "(vIndex %% %d = %d AND vValue %s '%s')"



/* Functions to perform comparisons */
int compareWithDBVector(MYSQL * sqlConn, rdbVector * resultVector,
			rdbVector * dataVector, rdbVector * compareVector,
			char * comparisonOp);

int compareWithIntegerValues(MYSQL * sqlConn, rdbVector * resultVector,
			     rdbVector * dataVector, int compareValues[],
			     int size, char * comparisonOp);

int compareWithDoubleValues(MYSQL * sqlConn, rdbVector * resultVector,
			    rdbVector * dataVector, double compareValues[],
			    int size, char * comparisonOp);

int compareWithStringValues(MYSQL * sqlConn, rdbVector * resultVector,
			    rdbVector * dataVector, char * compareValues[],
			    int size, char * comparisonOp);

int internalCompareWithValues(MYSQL * sqlConn, rdbVector * resultVector,
			      rdbVector * dataVector, char * strCompareSQL);


/* Functions to build strings for comparisons */
void buildIntegerComparisonString(char ** strComparison, int compareValues[],
				  int size, char * comparisonOp);

void buildDoubleComparisonString(char ** strComparison, double compareValues[],
				 int size, char * comparisonOp);

void buildStringComparisonString(char ** strComparison, char* compareValues[],
				 int size, char * comparisonOp);


#endif

