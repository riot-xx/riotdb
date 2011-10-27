#ifndef _SET_VECTOR_DATA_H
#define _SET_VECTOR_DATA_H

/*****************************************************************************
 * Contains functions for updating elements in an existing vector (setting a
 * single value, range of values, sparce values, with dbvectors containing
 * indexes and logic vectors).
 *
 * Author: Herodotos Herodotou
 * Date:   Sep 17, 2008
 ****************************************************************************/

/* Templates to update vector tables */
#define sqlTemplateInsertIntElement       "INSERT INTO %s VALUES (%d,%d)"

#define sqlTemplateInsertDoubleElement    "INSERT INTO %s VALUES (%d,%lf)"

#define sqlTemplateInsertStringElement    "INSERT INTO %s VALUES (%d,'%s')"

#define sqlTemplateInsertComplexElement   "INSERT INTO %s VALUES (%d,%lf,%lf)"

#define sqlTemplateInsertLogicElement     "INSERT INTO %s VALUES (%d,%c)"


#define sqlTemplateUpdateIntElement       "UPDATE %s SET vValue = %d \
                                           WHERE vIndex = %d"

#define sqlTemplateUpdateDoubleElement    "UPDATE %s SET vValue = %lf \
                                           WHERE vIndex = %d"

#define sqlTemplateUpdateStringElement    "UPDATE %s SET vValue = '%s' \
                                           WHERE vIndex = %d"

#define sqlTemplateUpdateComplexElement   "UPDATE %s SET vReal=%lf, vImag=%lf \
                                           WHERE vIndex = %d"

#define sqlTemplateUpdateLogicElement     "UPDATE %s SET vValue = %c \
                                           WHERE vIndex = %d"


/* Functions to update vector tables by element */
int setIntElement(MYSQL * sqlConn, rdbVector * vectorInfo,
		  unsigned int index, int value);

int setDoubleElement(MYSQL * sqlConn, rdbVector * vectorInfo,
		     unsigned  int index, double value);

int setStringElement(MYSQL * sqlConn, rdbVector * vectorInfo,
		     unsigned int index, char * value);

int setComplexElement(MYSQL * sqlConn, rdbVector * vectorInfo,
		      unsigned int index, Rcomplex value);

int setLogicElement(MYSQL * sqlConn, rdbVector * vectorInfo,
		    unsigned int index, int value);

/* Functions to update vector tables with ranges */
int setRangeIntElements(MYSQL * sqlConn, rdbVector * vectorInfo,
		        unsigned int greaterThan,  unsigned int lessThan,
			int values[], int numValues);

int setRangeDoubleElements(MYSQL * sqlConn, rdbVector * vectorInfo,
			   unsigned int greaterThan,  unsigned int lessThan,
			   double values[], int numValues);

int setRangeStringElements(MYSQL * sqlConn, rdbVector * vectorInfo,
			   unsigned int greaterThan,  unsigned int lessThan,
			   char* values[], int numValues);

int setRangeComplexElements(MYSQL * sqlConn, rdbVector * vectorInfo,
			    unsigned int greaterThan,  unsigned int lessThan,
			    Rcomplex values[], int numValues);

int setRangeLogicElements(MYSQL * sqlConn, rdbVector * vectorInfo,
			  unsigned int greaterThan,  unsigned int lessThan,
			  int values[], int numValues);


/* Functions to update some elements in the vector tables */
int setSparseIntElements(MYSQL * sqlConn, rdbVector * vectorInfo,
			 unsigned int indexes[], int numIndexes,
			 int values[], int numValues);

int setSparseDoubleElements(MYSQL * sqlConn, rdbVector * vectorInfo,
			    unsigned int indexes[], int numIndexes,
			    double values[], int numValues);

int setSparseStringElements(MYSQL * sqlConn, rdbVector * vectorInfo,
			    unsigned int indexes[], int numIndexes,
			    char * values[], int numValues);

int setSparseComplexElements(MYSQL * sqlConn, rdbVector * vectorInfo,
			     unsigned int indexes[], int numIndexes,
			     Rcomplex values[], int numValues);

int setSparseLogicElements(MYSQL * sqlConn, rdbVector * vectorInfo,
			   unsigned int indexes[], int numIndexes,
			   int values[], int numValues);


/* Functions to update some elements using a DBVector with indexes */
int setIntElementsWDBVector(MYSQL * sqlConn, rdbVector * vectorInfo,
			    rdbVector * indexVector,
			    int values[], int numValues);

int setDoubleElementsWDBVector(MYSQL * sqlConn, rdbVector * vectorInfo,
			       rdbVector * indexVector,
			       double values[], int numValues);

int setStringElementsWDBVector(MYSQL * sqlConn, rdbVector * vectorInfo,
			       rdbVector * indexVector,
			       char * values[], int numValues);

int setComplexElementsWDBVector(MYSQL * sqlConn, rdbVector * vectorInfo,
				rdbVector * indexVector,
				Rcomplex values[], int numValues);

int setLogicElementsWDBVector(MYSQL * sqlConn, rdbVector * vectorInfo,
			      rdbVector * indexVector,
			      int values[], int numValues);


/* Functions to update some elements using a Logic Vector as indexes */
int setIntElementsWithLogic(MYSQL * sqlConn, rdbVector * vectorInfo,
			    rdbVector * logicVector,
			    int values[], int numValues);

int setDoubleElementsWithLogic(MYSQL * sqlConn, rdbVector * vectorInfo,
			       rdbVector * logicVector,
			       double values[], int numValues);

int setStringElementsWithLogic(MYSQL * sqlConn, rdbVector * vectorInfo,
			       rdbVector * logicVector,
			       char * values[], int numValues);

int setComplexElementsWithLogic(MYSQL * sqlConn, rdbVector * vectorInfo,
				rdbVector * logicVector,
				Rcomplex values[], int numValues);

int setLogicElementsWithLogic(MYSQL * sqlConn, rdbVector * vectorInfo,
			      rdbVector * logicVector,
			      int values[], int numValues);



/* ---------------------------------- */
/* Internal functions for the setters */
int internalSetIntElement(MYSQL * sqlConn, rdbVector * vectorInfo,
			  unsigned int index, int value);

int internalSetDoubleElement(MYSQL * sqlConn, rdbVector * vectorInfo,
			     unsigned  int index, double value);

int internalSetStringElement(MYSQL * sqlConn, rdbVector * vectorInfo,
			     unsigned int index, char * value);

int internalSetComplexElement(MYSQL * sqlConn, rdbVector * vectorInfo,
			      unsigned int index, Rcomplex value);

int internalSetLogicElement(MYSQL * sqlConn, rdbVector * vectorInfo,
			    unsigned int index, int value);


/* Internal functions to perform insertions of new values */
int internalInsertIntElement(MYSQL * sqlConn, rdbVector * vectorInfo,
			     unsigned int index, int value);

int internalInsertDoubleElement(MYSQL * sqlConn, rdbVector * vectorInfo,
				unsigned int index, double value);

int internalInsertStringElement(MYSQL * sqlConn, rdbVector * vectorInfo,
				unsigned int index, char* value);

int internalInsertComplexElement(MYSQL * sqlConn, rdbVector * vectorInfo,
				 unsigned int index, Rcomplex value);

int internalInsertLogicElement(MYSQL * sqlConn, rdbVector * vectorInfo,
			       unsigned int index, int value);


/* Internal functions to perform updates of old values */
int internalUpdateIntElement(MYSQL * sqlConn, rdbVector * vectorInfo,
			     unsigned int index, int value);

int internalUpdateDoubleElement(MYSQL * sqlConn, rdbVector * vectorInfo,
				unsigned int index, double value);

int internalUpdateStringElement(MYSQL * sqlConn, rdbVector * vectorInfo,
				unsigned int index, char* value);

int internalUpdateComplexElement(MYSQL * sqlConn, rdbVector * vectorInfo,
				 unsigned int index, Rcomplex value);

int internalUpdateLogicElement(MYSQL * sqlConn, rdbVector * vectorInfo,
			       unsigned int index, int value);


#endif
