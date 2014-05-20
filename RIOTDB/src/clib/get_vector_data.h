#ifndef _GET_VECTOR_DATA_H
#define _GET_VECTOR_DATA_H

/*****************************************************************************
 * Contains functions for accessing elements in vectors (get all, single
 * values, range of values, sparse values, with other dbvectors as indexes
 * or with logic dbvectors)
 *
 * Author: Herodotos Herodotou
 * Date:   Sep 17, 2008
 ****************************************************************************/

/* Templates to access vector tables */
#define sqlTemplateGetAllValues          "SELECT * FROM %s ORDER BY vIndex"

#define sqlTemplateGetSingleValue        "SELECT * FROM %s WHERE vIndex = %d"

#define sqlTemplateGetRangeValues        "SELECT * FROM %s \
                                          WHERE vIndex >= %d AND vIndex <= %d \
                                          ORDER BY vIndex"

#define sqlTemplateGetSparseValues       "SELECT * FROM %s WHERE %s"


/* Templates to access vector tables using other DBVectors as indexes */
#define sqlTemplateGetValuesWithDBVec    "SELECT I.vIndex, D.vValue \
                                          FROM %s AS D, %s AS I \
                                          WHERE D.vIndex = floor(I.vValue) \
                                          ORDER BY I.vIndex"

#define sqlTemplateGetCplxValuesWithDBVec "SELECT I.vIndex, D.vReal, D.vImag \
                                          FROM %s AS D, %s AS I \
                                          WHERE D.vIndex = floor(I.vValue) \
                                          ORDER BY I.vIndex"

/* Templates to access vector tables using logic DBVector */
#define sqlTemplateGetValuesWithLogic "SELECT DT.vIndex, DT.vValue \
                    FROM %s AS DT, %s AS LT \
                    WHERE DT.vIndex %% %d = LT.vIndex %% %d \
                    AND LT.vValue = 1 ORDER BY DT.vIndex"

#define sqlTemplateGetCplxValuesWithLogic "SELECT DT.vIndex, DT.vReal, DT.vImag \
                    FROM %s AS DT, %s AS LT \
                    WHERE DT.vIndex %% %d = LT.vIndex %% %d \
                    AND LT.vValue = 1 ORDER BY DT.vIndex"

#define sqlTemplateTransformResults "SELECT (floor(RT.vIndex / %d) * \
              (SELECT COUNT(*) FROM %s WHERE vValue = 1) + \
              (SELECT COUNT(*) FROM %s as LT \
                 WHERE LT.vIndex <= RT.vIndex %% %d \
                       AND LT.vValue = 1 )), \
              RT.vValue FROM %s AS RT"

#define sqlTemplateTransformCplxResults "SELECT (floor(RT.vIndex / %d) * \
              (SELECT COUNT(*) FROM %s WHERE vValue = 1) + \
              (SELECT COUNT(*) FROM %s as LT \
                 WHERE LT.vIndex <= RT.vIndex %% %d \
                       AND LT.vValue = 1 )), \
              RT.vReal, RT.vImag FROM %s AS RT"


/* Other templates */
#define sqlTemplateVIndexOR   "vIndex = %d OR "
#define sqlTemplateVIndex     "vIndex = %d"


/* Functions to access vector tables by element */
int getIntElement(MYSQL * sqlConn, rdbVector * vectorInfo,
		  unsigned int index, int * value, char * byte);

int getDoubleElement(MYSQL * sqlConn, rdbVector * vectorInfo,
		     unsigned  int index, double * value, char * byte);

int getStringElement(MYSQL * sqlConn, rdbVector * vectorInfo,
		     unsigned int index, char ** value, char * byte);

int getComplexElement(MYSQL * sqlConn, rdbVector * vectorInfo,
		      unsigned int index, Rcomplex * value, char * byte);

int getLogicElement(MYSQL * sqlConn, rdbVector * vectorInfo,
		    unsigned int index, int * value, char * byte);

int execGetElementSQLCall(MYSQL * sqlConn, char* tableName, unsigned int index);


/* Functions to access vector tables and get all elements */
int getAllIntElements(MYSQL * sqlConn, rdbVector * vectorInfo,
		      int values[], char byteArray[]);

int getAllDoubleElements(MYSQL * sqlConn, rdbVector * vectorInfo,
			 double values[], char byteArray[]);

int getAllStringElements(MYSQL * sqlConn, rdbVector * vectorInfo,
			 char * values[], char byteArray[]);

int getAllComplexElements(MYSQL * sqlConn, rdbVector * vectorInfo,
			  Rcomplex values[], char byteArray[]);

int getAllLogicElements(MYSQL * sqlConn, rdbVector * vectorInfo,
			int values[], char byteArray[]);

int execGetAllElementsSQLCall(MYSQL * sqlConn, char * tableName);


/* Functions to access vector tables with ranges */
int getRangeIntElements(MYSQL * sqlConn, rdbVector * vectorInfo,
		        unsigned int greaterThan,  unsigned int lessThan,
			int values[], char byteArray[]);

int getRangeDoubleElements(MYSQL * sqlConn, rdbVector * vectorInfo,
			   unsigned int greaterThan,  unsigned int lessThan,
			   double values[], char byteArray[]);

int getRangeStringElements(MYSQL * sqlConn, rdbVector * vectorInfo,
			   unsigned int greaterThan,  unsigned int lessThan,
			   char* values[], char byteArray[]);

int getRangeComplexElements(MYSQL * sqlConn, rdbVector * vectorInfo,
			    unsigned int greaterThan,  unsigned int lessThan,
			    Rcomplex values[], char byteArray[]);

int getRangeLogicElements(MYSQL * sqlConn, rdbVector * vectorInfo,
			  unsigned int greaterThan,  unsigned int lessThan,
			  int values[], char byteArray[]);

int execGetRangeElementsSQLCall(MYSQL * sqlConn, char * tableName,
			     unsigned int greaterThan, unsigned int lessThan);


/* Functions to access vector tables and get some elements */
int getSparseIntElements(MYSQL * sqlConn, rdbVector * vectorInfo,
			 unsigned int indexes[], int size, int values[],
			 char byteArray[]);

int getSparseDoubleElements(MYSQL * sqlConn, rdbVector * vectorInfo,
			    unsigned int indexes[], int size, double values[],
			    char byteArray[]);

int getSparseStringElements(MYSQL * sqlConn, rdbVector * vectorInfo,
			    unsigned int indexes[], int size, char * values[],
			    char byteArray[]);

int getSparseComplexElements(MYSQL * sqlConn, rdbVector * vectorInfo,
			     unsigned int indexes[], int size,
			     Rcomplex values[], char byteArray[]);

int getSparseLogicElements(MYSQL * sqlConn, rdbVector * vectorInfo,
			   unsigned int indexes[], int size, int values[],
			   char byteArray[]);

int execGetSparseElementsSQLCall(MYSQL * sqlConn, char * tableName,
				 unsigned int indexes[], int size);


/* Functions to access vector tables using DBVectors */
int getIntElementsWithDBVector(MYSQL * sqlConn, rdbVector * dataVector,
			       rdbVector * indexVector, rdbVector * resultVector);

int getDoubleElementsWithDBVector(MYSQL * sqlConn, rdbVector * dataVector,
			       rdbVector * indexVector, rdbVector * resultVector);

int getStringElementsWithDBVector(MYSQL * sqlConn, rdbVector * dataVector,
			       rdbVector * indexVector, rdbVector * resultVector);

int getComplexElementsWithDBVector(MYSQL * sqlConn, rdbVector * dataVector,
			       rdbVector * indexVector, rdbVector * resultVector);

int getLogicElementsWithDBVector(MYSQL * sqlConn, rdbVector * dataVector,
				rdbVector * indexVector, rdbVector * resultVector);

void buildGetElemWithDBVectorSQL(char ** strGetElems, char * sqlTemplate,
				rdbVector* dataVector, rdbVector* indexVector);


/* Functions to access vector tables using Logic DBVectors */
int getIntElementsWithLogic(MYSQL * sqlConn, rdbVector * dataVector,
			     rdbVector * logicVector, rdbVector * resultVector);

int getDoubleElementsWithLogic(MYSQL * sqlConn, rdbVector * dataVector,
			     rdbVector * logicVector, rdbVector * resultVector);

int getStringElementsWithLogic(MYSQL * sqlConn, rdbVector * dataVector,
			     rdbVector * logicVector, rdbVector * resultVector);

int getComplexElementsWithLogic(MYSQL * sqlConn, rdbVector * dataVector,
			     rdbVector * logicVector, rdbVector * resultVector);

int getLogicElementsWithLogic(MYSQL * sqlConn, rdbVector * dataVector,
			      rdbVector * logicVector, rdbVector * resultVector);

int internalGetElementsWithLogic(MYSQL * sqlConn, rdbVector * dataVector,
				 rdbVector * logicVector, rdbVector * resultVector,
				 char * sqlTemplateTemp, char * sqlTemplateFinal);

void buildGetElemWithLogicSQL(char ** strGetElems, char * sqlTemplate,
			      rdbVector* dataVector, rdbVector* logicVector);

void buildTranformResultsSQL(char ** strResults, char * sqlTemplate,
			     rdbVector* resultsVector, rdbVector* logicVector);



#endif

