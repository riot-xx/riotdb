#ifndef _RDB_ELEMENT_FUNCTIONS_H
#define _RDB_ELEMENT_FUNCTIONS_H


/* Templates for Trigonometric functions */
#define sqlTemplateNumericSIN    "SELECT vIndex, SIN(vValue) FROM %s"

#define sqlTemplateNumericCOS    "SELECT vIndex, COS(vValue) FROM %s"

#define sqlTemplateComplexSIN    "SELECT vIndex, \
                                  sin(vReal)*(exp(vImag)+exp(-vImag))/2, \
                                  cos(vReal)*(exp(vImag)-exp(-vImag))/2  \
                                  FROM %s"

#define sqlTemplateComplexCOS    "SELECT vIndex, \
                                  cos(vReal)*(exp(-vImag)+exp(vImag))/2, \
                                  sin(vReal)*(exp(-vImag)-exp(vImag))/2  \
                                  FROM %s"

/* Functions for Trigonometric functions  */
int performNumericSin(MYSQL * sqlConn,rdbVector * dataVector,
		      rdbVector * resultVector);

int performNumericCos(MYSQL * sqlConn,rdbVector * dataVector,
		      rdbVector * resultVector);

int performComplexSin(MYSQL * sqlConn,rdbVector * dataVector,
		      rdbVector * resultVector);

int performComplexCos(MYSQL * sqlConn,rdbVector * dataVector,
		      rdbVector * resultVector);


/* Internal Method for all trig functions */
int internalPerformNumericTrig(MYSQL * sqlConn,rdbVector * dataVector,
			       rdbVector * resultVector, char * sqlTemplate);

int internalPerformComplexTrig(MYSQL * sqlConn,rdbVector * dataVector,
			       rdbVector * resultVector, char * sqlTemplate);



#endif

