#ifndef _VECTOR_ELEMENT_FUNCTIONS_H
#define _VECTOR_ELEMENT_FUNCTIONS_H

/*****************************************************************************
 * Contains element-wise functions for vectors (sin, cos, pow, sqrt)
 *
 * Author: Herodotos Herodotou
 * Date:   Sep 17, 2008
 ****************************************************************************/

/* Functions */
#define SIN_OP	"SIN"
#define COS_OP	"COS"
#define SQRT_OP	"SQRT"


/* Templates for functions */
#define sqlTemplateUnaryFunction  "SELECT vIndex, %s(vValue) from %s"

#define sqlTemplatePowFunction    "SELECT vIndex, POW(vValue,%f) from %s"


/* Templates for Complex Trigonometric functions */
#define sqlTemplateComplexSIN    "SELECT vIndex, \
                                  sin(vReal)*(exp(vImag)+exp(-vImag))/2, \
                                  cos(vReal)*(exp(vImag)-exp(-vImag))/2  \
                                  FROM %s"

#define sqlTemplateComplexCOS    "SELECT vIndex, \
                                  cos(vReal)*(exp(-vImag)+exp(vImag))/2, \
                                  sin(vReal)*(exp(-vImag)-exp(vImag))/2  \
                                  FROM %s"


/* General Functions */
int performNumericPow(MYSQL* sqlConn, rdbVector* resultVector,
		      rdbVector* dataVector, double exponent);

int performNumericSqrt(MYSQL* sqlConn, rdbVector* resultVector,
		       rdbVector* dataVector);

int performNumericSin(MYSQL * sqlConn, rdbVector * dataVector,
		      rdbVector * resultVector);

int performNumericCos(MYSQL * sqlConn, rdbVector * dataVector,
		      rdbVector * resultVector);

int performComplexSin(MYSQL * sqlConn, rdbVector * dataVector,
		      rdbVector * resultVector);

int performComplexCos(MYSQL * sqlConn, rdbVector * dataVector,
		      rdbVector * resultVector);

/* Internal Methods */
int internalPerformNumericFunction(MYSQL * sqlConn, rdbVector * dataVector,
			           rdbVector * resultVector, char * function);

int internalPerformComplexTrig(MYSQL * sqlConn, rdbVector * dataVector,
			       rdbVector * resultVector, char * sqlTemplate);



#endif

