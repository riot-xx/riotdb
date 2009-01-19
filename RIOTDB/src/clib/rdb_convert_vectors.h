#ifndef _RDB_CONVERT_VECTORS_H
#define _RDB_CONVERT_VECTORS_H

/*****************************************************************************
 * Contains functions for performing data type conversions for vectors.
 *
 * Author: Herodotos Herodotou
 * Date:   Sep 17, 2008
 ****************************************************************************/

/* Template for data type convertions */
#define sqlTemplateNumericToComplex  "SELECT vIndex, vValue, 0 FROM %s"

#define sqlTemplateDoubleToInteger   "SELECT vIndex, floor(vValue) FROM %s"

#define sqlTemplateGenericConvert    "SELECT vIndex, vValue FROM %s"


/* Functions for data type convertions */
int convertNumericToComplex(MYSQL * sqlConn, rdbVector * numericVector, 
			    rdbVector * complexVector);

int convertDoubleToInteger(MYSQL * sqlConn, rdbVector * doubleVector, 
			   rdbVector * integerVector);

int convertIntegerToDouble(MYSQL * sqlConn, rdbVector * integerVector, 
			   rdbVector * doubleVector);

int convertLogicToInteger(MYSQL * sqlConn, rdbVector * logicVector, 
			  rdbVector * integerVector);

int convertLogicToDouble(MYSQL * sqlConn, rdbVector * logicVector, 
			 rdbVector * doubleVector);

int internalConvertVectors (MYSQL * sqlConn, rdbVector * fromVector, 
			    rdbVector * toVector, char * sqlTemplate, int type);


#endif

