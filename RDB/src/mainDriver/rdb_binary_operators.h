#ifndef _RDB_BINARY_OPERATORS_H
#define _RDB_BINARY_OPERATORS_H

/*****************************************************************************
 * Contains functions for binary operators for vectors (+, -, *, /, %)
 *
 * Author: Herodotos Herodotou
 * Date:   Sep 17, 2008
 ****************************************************************************/

/* Mathematical Signs */
#define PLUS_SIGN   "+"
#define MINUS_SIGN  "-"
#define MULT_SIGN   "*"
#define DIV_SIGN    "/"
#define MOD_SIGN    "%"

/* Other constants */
#define PLUS_OP    1
#define MINUS_OP   2
#define MULT_OP    3
#define DIV_OP     4

#define TABLE_1     "T1"
#define TABLE_2     "T2"

/* Templates to execute binary operations */
#define sqlTemplateNumericBinaryOps_EQ "SELECT T1.vIndex, T1.vValue %s T2.vValue \
                                        FROM %s as T1, %s as T2 \
                                        WHERE T1.vIndex = T2.vIndex"

#define sqlTemplateNumericBinaryOps_NE "SELECT %s.vIndex, T1.vValue %s T2.vValue \
                                        FROM %s as T1, %s as T2 \
                                        WHERE T1.vIndex %% %d = T2.vIndex %% %d"


#define sqlTemplateComplexAddSub_EQ    "SELECT T1.vIndex, T1.vReal %s T2.vReal, \
                                        T1.vImag %s T2.vImag \
                                        FROM %s as T1, %s as T2 \
                                        WHERE T1.vIndex = T2.vIndex"

#define sqlTemplateComplexAddSub_NE    "SELECT %s.vIndex, T1.vReal %s T2.vReal, \
                                        T1.vImag %s T2.vImag \
                                        FROM %s as T1, %s as T2 \
                                        WHERE T1.vIndex %% %d = T2.vIndex %% %d"

#define sqlTemplateComplexMultiply_EQ  "SELECT T1.vIndex, \
                                        (T1.vReal*T2.vReal)-(T1.vImag*T2.vImag), \
                                        (T1.vReal*T2.vImag)+(T1.vImag*T2.vReal) \
                                        FROM %s as T1, %s as T2 \
                                        WHERE T1.vIndex = T2.vIndex"

#define sqlTemplateComplexMultiply_NE  "SELECT %s.vIndex, \
                                        (T1.vReal*T2.vReal)-(T1.vImag*T2.vImag), \
                                        (T1.vReal*T2.vImag)+(T1.vImag*T2.vReal) \
                                        FROM %s as T1, %s as T2 \
                                        WHERE T1.vIndex %% %d = T2.vIndex %% %d"

#define sqlTemplateComplexDivide_EQ   "SELECT T1.vIndex, \
((T1.vReal*T2.vReal)+(T1.vImag*T2.vImag))/((T2.vReal*T2.vReal)+(T2.vImag*T2.vImag)), \
((T1.vImag*T2.vReal)-(T1.vReal*T2.vImag))/((T2.vReal*T2.vReal)+(T2.vImag*T2.vImag)) \
                                        FROM %s as T1, %s as T2 \
                                        WHERE T1.vIndex = T2.vIndex"

#define sqlTemplateComplexDivide_NE   "SELECT %s.vIndex, \
((T1.vReal*T2.vReal)+(T1.vImag*T2.vImag))/((T2.vReal*T2.vReal)+(T2.vImag*T2.vImag)), \
((T1.vImag*T2.vReal)-(T1.vReal*T2.vImag))/((T2.vReal*T2.vReal)+(T2.vImag*T2.vImag)) \
                                        FROM %s as T1, %s as T2 \
                                        WHERE T1.vIndex %% %d = T2.vIndex %% %d"

#define sqlTemplateSimpleNumericBinary "SELECT vIndex, vValue %s %f from %s"


/* Functions to execute binary operations on ints and doubles */
int addNumericVectors(MYSQL * sqlConn, rdbVector * result, 
		      rdbVector * input1, rdbVector * input2);

int subtractNumericVectors(MYSQL * sqlConn, rdbVector * result, 
			   rdbVector * input1, rdbVector * input2);

int multiplyNumericVectors(MYSQL * sqlConn, rdbVector * result, 
			   rdbVector * input1, rdbVector * input2);

int divideNumericVectors(MYSQL * sqlConn, rdbVector * result, 
			 rdbVector * input1, rdbVector * input2);

int modNumericVectors(MYSQL * sqlConn, rdbVector * result, 
		      rdbVector * input1, rdbVector * input2);


/* Helper Functions with binary operations on ints and doubles */
int internalHandleNumericBinOp(MYSQL * sqlConn, rdbVector * result, 
			       rdbVector * input1, rdbVector * input2,
			       char * sign, int forceDouble);

void buildNumericBinaryOpsSQL(rdbVector * input1, rdbVector * input2, 
			      char * sign, char ** sqlStr);


/* Functions to execute binary operations on complex */
int addComplexVectors(MYSQL * sqlConn, rdbVector * result, 
		      rdbVector * input1, rdbVector * input2);

int subtractComplexVectors(MYSQL * sqlConn, rdbVector * result, 
			   rdbVector * input1, rdbVector * input2);

int multiplyComplexVectors(MYSQL * sqlConn, rdbVector * result, 
			   rdbVector * input1, rdbVector * input2);

int divideComplexVectors(MYSQL * sqlConn, rdbVector * result, 
			 rdbVector * input1, rdbVector * input2);


/* Helper Functions with binary operations on complex */
int internalHandleComplexBinOp(MYSQL * sqlConn, rdbVector * result, 
			       rdbVector * input1, rdbVector * input2, int op);

void buildComplexAddSubSQL(rdbVector * input1, rdbVector * input2, 
			   char * sign, char ** sqlStr);

void buildComplexMultDivSQL(rdbVector * input1, rdbVector * input2, 
			    char * sqlTemplateEQ, char * sqlTemplateNE, 
			    char ** sqlStr);

/* Other functions */
int subtractDoubleFromNumericVector(MYSQL * sqlConn, rdbVector * result, 
				    rdbVector *input1, double y);


#endif

