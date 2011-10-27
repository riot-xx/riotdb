#ifndef _MATRIX_FUNCTIONS_H
#define _MATRIX_FUNCTIONS_H

/*****************************************************************************
 * Contains functions for matrices (multiplication, solve)
 *
 * Author: Herodotos Herodotou, Jun Yang
 * Date:   Sep 23, 2008
 ****************************************************************************/

/* Templates for Matrix Multiplication */
#define sqlTemplateMatrixMultiplication    "SELECT T2.mCol, T1.mRow, \
						   SUM(T1.mValue * T2.mValue)\
					    FROM %s AS T1, %s AS T2 \
					    WHERE T1.mCol = T2.mRow \
					    GROUP BY T1.mRow, T2.mCol"

#define sqlTemplateMatrixFromVectorByCol \
"SELECT CEIL(vIndex/%d) AS mCol, "\
       "IF(vIndex %% %d, vIndex %% %d, %d) AS mRow, "\
       "vValue AS mValue "\
       "FROM %s "\
       "ORDER BY 1, 2"

#define sqlTemplateMatrixFromVectorByRow \
"SELECT IF(vIndex %% %d, vIndex %% %d, %d) AS mCol, "\
       "CEIL(vIndex/%d) AS mRow, "\
       "vValue AS mValue "\
       "FROM %s "\
       "ORDER BY 1, 2"

int restructureMatrixFromVector(MYSQL *sqlConn, rdbMatrix *result,
                                rdbVector *input, int nrows, int ncols, int byrow);

int performMatrixMultiplication(MYSQL * sqlConn, rdbMatrix * result,
		                rdbMatrix * input1, rdbMatrix * input2);

int solveByGuassianElimination(MYSQL *sqlConn, rdbMatrix *result, rdbMatrix *A, rdbMatrix *b);

int computeDoubleMatrixInverse(MYSQL *sqlConn, rdbMatrix *result,
                               rdbMatrix *input);

int computeMatrixTranspose(MYSQL *sqlConn, rdbMatrix *result,
                           rdbMatrix *input);

#endif

