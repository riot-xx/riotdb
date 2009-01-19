#ifndef _MATRIX_FUNCTIONS_H
#define _MATRIX_FUNCTIONS_H

/*****************************************************************************
 * Contains functions for matrices (multiplication)
 *
 * Author: Herodotos Herodotou
 * Date:   Sep 23, 2008
 ****************************************************************************/

/* Templates for Matrix Multiplication */
#define sqlTemplateMatrixMultiplication    "SELECT T2.mCol, T1.mRow, \
						   SUM(T1.mValue * T2.mValue)\
					    FROM %s AS T1, %s AS T2 \
					    WHERE T1.mCol = T2.mRow \
					    GROUP BY T1.mRow, T2.mCol"


int performMatrixMultiplication(MYSQL * sqlConn, rdbMatrix * result, 
		                rdbMatrix * input1, rdbMatrix * input2);

#endif

