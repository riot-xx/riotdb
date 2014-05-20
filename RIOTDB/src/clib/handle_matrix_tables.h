#ifndef _HANDLE_MATRIX_TABLES_H
#define _HANDLE_MATRIX_TABLES_H

/*****************************************************************************
 * Contains functions for managing matrix tables (create, delete, dublicate)
 *
 * Author: Herodotos Herodotou
 * Date:   Sep 17, 2008
 ****************************************************************************/
#include "basics.h"

/* Templates to create matrix tables */
#define sqlTemplateCreateIntMatrix  "CREATE TABLE %s ( \
                                     mCol INT UNSIGNED , \
                                     mRow INT UNSIGNED , \
                                     mValue INT NOT NULL, \
                                     PRIMARY KEY (mCol, mRow) )"

#define sqlTemplateCreateDoubleMatrix  "CREATE TABLE %s ( \
                                        mCol INT UNSIGNED , \
                                        mRow INT UNSIGNED , \
                                        mValue DOUBLE NOT NULL, \
                                        PRIMARY KEY (mCol, mRow) )"

/* Templates to drop matrix tables */
#define sqlTemplateDropMatrixTable        "DROP TABLE %s"


/* Template to create a dublicate matrix tables */
#define sqlTemplateDublicateMatrixTable  "INSERT INTO %s SELECT * \
                                          FROM %s ORDER BY mCol, mRow"


/* Template to handle matrix sizes */
#define sqlTemplateGetLogicalMatrixSize  "SELECT MAX(mRow), MAX(mCol) FROM %s"

#define sqlTemplateSetCurrentMatrixSize  "UPDATE Metadata SET size = '%s' \
                                          WHERE metadata_id = %ld"


/* Functions to create matrix tables */
int createNewIntMatrixTable(MYSQL * sqlConn, rdbMatrix * matrixInfo);

int createNewDoubleMatrixTable(MYSQL * sqlConn, rdbMatrix * matrixInfo);

int internalCreateNewMatrixTable(MYSQL * sqlConn, rdbMatrix * matrixInfo,
				 char * sqlTemplate);


/* Functions for naming tables */
int buildUniqueMatrixTableName(MYSQL * sqlConn, char ** newTableName);


/* Functions to Delete an RDBMatrix i.e. decrement refCounter or drop */
int deleteRDBMatrix(MYSQL * sqlConn, rdbMatrix * matrixInfo);

int dropMatrixTable(MYSQL * sqlConn, rdbMatrix * matrixInfo);


/* Functions to dublicate a matrix table */
int duplicateMatrixTable(MYSQL * sqlConn, rdbMatrix * orignalMatrix,
			 rdbMatrix * copyMatrix);


/* Function to handle table sizes */
int getLogicalMatrixSize(MYSQL * sqlConn, rdbMatrix * matrixInfo,
			 int * numRows, int * numCols);

int setLogicalMatrixSize(MYSQL * sqlConn, rdbMatrix * matrixInfo,
			 int newNumRows, int newNumCols);

int updateLogicalMatrixSize(MYSQL * sqlConn, rdbMatrix * matrixInfo);

#endif

