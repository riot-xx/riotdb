#ifndef _RDB_HANDLE_MATRIX_VIEWS_H
#define _RDB_HANDLE_MATRIX_VIEWS_H

/*****************************************************************************
 * Contains functions for handling matrix views (i.e create, drop, 
 * materialize views and keep track of view references)
 *
 * Author: Herodotos Herodotou
 * Date:   Sep 17, 2008
 ****************************************************************************/

/* Templates to create views */
#define sqlTemplateCreateMatrixView "CREATE VIEW %s (mCol, mRow, mValue) AS %s"

/* Templates to drop views */
#define sqlTemplateDropMatrixView   "DROP VIEW %s"

/* Templates to materialize views */
#define sqlTemplateMaterializeMatrixView "INSERT INTO %s SELECT * \
FROM %s"

/* Templates to handle view references located in rdb_handle_vector_views.h */


/* ----------------- Functions to create views ---------------------- */
int createNewIntegerMatrixView(MYSQL * sqlConn, rdbMatrix * viewMatrix,
			       char sqlString[]);

int createNewDoubleMatrixView(MYSQL * sqlConn, rdbMatrix * viewMatrix,
			      char sqlString[]);

int internalCreateNewMatrixView(MYSQL * sqlConn, rdbMatrix * viewMatrix,
				char sqlTemplate[], char sqlString[]);


/* --------------- Functions to handle view references --------------- */
int createMatrixViewReferences(MYSQL * sqlConn, rdbMatrix * viewMatrix,
	rdbMatrix * leftInput, rdbMatrix * rightInput);

int getMatrixViewReferences(MYSQL * sqlConn, rdbMatrix * viewMatrix,
	rdbMatrix ** leftInput, rdbMatrix ** rightInput, int alloc);

int getMatrixViewRefCount(MYSQL * sqlConn, rdbMatrix * matrixInfo, int * count);

int updateMatrixViewReferences(MYSQL * sqlConn, rdbMatrix * viewMatrix, 
			       rdbMatrix * newMatrix);

int removeMatrixViewReferences(MYSQL * sqlConn, rdbMatrix * viewMatrix);


/* Functions to delete matrix tables */
int dropMatrixView(MYSQL * sqlConn, rdbMatrix * matrixInfo);


/* Functions for naming tables */
int buildUniqueMatrixViewName(MYSQL * sqlConn, char ** newViewName);


/* Functions to materialize views */
int ensureMatrixMaterialization(MYSQL * sqlConn, rdbMatrix * matrixInfo);

int materializeMatrixView(MYSQL * sqlConn, rdbMatrix * viewMatrix);

int materializeIntegerMatrixView (MYSQL * sqlConn, rdbMatrix * viewMatrix);

int materializeDoubleMatrixView  (MYSQL * sqlConn, rdbMatrix * viewMatrix);

int internalMaterializeMatrixView(MYSQL * sqlConn, rdbMatrix * viewMatrix, 
			          rdbMatrix * newMatrix);


#endif

