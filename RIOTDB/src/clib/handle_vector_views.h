#ifndef _HANDLE_VECTOR_VIEWS_H
#define _HANDLE_VECTOR_VIEWS_H

/*****************************************************************************
 * Contains functions for handling vector views (i.e create, drop,
 * materialize views and keep track of view references)
 *
 * Author: Herodotos Herodotou
 * Date:   Sep 17, 2008
 ****************************************************************************/

/* Templates to create views */
#define sqlTemplateCreateVectorView  "CREATE VIEW %s (vIndex, vValue) AS %s"

#define sqlTemplateCreateComplexVectorView "CREATE VIEW %s (vIndex,vReal,vImag) AS %s"


/* Templates to handle view references */
#define sqlTemplateCreateViewReferences "INSERT INTO View_Reference \
                                         VALUES(%ld, %ld, %ld)"

#define sqlTemplateGetViewReferences    "SELECT table1_id, table2_id \
                                         FROM View_Reference \
                                         WHERE view_id = %ld"

#define sqlTemplateGetViewRefCount   "SELECT COUNT(*) FROM View_Reference \
                                      WHERE table1_id = %ld OR table2_id = %ld"

#define sqlTemplateUpdateViewReferences1 "UPDATE View_Reference \
                                          SET table1_id = %ld \
                                          WHERE table1_id = %ld"

#define sqlTemplateUpdateViewReferences2 "UPDATE View_Reference \
                                          SET table2_id = %ld \
                                          WHERE table2_id = %ld"

#define sqlTemplateRemoveViewReferences "DELETE FROM View_Reference \
                                         WHERE view_id = %ld"


/* Templates to drop views */
#define sqlTemplateDropVectorView             "DROP VIEW %s"

/* Templates to materialize views */
#define sqlTemplateMaterializeVectorView      "INSERT INTO %s SELECT * \
                                               FROM %s ORDER BY vIndex"


/* Functions to create views */
int createNewIntegerVectorView(MYSQL * sqlConn, rdbVector * viewVector,
			       char sqlString[]);

int createNewDoubleVectorView(MYSQL * sqlConn, rdbVector * viewVector,
			      char sqlString[]);

int createNewStringVectorView(MYSQL * sqlConn, rdbVector * viewVector,
			      char sqlString[]);

int createNewComplexVectorView(MYSQL * sqlConn, rdbVector * viewVector,
			       char sqlString[]);

int createNewLogicVectorView(MYSQL * sqlConn, rdbVector * viewVector,
			     char sqlString[]);

int internalCreateNewVectorView(MYSQL * sqlConn, rdbVector * viewVector,
				char sqlTemplate[], char sqlString[]);


/* Functions to handle view references */
int createVectorViewReferences(MYSQL * sqlConn, rdbVector * viewVector,
                               rdbVector * leftInput, rdbVector * rightInput);

int getVectorViewReferences(MYSQL * sqlConn, rdbVector * viewVector,
                            rdbObject *leftInput, rdbObject *rightInput);

int getVectorViewRefCount(MYSQL * sqlConn, rdbVector * vectorInfo, int * count);

int updateVectorViewReferences(MYSQL * sqlConn, rdbVector * viewVector,
			       rdbVector * newVector);

int removeVectorViewReferences(MYSQL * sqlConn, rdbVector * viewVector);


/* Functions to delete vector tables */
int dropVectorView(MYSQL * sqlConn, rdbVector * vectorInfo);


/* Functions for naming tables */
int buildUniqueVectorViewName(MYSQL * sqlConn, char ** newViewName);


/* Functions to materialize views */
int ensureVectorMaterialization(MYSQL * sqlConn, rdbVector * vectorInfo);

int materializeVectorView(MYSQL * sqlConn, rdbVector * viewVector);

int materializeIntegerVectorView (MYSQL * sqlConn, rdbVector * viewVector);

int materializeDoubleVectorView  (MYSQL * sqlConn, rdbVector * viewVector);

int materializeStringVectorView  (MYSQL * sqlConn, rdbVector * viewVector);

int materializeComplexVectorView (MYSQL * sqlConn, rdbVector * viewVector);

int materializeLogicVectorView (MYSQL * sqlConn, rdbVector * viewVector);

int internalMaterializeVectorView(MYSQL * sqlConn, rdbVector * viewVector,
			          rdbVector * newVector);


#endif

