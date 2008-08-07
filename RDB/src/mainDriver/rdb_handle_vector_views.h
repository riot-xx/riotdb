#ifndef _RDB_HANDLE_VECTOR_VIEWS_H
#define _RDB_HANDLE_VECTOR_VIEWS_H


/* Templates to create views */
#define sqlTemplateCreateView        "CREATE VIEW %s (vIndex, vValue) AS %s"

#define sqlTemplateCreateComplexView "CREATE VIEW %s (vIndex,vReal,vImag) AS %s"


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


/* Templates to handle views */
#define sqlTemplateDropView             "DROP VIEW %s"

#define sqlTemplateViewName             "VectorView%d"


/* Templates to materialize views */
#define sqlTemplateMaterializeView      "INSERT INTO %s SELECT * \
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
int createViewReferences(MYSQL * sqlConn, rdbVector * viewVector,
			 rdbVector * leftInput, rdbVector * rightInput);

int getViewReferences(MYSQL * sqlConn, rdbVector * viewVector,
		      rdbVector ** leftInput, rdbVector ** rightInput);

int getViewRefCount(MYSQL * sqlConn, rdbVector * vectorInfo, int * count);

int updateViewReferences(MYSQL * sqlConn, rdbVector * viewVector, 
			 rdbVector * newVector);

int removeViewReferences(MYSQL * sqlConn, rdbVector * viewVector);


/* Functions to delete vector tables */
int dropVectorView(MYSQL * sqlConn, rdbVector * vectorInfo);


/* Functions for naming tables */
int buildUniqueViewName(MYSQL * sqlConn, char ** newViewName);


/* Functions to materialize views */
int materializeIntegerView (MYSQL * sqlConn, rdbVector * viewVector);

int materializeDoubleView  (MYSQL * sqlConn, rdbVector * viewVector);

int materializeStringView  (MYSQL * sqlConn, rdbVector * viewVector);

int materializeComplexView (MYSQL * sqlConn, rdbVector * viewVector);

int materializeLogicView (MYSQL * sqlConn, rdbVector * viewVector);

int internalMaterializeView(MYSQL * sqlConn, rdbVector * viewVector, 
			    rdbVector * newVector);


#endif

