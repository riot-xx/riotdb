#ifndef _BASICS_H
#define _BASICS_H

/*****************************************************************************
 * Contains necessary constants, the RDBVector definition, the RDBMatrix
 * definition and functions for initializing, copying, clearing and loading
 * RDBVectors and RDBMatrices.
 *
 * Author: Herodotos Herodotou
 * Date:   Sep 17, 2008
 ****************************************************************************/
#include "mysql.h"

/* Useful constants */
#define MAX_INT_LENGTH       12
#define MAX_DOUBLE_LENGTH    22
#define MAX_LOGIC_LENGTH     1
#define TRUE   1
#define FALSE  0

/* Constants refering to SXP Types */
#define SXP_TYPE_LOGIC     10
#define SXP_TYPE_INTEGER   13
#define SXP_TYPE_DOUBLE    14
#define SXP_TYPE_COMPLEX   15
#define SXP_TYPE_STRING    16

/* Templates to create RDBVector objects from Metadata */
#define sqlGetAllRDBVectors    "SELECT * FROM Metadata \
                                WHERE table_name LIKE \"Vector%\""

#define sqlGetRDBVector        "SELECT * FROM Metadata WHERE metadata_id = %ld"

#define sqlGetRDBObject        "SELECT * FROM Metadata WHERE metadata_id = %ld"


/* Templates to create RDBMatrix objects from Metadata */
#define sqlGetAllRDBMatrix     "SELECT * FROM Metadata \
                                WHERE table_name LIKE \"Matrix%\""

#define sqlGetRDBMatrix        "SELECT * FROM Metadata WHERE metadata_id = %ld"


/* Template for naming conventions */
#define sqlTemplateVectorTableName        "VectorTable%d"
#define sqlTemplateVectorViewName         "VectorView%d"
#define sqlTemplateMatrixTableName        "MatrixTable%d"
#define sqlTemplateMatrixViewName         "MatrixView%d"

/* RDBVector Internal Structure */
typedef struct structRDBVector
{
  unsigned long int metadataID;
  char * tableName;
  int dbSourceID;
  int size;
  int isView;
  int refCounter;

  unsigned short int  sxp_type;
  unsigned short int  sxp_obj;
  unsigned short int  sxp_named;
  unsigned short int  sxp_gp;
  unsigned short int  sxp_mark;
  unsigned short int  sxp_debug;
  unsigned short int  sxp_trace;
  unsigned short int  sxp_spare;
  unsigned short int  sxp_gcgen;
  unsigned short int  sxp_gccls;

} rdbVector;

/* RDBMatrix Internal Structure */
typedef struct structRDBMatrix
{
  unsigned long int metadataID;
  char * tableName;
  int dbSourceID;
  int numRows;
  int numCols;
  int isView;
  int refCounter;

  unsigned short int  sxp_type;
  unsigned short int  sxp_obj;
  unsigned short int  sxp_named;
  unsigned short int  sxp_gp;
  unsigned short int  sxp_mark;
  unsigned short int  sxp_debug;
  unsigned short int  sxp_trace;
  unsigned short int  sxp_spare;
  unsigned short int  sxp_gcgen;
  unsigned short int  sxp_gccls;

} rdbMatrix;

typedef struct structRDBObject {
  unsigned short int isVector;
  union unionRDBVectorOrMatrix {
    rdbVector vector;
    rdbMatrix matrix;
  } payload;
} rdbObject;

/* Function Prototypes */
int connectToLocalDB(MYSQL ** sqlConn);

/* Functions to handle generic RDBObjects */
rdbObject *newRDBObject();
void initRDBObject(rdbObject *info);
void copyRDBObjectFromVector(rdbObject *info, rdbVector *vector);
void copyRDBObjectFromMatrix(rdbObject *info, rdbMatrix *matrix);
unsigned long int getMetadataIDInRDBObject(rdbObject *info);
void clearRDBObject(rdbObject **info);
void initRDBObjectFromSQLRow(rdbObject *info, MYSQL_ROW sqlRow);
int loadRDBObject(MYSQL * sqlConn, rdbObject *info, unsigned long int metadataID);

/* Functions to handle RDBVector objects */
rdbVector *newRDBVector();
void initRDBVector(rdbVector *vectorInfo);
void copyRDBVector(rdbVector *copyVector, rdbVector *originalVector);
void clearRDBVector(rdbVector ** vectorInfo);
void initRDBVectorFromSQLRow(rdbVector *info, MYSQL_ROW sqlRow);
int loadRDBVector(MYSQL * sqlConn, rdbVector *vectorInfo, unsigned long int metadataID);
int loadAllRDBVectors(MYSQL * sqlConn, rdbVector *** arrayVectorInfoObjects, unsigned long int * numObjects);

/* Functions to handle RDBMatrix objects */
rdbMatrix *newRDBMatrix();
void initRDBMatrix(rdbMatrix *matrixInfo);
void copyRDBMatrix(rdbMatrix *copyMatrix, rdbMatrix *originalMatrix);
void clearRDBMatrix(rdbMatrix ** matrixInfo);
void initRDBMatrixFromSQLRow(rdbMatrix *info, MYSQL_ROW sqlRow);
int loadRDBMatrix(MYSQL * sqlConn, rdbMatrix * matrixInfo, unsigned long int metadataID);
int loadAllRDBMatrices(MYSQL * sqlConn, rdbMatrix *** arrayMatrixInfoObjects, unsigned long int * numObjects);
void getRDBMatrixDimensions(char * strSize, int * numRows, int * numCols);

#endif

