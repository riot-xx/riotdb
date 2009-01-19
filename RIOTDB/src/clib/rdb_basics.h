#ifndef _RDB_BASICS_H
#define _RDB_BASICS_H

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

/* Function Prototypes */
int connectToLocalDB(MYSQL ** sqlConn);


/* Functions to handle RDBVector objects */
void initRDBVector(rdbVector ** vectorInfo, int isView, int alloc);

void copyRDBVector(rdbVector ** copyVector, rdbVector * originalVector,
		   int alloc);

void clearRDBVector(rdbVector ** vectorInfo);

int loadRDBVector(MYSQL * sqlConn, rdbVector ** vectorInfo,
		  unsigned long int metadataID, int alloc);

int loadAllRDBVectors(MYSQL * sqlConn, rdbVector *** arrayVectorInfoObjects,
		      unsigned long int * numObjects);


/* Functions to handle RDBMatrix objects */
void initRDBMatrix(rdbMatrix ** matrixInfo, int isView, int alloc);

void copyRDBMatrix(rdbMatrix ** copyMatrix, rdbMatrix * originalMatrix,
		   int alloc);

void clearRDBMatrix(rdbMatrix ** matrixInfo);

int loadRDBMatrix(MYSQL * sqlConn, rdbMatrix ** matrixInfo,
		  unsigned long int metadataID, int alloc);

int loadAllRDBMatrices(MYSQL * sqlConn, rdbMatrix *** arrayMatrixInfoObjects,
		       unsigned long int * numObjects);

void getRDBMatrixDimensions(char * strSize, int * numRows, int * numCols);

#endif

