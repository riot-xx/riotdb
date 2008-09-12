#ifndef _RDB_BASICS_H
#define _RDB_BASICS_H

#include "mysql.h"

/* Useful constrants */
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

/* Templates to create RDBVectors from Metadata */
#define sqlGetAllRDBVectors    "SELECT * FROM Metadata"

#define sqlGetRDBVector        "SELECT * FROM Metadata WHERE metadata_id = %ld"


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


/* Function Prototypes */
int connectToLocalDB(MYSQL ** sqlConn);


/* Functions to initialize/clear RDBVector objects */
void initRDBVector(rdbVector ** vectorInfo, int isView, int alloc);

void copyRDBVector(rdbVector ** copyVector, rdbVector * originalVector,
		   int alloc);

void clearRDBVector(rdbVector ** vectorInfo);


int loadAllRDBVectors(MYSQL * sqlConn, rdbVector *** arrayVectorInfoObjects,
		      unsigned long int * numObjects);

int loadRDBVector(MYSQL * sqlConn, rdbVector ** vectorInfo,
		  unsigned long int metadataID );


#endif

