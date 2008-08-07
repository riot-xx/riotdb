#ifndef _RDB_HANDLE_VECTORS_H
#define _RDB_HANDLE_VECTORS_H



/* Template for data type convertions */
#define sqlTemplateNumericToComplex  "SELECT vIndex, vValue, 0 FROM %s"

#define sqlTemplateDoubleToInteger   "SELECT vIndex, floor(vValue) FROM %s"

#define sqlTemplateGenericConvert    "SELECT vIndex, vValue FROM %s"


/* Template to create a dublicate table */
#define sqlTemplateDublicateTable    "INSERT INTO %s SELECT * \
                                      FROM %s ORDER BY vIndex"

/* Template to get the sizes */
#define sqlTemplateGetLogicalSize    "SELECT MAX(vIndex) FROM %s"

#define sqlTemplateGetActualSize     "SELECT COUNT(*) FROM %s"


/* Template to check for NA */
#define sqlTemplateCheckForNA        "UPDATE %s AS DT, %s AS LT \
                                      SET LT.vValue = 0 \
                                      WHERE LT.vIndex = DT.vIndex;"

/* Template to get views referred by a vector */
#define sqlTemplateGetReferredViews  "SELECT view_id FROM View_Reference \
                                      WHERE table1_id = %ld or table2_id = %ld"


/* Templates that refer to the reference counter*/
#define sqlTemplateGetRefCounter   "SELECT ref_counter FROM Metadata \
                                    WHERE metadata_id = %ld"

#define sqlTemplateSetRefCounter   "UPDATE Metadata SET ref_counter = %d \
                                    WHERE metadata_id = %ld"

#define sqlTemplateRenameTable     "RENAME TABLE %s TO %s"


/* Queries that refer to the DB sources */
#define sqlGetNextVectorID         "SELECT next_vec_index FROM DB_Sources \
                                    WHERE db_source_id = 1"

/* Templates that refer to the Metadata table */
#define sqlTemplateInsertMetadata  "INSERT INTO Metadata \
                                    VALUES(NULL, '%s', %d, %d, %d, %d, \
                                    %d, %d, %d, %d, %d, %d, %d, %d, %d, %d )"

#define sqlTemplateDeleteMetadata  "DELETE FROM Metadata \
                                    WHERE metadata_id = %ld"

#define sqlTemplateUpdateMetadata  "UPDATE Metadata SET table_name = '%s', \
                                    db_source_id = %d, size = %d, is_view = %d,\
                                    ref_counter = %d, sxp_type = %d,\
                                    sxp_obj = %d, sxp_named = %d, sxp_gp = %d,\
                                    sxp_mark = %d, sxp_debug = %d,\
                                    sxp_trace = %d, sxp_spare = %d,\
                                    sxp_gcgen = %d, sxp_gccls = %d \
                                    WHERE metadata_id = %ld"


/* Functions to Delete an RDBVector i.e. decrement refCounter */
int deleteRDBVector(MYSQL * sqlConn, rdbVector * vectorInfo);

/* Functions to dublicate a vector table */
int duplicateVectorTable(MYSQL * sqlConn, rdbVector * orignalVector,
			 rdbVector * copyVector);


/* Functions for data type convertions */
int convertNumericToComplex(MYSQL * sqlConn, rdbVector * numericVector, 
			    rdbVector * complexVector);

int convertDoubleToInteger(MYSQL * sqlConn, rdbVector * doubleVector, 
			   rdbVector * integerVector);

int convertIntegerToDouble(MYSQL * sqlConn, rdbVector * integerVector, 
			   rdbVector * doubleVector);

int convertLogicToInteger(MYSQL * sqlConn, rdbVector * logicVector, 
			  rdbVector * integerVector);

int convertLogicToDouble(MYSQL * sqlConn, rdbVector * logicVector, 
			 rdbVector * doubleVector);

int internalConvertVectors (MYSQL * sqlConn, rdbVector * fromVector, 
			    rdbVector * toVector, char * sqlTemplate, int type);


/* Function to get size */
int getLogicalTableSize(MYSQL * sqlConn, rdbVector * vectorInfo, int * size);

int getActualTableSize(MYSQL * sqlConn, rdbVector * vectorInfo, int * size);

int internalGetTableSize(MYSQL * sqlConn, rdbVector * vectorInfo, int * size,
			 char* sqlTemplate);


/* Function to check for NA */
int checkRDBVectorForNA(MYSQL * sqlConn, rdbVector * vectorInfo,
		       rdbVector * resultVector);

int hasRDBVectorAnyNA(MYSQL * sqlConn, rdbVector * vectorInfo, int * flag);


/* ------------------------- Internal Functions ----------------------- */
/* Functions to check on a vector update */
int ensureMaterialization(MYSQL * sqlConn, rdbVector * vectorInfo);

int materializeView(MYSQL * sqlConn, rdbVector * viewVector);


/* Functions to handle the reference counter */
int decrementRefCounter(MYSQL * sqlConn, rdbVector * vectorInfo);

int incrementRefCounter(MYSQL * sqlConn, rdbVector * vectorInfo);

int getRefCounter(MYSQL * sqlConn, rdbVector * vectorInfo, int * refCounter);

int setRefCounter(MYSQL * sqlConn, rdbVector * vectorInfo, int refCounter);


/* Functions to keep track of vector table naming scheme */
int getNextVectorID(MYSQL * sqlConn, int * next);

int renameTable(MYSQL * sqlConn, char* oldName, char* newName);


/* Functions to handle Metadata*/
int insertMetadataInfo(MYSQL * sqlConn, rdbVector * vectorInfo);

int deleteMetadataInfo(MYSQL * sqlConn, rdbVector * vectorInfo);

int updateMetadataInfo(MYSQL * sqlConn, rdbVector * vectorInfo);

#endif

