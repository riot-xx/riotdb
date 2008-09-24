#ifndef _RDB_HANDLE_METADATA_H
#define _RDB_HANDLE_METADATA_H

/*****************************************************************************
 * Contains functions for managing the matadata table.
 *
 * Author: Herodotos Herodotou
 * Date:   Sep 17, 2008
 ****************************************************************************/

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
                                    VALUES(NULL, '%s', %d, '%s', %d, %d, \
                                    %d, %d, %d, %d, %d, %d, %d, %d, %d, %d )"

#define sqlTemplateDeleteMetadata  "DELETE FROM Metadata \
                                    WHERE metadata_id = %ld"

#define sqlTemplateUpdateMetadata  "UPDATE Metadata SET table_name = '%s', \
                                    db_source_id = %d, size = '%s', is_view = %d,\
                                    ref_counter = %d, sxp_type = %d,\
                                    sxp_obj = %d, sxp_named = %d, sxp_gp = %d,\
                                    sxp_mark = %d, sxp_debug = %d,\
                                    sxp_trace = %d, sxp_spare = %d,\
                                    sxp_gcgen = %d, sxp_gccls = %d \
                                    WHERE metadata_id = %ld"


/* Functions to keep track of vector table naming scheme */
int getNextTableID(MYSQL* sqlConn, int* next);

int renameTable(MYSQL* sqlConn, char* oldName, char* newName);


/* Functions to handle the reference counter */
int decrementRefCounter(MYSQL* sqlConn, int* newRefCounter, long int metadataID);

int incrementRefCounter(MYSQL* sqlConn, int* newRefCounter, long int metadataID);

int getRefCounter(MYSQL* sqlConn, long int metadataID, int* refCounter);

int setRefCounter(MYSQL* sqlConn, long int metadataID, int refCounter);


/* Functions to handle Metadata*/
int insertVectorMetadataInfo(MYSQL* sqlConn, rdbVector* vectorInfo);

int updateVectorMetadataInfo(MYSQL* sqlConn, rdbVector* vectorInfo);

int insertMatrixMetadataInfo(MYSQL* sqlConn, rdbMatrix* matrixInfo);

int updateMatrixMetadataInfo(MYSQL* sqlConn, rdbMatrix* matrixInfo);

int deleteMetadataInfo(MYSQL* sqlConn, long int metadataID);


#endif

