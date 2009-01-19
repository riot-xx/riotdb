#ifndef _RDB_HANDLE_VECTOR_TABLES_H
#define _RDB_HANDLE_VECTOR_TABLES_H

/*****************************************************************************
 * Contains functions for managing vector tables (create, delete, dublicate,
 * check for N/A).
 *
 * Author: Herodotos Herodotou
 * Date:   Sep 17, 2008
 ****************************************************************************/
#include "rdb_basics.h"

/* Templates to create vector tables */
#define sqlTemplateCreateIntVector  "CREATE TABLE %s ( \
                                    vIndex INT UNSIGNED AUTO_INCREMENT, \
                                    vValue INT NOT NULL, \
                                    PRIMARY KEY (vIndex), \
                                    INDEX (vIndex) )"

#define sqlTemplateCreateDoubleVector  "CREATE TABLE %s ( \
                                    vIndex INT UNSIGNED AUTO_INCREMENT, \
                                    vValue DOUBLE NOT NULL, \
                                    PRIMARY KEY (vIndex), \
                                    INDEX (vIndex) )"

#define sqlTemplateCreateStringVector  "CREATE TABLE %s ( \
                                    vIndex INT(10) UNSIGNED AUTO_INCREMENT, \
                                    vValue VARCHAR(50), \
                                    PRIMARY KEY (vIndex), \
                                    INDEX (vIndex) )"

#define sqlTemplateCreateComplexVector  "CREATE TABLE %s ( \
                                    vIndex INT(10) UNSIGNED AUTO_INCREMENT, \
                                    vReal DOUBLE(22,12), \
                                    vImag DOUBLE(22,12), \
                                    PRIMARY KEY (vIndex), \
                                    INDEX (vIndex) )"

#define sqlTemplateCreateLogicVector  "CREATE TABLE %s ( \
                                    vIndex INT(10) UNSIGNED AUTO_INCREMENT, \
                                    vValue BOOLEAN, \
                                    PRIMARY KEY (vIndex), \
                                    INDEX (vIndex) )"

/* Templates to drop tables */
#define sqlTemplateDropVectorTable        "DROP TABLE %s"


/* Template to create a dublicate table */
#define sqlTemplateDublicateTable    "INSERT INTO %s SELECT * \
                                      FROM %s ORDER BY vIndex"

/* Template to check for NA */
#define sqlTemplateCheckForNA        "UPDATE %s AS DT, %s AS LT \
                                      SET LT.vValue = 0 \
                                      WHERE LT.vIndex = DT.vIndex;"

/* Template to handle vector sizes */
#define sqlTemplateGetVectorLogicalSize    "SELECT MAX(vIndex) FROM %s"

#define sqlTemplateGetVectorActualSize     "SELECT COUNT(*) FROM %s"

#define sqlTemplateSetVectorCurrentSize    "UPDATE Metadata SET size = %d \
                                            WHERE metadata_id = %ld"


/* Functions to create vector tables */
int createNewIntVectorTable(MYSQL * sqlConn, rdbVector * vectorInfo);

int createNewDoubleVectorTable(MYSQL * sqlConn, rdbVector * vectorInfo);

int createNewComplexVectorTable(MYSQL * sqlConn, rdbVector * vectorInfo);

int createNewStringVectorTable(MYSQL * sqlConn, rdbVector * vectorInfo);

int createNewLogicVectorTable(MYSQL * sqlConn, rdbVector * vectorInfo);

int internalCreateNewVectorTable(MYSQL * sqlConn, rdbVector * vectorInfo, 
				 char * sqlTemplate);


/* Functions for naming tables */
int buildUniqueVectorTableName(MYSQL * sqlConn, char ** newTableName);


/* Functions to Delete an RDBVector i.e. decrement refCounter or drop */
int deleteRDBVector(MYSQL * sqlConn, rdbVector * vectorInfo);

int dropVectorTable(MYSQL * sqlConn, rdbVector * vectorInfo);


/* Functions to dublicate a vector table */
int duplicateVectorTable(MYSQL * sqlConn, rdbVector * orignalVector,
			 rdbVector * copyVector);


/* Functions to check for NA */
int checkRDBVectorForNA(MYSQL * sqlConn, rdbVector * vectorInfo,
		       rdbVector * resultVector);

int hasRDBVectorAnyNA(MYSQL * sqlConn, rdbVector * vectorInfo, int * flag);


/* Function to handle vector sizes */
int getLogicalVectorSize(MYSQL * sqlConn, rdbVector * vectorInfo, int * size);

int getActualVectorSize(MYSQL * sqlConn, rdbVector * vectorInfo, int * size);

int internalGetVectorSize(MYSQL * sqlConn, rdbVector * vectorInfo, int * size,
			 char* sqlTemplate);

int setLogicalVectorSize(MYSQL * sqlConn, rdbVector * vectorInfo, int newSize);


#endif

