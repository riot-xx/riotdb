#ifndef _RDB_HANDLE_VECTOR_TABLES_H
#define _RDB_HANDLE_VECTOR_TABLES_H


/* Templates to create vector tables */
#define sqlTemplateCreateIntVector  "CREATE TABLE %s ( \
                                    vIndex INT(10) UNSIGNED AUTO_INCREMENT, \
                                    vValue INT(12), \
                                    PRIMARY KEY (vIndex), \
                                    INDEX (vIndex) )"

#define sqlTemplateCreateDoubleVector  "CREATE TABLE %s ( \
                                    vIndex INT(10) UNSIGNED AUTO_INCREMENT, \
                                    vValue DOUBLE(22,12), \
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

/* Templates to handle tables */
#define sqlTemplateDropTable        "DROP TABLE %s"

#define sqlTemplateTableName        "VectorTable%d"



/* Functions to create vector tables */
int createNewIntVectorTable(MYSQL * sqlConn, rdbVector * vectorInfo);

int createNewDoubleVectorTable(MYSQL * sqlConn, rdbVector * vectorInfo);

int createNewComplexVectorTable(MYSQL * sqlConn, rdbVector * vectorInfo);

int createNewStringVectorTable(MYSQL * sqlConn, rdbVector * vectorInfo);

int createNewLogicVectorTable(MYSQL * sqlConn, rdbVector * vectorInfo);

int internalCreateNewVectorTable(MYSQL * sqlConn, rdbVector * vectorInfo, 
				 char * sqlTemplate);


/* Functions to drop vector tables */
int dropVectorTable(MYSQL * sqlConn, rdbVector * vectorInfo);


/* Functions for naming tables */
int buildUniqueTableName(MYSQL * sqlConn, char ** newTableName);


#endif

