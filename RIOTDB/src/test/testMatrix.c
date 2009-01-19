#include <stdio.h>
#include <stdlib.h>
#include <mysql.h>

#include "Rinternals.h"

#include "rdb_basics.h"
#include "rdb_handle_metadata.h"
#include "rdb_handle_matrix_tables.h"
#include "rdb_insert_matrix_data.h"
#include "rdb_get_matrix_data.h"
#include "rdb_matrix_functions.h"

/*
#include "rdb_handle_matrix_views.h"
#include "rdb_convert_matrixs.h"
#include "rdb_set_matrix_data.h"
#include "rdb_binary_operators.h"
#include "rdb_compare_matrixs.h"
#include "rdb_aggregate_functions.h"
#include "rdb_element_functions.h"
#include "rdb_delete_matrix_data.h"
*/

/* ------------------ Testing functions prototypes ------------------- */
int TestIntegerInsertions(MYSQL* sqlConn, int testInsert, 
			  int testInsertSeq, int testLoad);

int TestDoubleInsertions(MYSQL* sqlConn, int testInsert, 
			 int testInsertSeq, int testLoad);

int TestMatrixMultiplication(MYSQL* sqlConn);


int TestLoadAllMatrices(MYSQL* sqlConn, int testDelete);

int printIntegerRDBMatrix(MYSQL* sqlConn, rdbMatrix * matrixInfo);

int printDoubleRDBMatrix(MYSQL* sqlConn, rdbMatrix * matrixInfo);

/* --------------------------- main ---------------------------------- */
int main()
{
  printf("Starting Execution!\n");

  /* Connect to database */
  MYSQL *sqlConn = NULL;
  int success = connectToLocalDB(&sqlConn);
  if( !success || sqlConn == NULL )
  {
     printf("ERROR Connect DB: %s\n", mysql_error(sqlConn));
     return -1;
  }

  printf("Connected to DB\n");

  /* Choose which tests to perform */
  int testIntInsertions     = 1;
  int testDoubleInsertions  = 1;

  int testInsert    = 1;
  int testInsertSeq = 1;
  int testLoad      = 1;

  /* ------------------------------ */
  int testMultiplication  = 1;

  /* ------------------------------ */
  int testLoadMatrices = 1;
  int testDelete = 0;

  /* ------------------------------ */

  /* Perform the tests */
  if( testIntInsertions && 
      !TestIntegerInsertions(sqlConn, testInsert, testInsertSeq, testLoad)){
    return -1;
  }

  if( testDoubleInsertions && 
      !TestDoubleInsertions(sqlConn, testInsert, testInsertSeq, testLoad)){
    return -1;
  }

  /* --------------------------------------------------------------------- */

  if( testMultiplication && !TestMatrixMultiplication(sqlConn) ) {
    return -1;
  }

  /* --------------------------------------------------------------------- */

  if( testLoadMatrices && !TestLoadAllMatrices(sqlConn, testDelete) ) {
    return -1;
  }


  /* Close Connection */
  mysql_close(sqlConn);

  printf("Completed Execution!\n");

  return 0;
}


/* --------------------- Test Integer Insertions -------------------------- */
int TestIntegerInsertions(MYSQL* sqlConn, int testInsert, 
			  int testInsertSeq, int testLoad)
{
  if( testInsert )
  {
    rdbMatrix * matrixInfo;
    initRDBMatrix(&matrixInfo, 0, 1);
    if( !createNewIntMatrixTable(sqlConn, matrixInfo) )
    {
       printf("ERROR Create Int Table: %s\n", mysql_error(sqlConn)); 
       return 0;
    }
  
    printf("Insert elements into integer table %s\n", matrixInfo->tableName);
    int size = 7;
    int array[7] = {101, 102, 103, 104, 105, 106, 107};
    int nRows = 3, nCols = 4;
    if( !insertIntoIntMatrixTable(sqlConn, matrixInfo, array, 
				  size, nRows, nCols) )
    {
      printf("ERROR Insert Elem Int Table: %s\n", mysql_error(sqlConn));
      return 0;
    }

    int value = 0;
    char byte = 0;
    if( !getIntMatrixElement(sqlConn, matrixInfo, 2, 3, &value, &byte) )
    {
      printf("ERROR Getting Elem Int Table: %s\n", mysql_error(sqlConn));
      return 0;
    }
    if( byte == 1)
       printf("Got element (row, col)=(2, 3): %d\n", value);
    else
       printf("Got element (row, col)=(2, 3): NA\n");


    if( !printIntegerRDBMatrix(sqlConn, matrixInfo) )
      return 0;
  }
  
  if( testInsertSeq )
  {
    rdbMatrix * matrixInfo;
    initRDBMatrix(&matrixInfo, 0, 1);
    if( !createNewIntMatrixTable(sqlConn, matrixInfo) )
    {
       printf("ERROR Create Int Table: %s\n", mysql_error(sqlConn)); 
       return 0;
    }

    printf("Insert sequence into integer table %s\n", matrixInfo->tableName);
    int nRows = 4, nCols = 5;
    if( !insertSeqIntMatrixTable(sqlConn, matrixInfo, 1, 18, 2, nRows, nCols))
    {
      printf("ERROR Insert Seq Int Table: %s\n", mysql_error(sqlConn));
      return 0;
    }

    if( !printIntegerRDBMatrix(sqlConn, matrixInfo) )
      return 0;
  }

  if( testLoad )
  {
    rdbMatrix * matrixInfo;
    initRDBMatrix(&matrixInfo, 0, 1);
    if( !createNewIntMatrixTable(sqlConn, matrixInfo) )
    {
       printf("ERROR Create Int Table: %s\n", mysql_error(sqlConn)); 
       return 0;
    }
  
    printf("Integer Table:\t%ld\t%s\n", matrixInfo->metadataID, 
	   matrixInfo->tableName);

    if( !loadIntoMatrixTable(sqlConn, matrixInfo, "testData/matrixInt.txt") )
    {
      printf("ERROR Load Ints: %s\n", mysql_error(sqlConn));
      return 0;
    }
    printf("Loaded into Integers table\n");

    if( !printIntegerRDBMatrix(sqlConn, matrixInfo) )
      return 0;
  }

  return 1;
}

/* --------------------- Test Double Insertions -------------------------- */
int TestDoubleInsertions(MYSQL* sqlConn, int testInsert, 
			 int testInsertSeq, int testLoad)
{
  if( testInsert )
  {
    rdbMatrix * matrixInfo;
    initRDBMatrix(&matrixInfo, 0, 1);
    if( !createNewDoubleMatrixTable(sqlConn, matrixInfo) )
    {
       printf("ERROR Create Double Table: %s\n", mysql_error(sqlConn)); 
       return 0;
    }
  
    printf("Insert elements into double table %s\n", matrixInfo->tableName);
    int size = 7;
    double array[7] = {10.1, 10.2, 10.3, 10.4, 10.5, 10.6, 10.7};
    int nRows = 3, nCols = 4;
    if( !insertIntoDoubleMatrixTable(sqlConn, matrixInfo, array, 
				     size, nRows, nCols) )
    {
      printf("ERROR Insert Elem Double Table: %s\n", mysql_error(sqlConn));
      return 0;
    }

    double value = 0;
    char byte = 0;
    if( !getDoubleMatrixElement(sqlConn, matrixInfo, 2, 3, &value, &byte) )
    {
      printf("ERROR Getting Elem Double Table: %s\n", mysql_error(sqlConn));
      return 0;
    }
    if( byte == 1)
       printf("Got element (row, col)=(2, 3): %lf\n", value);
    else
       printf("Got element (row, col)=(2, 3): NA\n");

    if( !printDoubleRDBMatrix(sqlConn, matrixInfo) )
      return 0;
  }
  
  if( testInsertSeq )
  {
    rdbMatrix * matrixInfo;
    initRDBMatrix(&matrixInfo, 0, 1);
    if( !createNewDoubleMatrixTable(sqlConn, matrixInfo) )
    {
       printf("ERROR Create Double Table: %s\n", mysql_error(sqlConn)); 
       return 0;
    }

    printf("Insert sequence into double table %s\n", matrixInfo->tableName);
    int nRows = 4, nCols = 5;
    if( !insertSeqDoubleMatrixTable(sqlConn, matrixInfo, 1, 1.8, 0.2, 
                                    nRows, nCols))
    {
      printf("ERROR Insert Seq Double Table: %s\n", mysql_error(sqlConn));
      return 0;
    }

    if( !printDoubleRDBMatrix(sqlConn, matrixInfo) )
      return 0;
  }

  if( testLoad )
  {
    rdbMatrix * matrixInfo;
    initRDBMatrix(&matrixInfo, 0, 1);
    if( !createNewDoubleMatrixTable(sqlConn, matrixInfo) )
    {
       printf("ERROR Create Double Table: %s\n", mysql_error(sqlConn)); 
       return 0;
    }
  
    printf("Load data into Double Table: %s\n", matrixInfo->tableName);

    if( !loadIntoMatrixTable(sqlConn, matrixInfo, "testData/matrixDouble.txt") )
    {
      printf("ERROR Load Doubles: %s\n", mysql_error(sqlConn));
      return 0;
    }

    if( !printDoubleRDBMatrix(sqlConn, matrixInfo) )
      return 0;
  }

  return 1;
}


/* --------------------- TestLoadAllMatrices ------------------------- */

int TestMatrixMultiplication(MYSQL * sqlConn)
{
    /* Create an integer matrix */
    rdbMatrix * input1;
    initRDBMatrix(&input1, 0, 1);
    if( !createNewIntMatrixTable(sqlConn, input1) )
    {
       printf("ERROR Create Int Table: %s\n", mysql_error(sqlConn)); 
       return 0;
    }

    printf("Insert sequence into integer table %s\n", input1->tableName);
     if( !insertSeqIntMatrixTable(sqlConn, input1, 1, 18, 2, 3, 4))
    {
      printf("ERROR Insert Seq Int Table: %s\n", mysql_error(sqlConn));
      return 0;
    }

    if( !printIntegerRDBMatrix(sqlConn, input1) )
      return 0;

    /* Create another integer matrix */
    rdbMatrix * input2;
    initRDBMatrix(&input2, 0, 1);
    if( !createNewIntMatrixTable(sqlConn, input2) )
    {
       printf("ERROR Create Int Table: %s\n", mysql_error(sqlConn)); 
       return 0;
    }

    printf("Insert sequence into integer table %s\n", input2->tableName);
    if( !insertSeqIntMatrixTable(sqlConn, input2, 1, 18, 2, 4, 3))
    {
      printf("ERROR Insert Seq Int Table: %s\n", mysql_error(sqlConn));
      return 0;
    }

    if( !printIntegerRDBMatrix(sqlConn, input2) )
      return 0;

    /* Create a double matrix */
    rdbMatrix * input3;
    initRDBMatrix(&input3, 0, 1);
    if( !createNewDoubleMatrixTable(sqlConn, input3) )
    {
       printf("ERROR Create Double Table: %s\n", mysql_error(sqlConn)); 
       return 0;
    }

    printf("Insert sequence into double table %s\n", input3->tableName);
    if( !insertSeqDoubleMatrixTable(sqlConn, input3, 1, 1.8, 0.2, 4, 5))
    {
      printf("ERROR Insert Seq Double Table: %s\n", mysql_error(sqlConn));
      return 0;
    }

    if( !printDoubleRDBMatrix(sqlConn, input3) )
      return 0;

    /* Create the result views */
    rdbMatrix * result1, * result2;
    initRDBMatrix(&result1, 0, 1);
    initRDBMatrix(&result2, 0, 1);

    /* Perform multiplications */
    printf("Multiply %s with %s\n", input1->tableName, input2->tableName);
    if( !performMatrixMultiplication(sqlConn, result1, input1, input2) )
    {
      printf("ERROR Performing multiplication: %s\n", mysql_error(sqlConn));
      return 0;
    }

    if( !printIntegerRDBMatrix(sqlConn, result1) )
      return 0;

    printf("Multiply %s with %s\n", input1->tableName, input3->tableName);
    if( !performMatrixMultiplication(sqlConn, result2, input1, input3) )
    {
      printf("ERROR Performing multiplication: %s\n", mysql_error(sqlConn));
      return 0;
    }

    if( !printDoubleRDBMatrix(sqlConn, result2) )
      return 0;

    return 1;
}


/* --------------------- TestLoadAllMatrices ------------------------- */

int TestLoadAllMatrices(MYSQL * sqlConn, int testDelete)
{
  rdbMatrix ** arrayMatrixInfoObjects;
  unsigned long int numTables;

  if( !loadAllRDBMatrices(sqlConn, &arrayMatrixInfoObjects, &numTables) )
  {
     printf("ERROR Load RDB Matrixs: %s\n", mysql_error(sqlConn));
     return 0;
  }

  printf("\nNumber of tables: %ld\n", numTables);
  int i;
  for( i = 0 ; i < numTables ; i++ )
  {
     printf("\t%ld\t%s", 
	    arrayMatrixInfoObjects[i]->metadataID,
	    arrayMatrixInfoObjects[i]->tableName);

     if( testDelete )
     {
        deleteRDBMatrix(sqlConn, arrayMatrixInfoObjects[i]);
	printf("\t--Deleted! %s", mysql_error(sqlConn));
     }
     printf("\n");
  }

  for( i = 0 ; i < numTables ; i++ )
  {
     clearRDBMatrix(&arrayMatrixInfoObjects[i]);
  }

  if( numTables != 0 )
  {
     free(arrayMatrixInfoObjects);
  }
  printf("----------------------------------------\n");

  return 1;
}


/* ---------------------- Print Matrices ------------------------- */
int printIntegerRDBMatrix(MYSQL* sqlConn, rdbMatrix * matrixInfo)
{
    int numRows = matrixInfo->numRows;
    int numCols = matrixInfo->numCols;
    int i, row, col, index, size = numRows * numCols;
    int values[size];
    char byteArray[size];
    for( i = 0 ; i < size ; i++ )
      byteArray[i] = 0;

    if( !getAllIntMatrixElements(sqlConn, matrixInfo, values, byteArray) )
    {
      printf("ERROR Getting Int Matrix values: %s\n", mysql_error(sqlConn));
      return 0;
    }

    printf("Printing Integer Matrix: %s (ID: %ld) with dimensions %dx%d\n", 
	   matrixInfo->tableName, matrixInfo->metadataID, numRows, numCols);
    printf("       Col      Row      Value\n");

    for( col = 0 ; col < numCols ; col++ )
    {
    	for( row = 0 ; row < numRows ; row++ )
    	{
	    index = row + col * numRows;
      	    if( byteArray[index] == 1 )
	       printf("\t%d\t%d\t%d\n", col+1, row+1, values[index]);
            else
	       printf("\t%d\t%d\tNA\n", col+1, row+1);
	}
    }

    return 1;
}

int printDoubleRDBMatrix(MYSQL* sqlConn, rdbMatrix * matrixInfo)
{
    int numRows = matrixInfo->numRows;
    int numCols = matrixInfo->numCols;
    int i, row, col, index, size = numRows * numCols;
    double values[size];
    char byteArray[size];
    for( i = 0 ; i < size ; i++ )
      byteArray[i] = 0;

    if( !getAllDoubleMatrixElements(sqlConn, matrixInfo, values, byteArray) )
    {
      printf("ERROR Getting Double Matrix values: %s\n", mysql_error(sqlConn));
      return 0;
    }

    printf("Printing Double Matrix: %s (ID: %ld) with dimensions %dx%d\n", 
	   matrixInfo->tableName, matrixInfo->metadataID, numRows, numCols);
    printf("       Col      Row      Value\n");

    for( col = 0 ; col < numCols ; col++ )
    {
    	for( row = 0 ; row < numRows ; row++ )
    	{
	    index = row + col * numRows;
      	    if( byteArray[index] == 1 )
	       printf("\t%d\t%d\t%lf\n", col+1, row+1, values[index]);
            else
	       printf("\t%d\t%d\tNA\n", col+1, row+1);
	}
    }

    return 1;
}


