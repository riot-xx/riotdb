#include <stdio.h>
#include <stdlib.h>
#include <mysql.h>

#include "Rinternals.h"

#include "basics.h"
#include "handle_vector_tables.h"
#include "handle_vector_views.h"
#include "handle_metadata.h"
#include "convert_vectors.h"
#include "insert_vector_data.h"
#include "get_vector_data.h"
#include "set_vector_data.h"
#include "vector_binary_ops.h"
#include "compare_vectors.h"
#include "vector_aggregate_functions.h"
#include "vector_element_functions.h"
#include "delete_vector_data.h"



/* --- Testing functions prototypes --- */
int TestIntegerVectors(MYSQL* sqlConn, int testInsert, int testLoad, 
		       int testGetRange, int testGetSparse, 
		       int testGetWDBVec, int testGetWLogic);
int TestDoubleVectors (MYSQL* sqlConn, int testInsert, int testLoad, 
		       int testGetRange, int testGetSparse,
		       int testGetWDBVec, int testGetWLogic);
int TestStringVectors (MYSQL* sqlConn, int testInsert, int testLoad,
		       int testGetRange, int testGetSparse,
		       int testGetWDBVec, int testGetWLogic);
int TestComplexVectors(MYSQL* sqlConn, int testInsert, int testLoad,
		       int testGetRange, int testGetSparse,
		       int testGetWDBVec, int testGetWLogic);
int TestLogicVectors  (MYSQL* sqlConn, int testInsert, int testLoad, 
		       int testGetRange, int testGetSparse,
		       int testGetWDBVec, int testGetWLogic);

int TestLoadAllVectors(MYSQL* sqlConn, int testDelete);


int TestIntegerBinaryOps(MYSQL* sqlConn, int testAdd, int testMinus,
			 int testMult, int testDiv, int testMod);
int TestDoubleBinaryOps (MYSQL* sqlConn, int testAdd, int testMinus,
			int testMult, int testDiv);
int TestComplexBinaryOps(MYSQL* sqlConn, int testAdd, int testMinus,
			 int testMult, int testDiv);
int TestLogicBinaryOps  (MYSQL* sqlConn, int testAdd, int testMinus,
			 int testMult, int testDiv);

int TestIntegerSequence(MYSQL* sqlConn, int start, int end, int step);

int TestDoubleSequence(MYSQL* sqlConn, double start, double end, double step);


int TestIntegerSetters(MYSQL* sqlConn, int testSetElem, int testSetRange, 
		       int testSetSparse, int testSetWDBV, int testSetWLogic);
int TestDoubleSetters (MYSQL* sqlConn, int testSetElem, int testSetRange, 
		       int testSetSparse, int testSetWDBV, int testSetWLogic);
int TestStringSetters (MYSQL* sqlConn, int testSetElem, int testSetRange, 
		       int testSetSparse);
int TestComplexSetters(MYSQL* sqlConn, int testSetElem, int testSetRange, 
		       int testSetSparse);
int TestLogicSetters  (MYSQL* sqlConn, int testSetElem, int testSetRange, 
		       int testSetSparse);

int TestMaterialization(MYSQL* sqlConn, int testMat1, int testMat2,
			int testMat3, int testMat4);

int TestDuplicateTable(MYSQL* sqlConn, int testFromTable, int testFromView);

int TestComparisons(MYSQL* sqlConn, int testCompInts, int testCompDoubles, 
		    int testCompStrings, int testCompDBVectors);

int TestFunctions(MYSQL* sqlConn, int testFuncInts, int testFuncDoubles, 
		  int testFuncComplex, int testFuncStrings);

int TestConvertions(MYSQL* sqlConn);


int printIntegerRDBVector(MYSQL* sqlConn, rdbVector * vectorInfo);

int printDoubleRDBVector(MYSQL* sqlConn, rdbVector * vectorInfo);

int printStringRDBVector(MYSQL* sqlConn, rdbVector * vectorInfo);

int printComplexRDBVector(MYSQL* sqlConn, rdbVector * vectorInfo);

int printLogicRDBVector(MYSQL* sqlConn, rdbVector * vectorInfo);


/* --- main --- */
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
  int testInts    = 1;
  int testDoubles = 1;
  int testStrings = 1;
  int testComplex = 1;
  int testLogic   = 1;

  int testInsert    = 1;
  int testLoad      = 1;
  int testGetRange  = 1;
  int testGetSparse = 1;
  int testGetWDBVec = 1;
  int testGetWLogic = 1;

  /* ------------------------------ */
  int testSetInts    = 1;
  int testSetDoubles = 1;
  int testSetStrings = 1;
  int testSetComplex = 1;
  int testSetLogic   = 1;

  int testSetElem   = 1;
  int testSetRange  = 1;
  int testSetSparse = 1;
  int testSetWDBV   = 1;
  int testSetWLogic = 1;

  /* ------------------------------ */
  int testIntSeq    = 1;
  int testDoubleSeq = 1;

  /* ------------------------------ */
  int testIntBinOp     = 1;
  int testDoubleBinOp  = 1;
  int testComplexBinOp = 1;
  int testLogicBinOp   = 1;

  int testAdd   = 1;
  int testMinus = 1;
  int testMult  = 1;
  int testDiv   = 1;
  int testMod   = 1;

  /* ------------------------------ */
  int testMaterialization = 1;

  int testMat1 = 1;
  int testMat2 = 1;
  int testMat3 = 1;
  int testMat4 = 1;

  /* ------------------------------ */
  int testDuplicateTable = 1;

  int testFromTable  = 1;
  int testFromView   = 1;

  /* ------------------------------ */
  int testComparisons = 1;

  int testCompInts      = 1;
  int testCompDoubles   = 1;
  int testCompStrings   = 1;
  int testCompDBVectors = 1;

  /* ------------------------------ */
  int testFunctions = 1;

  int testFuncInts     = 1;
  int testFuncDoubles  = 1;
  int testFuncComplex  = 1;
  int testFuncStrings  = 1;

  /* ------------------------------ */
  int testConvert = 1;

  /* ------------------------------ */
  int testLoadVectors = 1;
  int testDelete = 0;

  /* ------------------------------ */

  /* Perform the tests */
  if( testInts && !TestIntegerVectors(sqlConn, testInsert, testLoad, 
      testGetRange, testGetSparse, testGetWDBVec, testGetWLogic)){
    return -1;
  }

  if( testDoubles && !TestDoubleVectors(sqlConn, testInsert, testLoad, 
      testGetRange, testGetSparse, testGetWDBVec, testGetWLogic)){
    return -1;
  }

  if( testStrings && !TestStringVectors(sqlConn, testInsert, testLoad, 
      testGetRange, testGetSparse, testGetWDBVec, testGetWLogic)){
    return -1;
  }

  if( testComplex && !TestComplexVectors(sqlConn, testInsert, testLoad,
      testGetRange, testGetSparse, testGetWDBVec, testGetWLogic)){
    return -1;
  }

  if( testLogic && !TestLogicVectors(sqlConn, testInsert, testLoad, 
      testGetRange, testGetSparse, testGetWDBVec, testGetWLogic)){
    return -1;
  }

  /* --------------------------------------------------------------------- */

  if( testSetInts && !TestIntegerSetters(sqlConn, testSetElem, testSetRange,
				testSetSparse, testSetWDBV, testSetWLogic )){
    return -1;
  }

  if( testSetDoubles && !TestDoubleSetters(sqlConn, testSetElem, testSetRange, 
				testSetSparse, testSetWDBV, testSetWLogic)){
    return -1;
  }

  if( testSetStrings && !TestStringSetters(sqlConn, testSetElem,
				      testSetRange, testSetSparse)){
    return -1;
  }

  if( testSetComplex && !TestComplexSetters(sqlConn, testSetElem,
				      testSetRange, testSetSparse)){
    return -1;
  }

  if( testSetLogic && !TestLogicSetters(sqlConn, testSetElem,
			       	      testSetRange, testSetSparse)){
    return -1;
  }

  /* --------------------------------------------------------------------- */

  if( testIntSeq && !TestIntegerSequence(sqlConn, -10, 13, 2) ) {
    return -1;
  }

  if( testDoubleSeq && !TestDoubleSequence(sqlConn, 0.34, -0.12, -0.05) ) {
    return -1;
  }

  /* --------------------------------------------------------------------- */

  if( testIntBinOp && !TestIntegerBinaryOps(sqlConn, testAdd, testMinus, 
					    testMult, testDiv, testMod) ){
    return -1;
  }

  if( testDoubleBinOp && !TestDoubleBinaryOps(sqlConn, testAdd, testMinus,
					      testMult, testDiv) ){
    return -1;
  }

  if( testComplexBinOp && !TestComplexBinaryOps(sqlConn, testAdd, testMinus,
						testMult, testDiv) ){
    return -1;
  }

  if( testLogicBinOp && !TestLogicBinaryOps(sqlConn, testAdd, testMinus,
						testMult, testDiv) ){
    return -1;
  }

  /* --------------------------------------------------------------------- */

  if( testMaterialization && !TestMaterialization(sqlConn, testMat1, testMat2,
						  testMat3, testMat4) ) {
    return -1;
  }

  /* --------------------------------------------------------------------- */

  if( testDuplicateTable && !TestDuplicateTable(sqlConn, testFromTable,
						testFromView ) ) {
    return -1;
  }

  /* --------------------------------------------------------------------- */
  if( testComparisons && !TestComparisons(sqlConn,testCompInts,testCompDoubles,
					  testCompStrings, testCompDBVectors) ){
    return -1;
  }

  /* --------------------------------------------------------------------- */
  if( testFunctions && !TestFunctions(sqlConn,testFuncInts,testFuncDoubles,
					testFuncComplex, testFuncStrings) ){
    return -1;
  }

  /* --------------------------------------------------------------------- */

  if( testConvert && !TestConvertions(sqlConn) ) {
    return -1;
  }

  /* --------------------------------------------------------------------- */

  if( testLoadVectors && !TestLoadAllVectors(sqlConn, testDelete) ) {
    return -1;
  }


  /* Close Connection */
  mysql_close(sqlConn);

  printf("Completed Execution!\n");

  return 0;

}


/* ------------------ TestIntegerVectors---------------------------- */
int TestIntegerVectors(MYSQL * sqlConn, int testInsert, int testLoad,
		       int testGetRange, int testGetSparse, 
		       int testGetWDBVec, int testGetWLogic)
{ 
  rdbVector * vectorInfo;
  initRDBVector(&vectorInfo, 0, 1);
  if( !createNewIntVectorTable(sqlConn, vectorInfo) )
  {
     printf("ERROR Create Int Table: %s\n", mysql_error(sqlConn)); 
     return 0;
  }
  
  printf("Integer Table:\t%ld\t%s\n", vectorInfo->metadataID, 
	 vectorInfo->tableName);

  if( testInsert )
  {
    int isize = 7;
    int iarray[7] = {2128, 123, 3211, 67, 724, 27, 9};
    if( !insertIntoIntVectorTable(sqlConn, vectorInfo, iarray, isize) )
    {
      printf("ERROR Insert Int Table: %s\n", mysql_error(sqlConn));
      return 0;
    }

    if( !printIntegerRDBVector(sqlConn, vectorInfo) )
      return 0;

    int index = 10;
    int num = 0;
    char byte = 0;
    if( !getIntElement(sqlConn, vectorInfo, index, &num, &byte) )
    {
      printf("ERROR Get Int Value: %s\n", mysql_error(sqlConn));
      return 0;
    }

    if( byte == 1 )
      printf("From integer table got elem %d: %d\n", index, num);
    else
      printf("From integer table got elem %d: NA\n", index);
  }
  
  if( testLoad )
  {
    if( !loadIntoVectorTable(sqlConn, vectorInfo, "testData/dataInt.txt") )
    {
      printf("ERROR Load Ints: %s\n", mysql_error(sqlConn));
      return 0;
    }
    printf("\tLoaded into Integers table\n");

    if( !printIntegerRDBVector(sqlConn, vectorInfo) )
      return 0;
  }
  
  if( testGetRange )
  {
    int gt = 5;
    int lt = 9;
    int i, size = lt - gt + 1;
    int values[size];
    char byteArray[size];
    for( i = 0 ; i < size ; i++ )
      byteArray[i] = 0;

    if( !getRangeIntElements(sqlConn, vectorInfo, gt, lt, values, byteArray) )
    {
       printf("ERROR Getting Range Integers: %s\n", mysql_error(sqlConn));
       return 0;
    }

    printf("Getting Range of Integers (%d,%d):\n", gt, lt);
    for( i = 0 ; i < size ; i++ )
    {
      if( byteArray[i] == 1 )
	printf("\t%d\t%d\n", gt+i, values[i]);
      else
	printf("\t%d\tNA\n", gt+i);
    }
  }

  if( testGetSparse )
  {
    unsigned int indexes[4] = {3, 30, 12, 6};
    int i, size = 4;
    int values[size];
    char byteArray[size];
    for( i = 0 ; i < size ; i++ )
      byteArray[i] = 0;

    if( !getSparseIntElements(sqlConn, vectorInfo, indexes, size, 
			      values, byteArray) )
    {
       printf("ERROR Getting Sparse Integers: %s\n", mysql_error(sqlConn));
       return 0;
    }

    printf("Getting Sparse Integers\n");
    for( i = 0 ; i < size ; i++ )
    {
      if( byteArray[i] == 1 )
	printf("\t%d\t%d\n", indexes[i], values[i]);
      else
	printf("\t%d\tNA\n", indexes[i]);
    }    
  }

  if( testGetWDBVec )
  {
    /* Create an index table vector */
    rdbVector * indexVector;
    initRDBVector(&indexVector, 0, 1);
    if( !createNewIntVectorTable(sqlConn, indexVector) )
    {
      printf("ERROR Create Int Table: %s\n", mysql_error(sqlConn)); 
      return 0;
    }
  
    int indexes[5] = {3, 30, 12, 6, 3};
    int indexSize = 5;
    if( !insertIntoIntVectorTable(sqlConn, indexVector, indexes, indexSize) )
    {
      printf("ERROR Insert Int Table: %s\n", mysql_error(sqlConn));
      return 0;
    }

    /* Get the corresponding values */
    rdbVector * resultVector;
    initRDBVector(&resultVector, 1, 1);
    if( !getIntElementsWithDBVector(sqlConn, vectorInfo, indexVector, 
				    resultVector) )
    {
       printf("ERROR Getting Integers With DBVec: %s\n", mysql_error(sqlConn));
       return 0;
    }

    printf("Getting Integers with indexes from %s \n", indexVector->tableName);
    printIntegerRDBVector(sqlConn, indexVector);
    printIntegerRDBVector(sqlConn, resultVector);

    clearRDBVector(&indexVector);
    clearRDBVector(&resultVector);

    /* Create an index table vector */
    rdbVector * indexVector2;
    initRDBVector(&indexVector2, 0, 1);
    if( !createNewDoubleVectorTable(sqlConn, indexVector2) )
    {
      printf("ERROR Create Double Table: %s\n", mysql_error(sqlConn)); 
      return 0;
    }
  
    double indexes2[5] = {3.2, 33.623, 17.23, 5.75, 2.9};
    int indexSize2 = 5;
    if( !insertIntoDoubleVectorTable(sqlConn, indexVector2,indexes2,indexSize2))
    {
      printf("ERROR Insert Double Table: %s\n", mysql_error(sqlConn));
      return 0;
    }

    /* Get the corresponding values */
    rdbVector * resultVector2;
    initRDBVector(&resultVector2, 1, 1);
    if( !getIntElementsWithDBVector(sqlConn, vectorInfo, indexVector2, 
				    resultVector2) )
    {
       printf("ERROR Getting Integers With DBVec: %s\n", mysql_error(sqlConn));
       return 0;
    }

    printf("Getting Integers with indexes from %s\n", indexVector2->tableName);
    printDoubleRDBVector(sqlConn, indexVector2);
    printIntegerRDBVector(sqlConn, resultVector2);

    clearRDBVector(&indexVector2);
    clearRDBVector(&resultVector2);
  }


  if( testGetWLogic )
  {
    if( !setIntElement(sqlConn, vectorInfo, 15, 150) )
    {
       printf("ERROR Setting Ints: %s\n", mysql_error(sqlConn));
       return 0;
    }
    printf("In integer table set elem 15: 150\n");

    /* Create a logic table vector */
    rdbVector * logicVector;
    initRDBVector(&logicVector, 0, 1);
    if( !createNewLogicVectorTable(sqlConn, logicVector) )
    {
      printf("ERROR Create Logic Table: %s\n", mysql_error(sqlConn)); 
      return 0;
    }
  
    int indexes[5] = {1, 0, 0, 1, 1};
    int indexSize = 5;
    if( !insertIntoLogicVectorTable(sqlConn, logicVector, indexes, indexSize) )
    {
      printf("ERROR Insert Logic Table: %s\n", mysql_error(sqlConn));
      return 0;
    }

    /* Get the corresponding values */
    rdbVector * resultVector;
    initRDBVector(&resultVector, 1, 1);
    if( !getIntElementsWithLogic(sqlConn, vectorInfo, logicVector, 
				    resultVector) )
    {
       printf("ERROR Getting Int With Logic: %s\n", mysql_error(sqlConn));
       return 0;
    }

    printf("Getting Int elems using logic vector %s (1,0,0,1,1)\n",
	   logicVector->tableName);
    printIntegerRDBVector(sqlConn, resultVector);

    clearRDBVector(&logicVector);
    clearRDBVector(&resultVector);
  }

  clearRDBVector(&vectorInfo);
  printf("----------------------------------------\n");

  return 1;
}


/* ------------------ TestDoubleVectors---------------------------- */
int TestDoubleVectors(MYSQL * sqlConn, int testInsert, int testLoad,
		      int testGetRange, int testGetSparse, 
		      int testGetWDBVec, int testGetWLogic)
{
  rdbVector * vectorInfo;
  initRDBVector(&vectorInfo, 0, 1);
  if( !createNewDoubleVectorTable(sqlConn, vectorInfo) )
  {
     printf("ERROR Create Double Table: %s\n", mysql_error(sqlConn)); 
     return 0;
  }
  
  printf("Double Table:\t%ld\t%s\n", vectorInfo->metadataID, 
	 vectorInfo->tableName);

  if( testInsert )
  {  
    int dsize = 2;
    double darray[dsize];
    darray[0] = 4234.1233244;
    darray[1] = 432.789;
    if( !insertIntoDoubleVectorTable(sqlConn, vectorInfo, darray, dsize) )
    {
      printf("ERROR Insert Double Table: %s\n", mysql_error(sqlConn));
      return 0;
    }

    if( !printDoubleRDBVector(sqlConn, vectorInfo) )
      return 0;

    int index = 4;  
    double dnum = 0;
    char byte = 0;
    if( !getDoubleElement(sqlConn, vectorInfo, index, &dnum, &byte) )
    {
      printf("ERROR Get Double Value: %s\n", mysql_error(sqlConn));
      return 0;
    }

    if( byte == 1 )
      printf("From double table got elem %d: %lf\n", index, dnum);
    else
      printf("From double table got elem %d: NA\n", index);
  }

  if( testLoad )
  {
    if( !loadIntoVectorTable(sqlConn, vectorInfo, "testData/dataDouble.txt") )
    {
      printf("ERROR Load Doubless: %s\n", mysql_error(sqlConn));
      return 0;
    }
    printf("\tLoaded into Doubles table\n");
  
    if( !printDoubleRDBVector(sqlConn, vectorInfo) )
      return 0;
  }

  if( testGetRange )
  {
    int gt = 4;
    int lt = 7;
    int i, size = lt - gt + 1;
    double values[size];
    char byteArray[size];
    for( i = 0 ; i < size ; i++ )
      byteArray[i] = 0;

    if( !getRangeDoubleElements(sqlConn, vectorInfo, gt, lt, values, byteArray))
    {
      printf("ERROR Getting Range Doubles: %s\n", mysql_error(sqlConn));
      return 0;
    }

    printf("Getting Range of Doubles: (%d,%d):\n", gt, lt);
    for( i = 0 ; i < size ; i++ )
    {
      if( byteArray[i] == 1 )
	printf("\t%d\t%lf\n", gt+i, values[i]);
      else
	printf("\t%d\tNA\n", gt+i);
    }
  }

  if( testGetSparse )
  {
    unsigned int indexes[4] = {2, 3, 5, 1};
    int i, size = 4;
    double values[size];
    char byteArray[size];
    for( i = 0 ; i < size ; i++ )
      byteArray[i] = 0;

    if( !getSparseDoubleElements(sqlConn, vectorInfo, indexes, size,
				 values, byteArray) )
    {
       printf("ERROR Getting Sparse Doubles: %s\n", mysql_error(sqlConn));
       return 0;
    }

    printf("Getting Sparse Doubles\n");
    for( i = 0 ; i < size ; i++ )
    {
      if( byteArray[i] == 1 )
	printf("\t%d\t%lf\n", indexes[i], values[i]);
      else
	printf("\t%d\tNA\n", indexes[i]);
    }
  }

  if( testGetWDBVec )
  {
    /* Create an index table vector */
    rdbVector * indexVector;
    initRDBVector(&indexVector, 0, 1);
    if( !createNewIntVectorTable(sqlConn, indexVector) )
    {
      printf("ERROR Create Int Table: %s\n", mysql_error(sqlConn)); 
      return 0;
    }
  
    int indexes[5] = {3, 30, 12, 2, 3};
    int indexSize = 5;
    if( !insertIntoIntVectorTable(sqlConn, indexVector, indexes, indexSize) )
    {
      printf("ERROR Insert Int Table: %s\n", mysql_error(sqlConn));
      return 0;
    }

    /* Get the corresponding values */
    rdbVector * resultVector;
    initRDBVector(&resultVector, 1, 1);
    if( !getDoubleElementsWithDBVector(sqlConn, vectorInfo, indexVector, 
				       resultVector) )
    {
       printf("ERROR Getting Integers With DBVec: %s\n", mysql_error(sqlConn));
       return 0;
    }

    printf("Getting Doubles with indexes from %s \n", indexVector->tableName);
    printIntegerRDBVector(sqlConn, indexVector);
    printDoubleRDBVector(sqlConn, resultVector);

    clearRDBVector(&indexVector);
    clearRDBVector(&resultVector);

    /* Create an index table vector */
    rdbVector * indexVector2;
    initRDBVector(&indexVector2, 0, 1);
    if( !createNewDoubleVectorTable(sqlConn, indexVector2) )
    {
      printf("ERROR Create Double Table: %s\n", mysql_error(sqlConn)); 
      return 0;
    }
  
    double indexes2[5] = {3.2, 33.623, 17.23, 1.75, 2.9};
    int indexSize2 = 5;
    if( !insertIntoDoubleVectorTable(sqlConn, indexVector2,indexes2,indexSize2))
    {
      printf("ERROR Insert Double Table: %s\n", mysql_error(sqlConn));
      return 0;
    }

    /* Get the corresponding values */
    rdbVector * resultVector2;
    initRDBVector(&resultVector2, 1, 1);
    if( !getDoubleElementsWithDBVector(sqlConn, vectorInfo, indexVector2, 
				       resultVector2) )
    {
       printf("ERROR Getting Integers With DBVec: %s\n", mysql_error(sqlConn));
       return 0;
    }

    printf("Getting Doubles with indexes from %s\n", indexVector2->tableName);
    printDoubleRDBVector(sqlConn, indexVector2);
    printDoubleRDBVector(sqlConn, resultVector2);

    clearRDBVector(&indexVector2);
    clearRDBVector(&resultVector2);
  }

  if( testGetWLogic )
  {
    /* Create a logic table vector */
    rdbVector * logicVector;
    initRDBVector(&logicVector, 0, 1);
    if( !createNewLogicVectorTable(sqlConn, logicVector) )
    {
      printf("ERROR Create Logic Table: %s\n", mysql_error(sqlConn)); 
      return 0;
    }
  
    int indexes[5] = {1, 0, 0, 1, 1};
    int indexSize = 5;
    if( !insertIntoLogicVectorTable(sqlConn, logicVector, indexes, indexSize) )
    {
      printf("ERROR Insert Logic Table: %s\n", mysql_error(sqlConn));
      return 0;
    }

    /* Get the corresponding values */
    rdbVector * resultVector;
    initRDBVector(&resultVector, 1, 1);
    if( !getDoubleElementsWithLogic(sqlConn, vectorInfo, logicVector, 
				    resultVector) )
    {
       printf("ERROR Getting Double With Logic: %s\n", mysql_error(sqlConn));
       return 0;
    }

    printf("Getting Double elems using logic vector %s (1,0,0,1,1)\n",
	   logicVector->tableName);
    printDoubleRDBVector(sqlConn, resultVector);

    clearRDBVector(&logicVector);
    clearRDBVector(&resultVector);
  }

  clearRDBVector(&vectorInfo);
  printf("----------------------------------------\n");

  return 1;
}


/* ----------------------- TestStringVectors --------------------- */
int TestStringVectors(MYSQL * sqlConn, int testInsert, int testLoad, 
		      int testGetRange, int testGetSparse, 
		      int testGetWDBVec, int testGetWLogic)
{
  rdbVector * vectorInfo;
  initRDBVector(&vectorInfo, 0, 1);
  if( !createNewStringVectorTable(sqlConn, vectorInfo) )
  {
     printf("ERROR Create String Table: %s\n", mysql_error(sqlConn)); 
     return 0;
  }
  
  printf("String Table:\t%ld\t%s\n", vectorInfo->metadataID, 
	 vectorInfo->tableName);

  if( testInsert )
  {
    int sSize = 3;
    char * strArray[3] = {"Hello's @#$ yp<int>?","world{},[] \"done.","yes`!"};
    if( !insertIntoStringVectorTable(sqlConn, vectorInfo, strArray, sSize) )
    {
      printf("ERROR Insert String Table: %s\n", mysql_error(sqlConn));
      return 0;
    }

    if( !printStringRDBVector(sqlConn, vectorInfo) )
      return 0;

    int index = 5;
    char * value;
    char byte = 0;
    if( !getStringElement(sqlConn, vectorInfo, index, &value, &byte) )
    {
      printf("ERROR Get String Value: %s\n", mysql_error(sqlConn));
      return 0;
    }

    if( byte == 1 )
      printf("From string table got elem %d: %s\n", index, value);
    else
      printf("From string table got elem %d: NA\n", index);
  }

  if( testLoad )
  {
    if( !loadIntoVectorTable(sqlConn, vectorInfo, "testData/dataStrings.txt") )
    {
      printf("ERROR Load Strings: %s\n", mysql_error(sqlConn));
      return 0;
    }
    printf("\tLoaded into Strings table\n");

    if( !printStringRDBVector(sqlConn, vectorInfo) )
      return 0;
  }

  if( testGetRange )
  {
    int gt = 3;
    int lt = 6;
    int i, size = lt - gt + 1;
    char* values[size];
    char byteArray[size];
    for( i = 0 ; i < size ; i++ )
      byteArray[i] = 0;

    if( !getRangeStringElements(sqlConn, vectorInfo, gt, lt, values, byteArray))
    {
      printf("ERROR Getting Range Strings: %s\n", mysql_error(sqlConn));
      return 0;
    }

    printf("Getting Range of Strings: (%d,%d):\n", gt, lt);
    for( i = 0 ; i < size ; i++ )
    {
      if( byteArray[i] == 1 )
	printf("\t%d\t%s\n", gt+i, values[i]);
      else
	printf("\t%d\tNA\n", gt+i);
    }
  }

  if( testGetSparse )
  {
    unsigned int indexes[3] = {1, 4, 3};
    int i, size = 3;
    char* values[size];
    char byteArray[size];
    for( i = 0 ; i < size ; i++ )
      byteArray[i] = 0;

    if( !getSparseStringElements(sqlConn, vectorInfo, indexes, size, 
				 values, byteArray) )
    {
       printf("ERROR Getting Sparse Strings: %s\n", mysql_error(sqlConn));
       return 0;
    }

    printf("Getting Sparse Strings\n");
    for( i = 0 ; i < size ; i++ )
    {
      if( byteArray[i] == 1 )
	printf("\t%d\t%s\n", indexes[i], values[i]);
      else
	printf("\t%d\tNA\n", indexes[i]);
    }
  }

  if( testGetWDBVec )
  {
    /* Create an index table vector */
    rdbVector * indexVector;
    initRDBVector(&indexVector, 0, 1);
    if( !createNewIntVectorTable(sqlConn, indexVector) )
    {
      printf("ERROR Create Int Table: %s\n", mysql_error(sqlConn)); 
      return 0;
    }
  
    int indexes[5] = {3, 30, 12, 6, 3};
    int indexSize = 5;
    if( !insertIntoIntVectorTable(sqlConn, indexVector, indexes, indexSize) )
    {
      printf("ERROR Insert Int Table: %s\n", mysql_error(sqlConn));
      return 0;
    }

    /* Get the corresponding values */
    rdbVector * resultVector;
    initRDBVector(&resultVector, 1, 1);
    if( !getStringElementsWithDBVector(sqlConn, vectorInfo, indexVector, 
				       resultVector) )
    {
       printf("ERROR Getting Integers With DBVec: %s\n", mysql_error(sqlConn));
       return 0;
    }

    printf("Getting Strings with indexes from %s \n", indexVector->tableName);
    printIntegerRDBVector(sqlConn, indexVector);
    printStringRDBVector(sqlConn, resultVector);

    clearRDBVector(&indexVector);
    clearRDBVector(&resultVector);

    /* Create an index table vector */
    rdbVector * indexVector2;
    initRDBVector(&indexVector2, 0, 1);
    if( !createNewDoubleVectorTable(sqlConn, indexVector2) )
    {
      printf("ERROR Create Double Table: %s\n", mysql_error(sqlConn)); 
      return 0;
    }
  
    double indexes2[5] = {3.2, 33.623, 17.23, 5.75, 2.9};
    int indexSize2 = 5;
    if( !insertIntoDoubleVectorTable(sqlConn, indexVector2,indexes2,indexSize2))
    {
      printf("ERROR Insert Double Table: %s\n", mysql_error(sqlConn));
      return 0;
    }

    /* Get the corresponding values */
    rdbVector * resultVector2;
    initRDBVector(&resultVector2, 1, 1);
    if( !getStringElementsWithDBVector(sqlConn, vectorInfo, indexVector2, 
				       resultVector2) )
    {
       printf("ERROR Getting Integers With DBVec: %s\n", mysql_error(sqlConn));
       return 0;
    }

    printf("Getting Strings with indexes from %s\n", indexVector2->tableName);
    printDoubleRDBVector(sqlConn, indexVector2);
    printStringRDBVector(sqlConn, resultVector2);

    clearRDBVector(&indexVector2);
    clearRDBVector(&resultVector2);
  }

  if( testGetWLogic )
  {
    /* Create a logic table vector */
    rdbVector * logicVector;
    initRDBVector(&logicVector, 0, 1);
    if( !createNewLogicVectorTable(sqlConn, logicVector) )
    {
      printf("ERROR Create Logic Table: %s\n", mysql_error(sqlConn)); 
      return 0;
    }
  
    int indexes[5] = {1, 0, 0, 1, 1};
    int indexSize = 5;
    if( !insertIntoLogicVectorTable(sqlConn, logicVector, indexes, indexSize) )
    {
      printf("ERROR Insert Logic Table: %s\n", mysql_error(sqlConn));
      return 0;
    }

    /* Get the corresponding values */
    rdbVector * resultVector;
    initRDBVector(&resultVector, 1, 1);
    if( !getStringElementsWithLogic(sqlConn, vectorInfo, logicVector, 
				     resultVector) )
    {
       printf("ERROR Getting String With Logic: %s\n", mysql_error(sqlConn));
       return 0;
    }

    printf("Getting String elems using logic vector %s (1,0,0,1,1)\n",
	   logicVector->tableName);
    printStringRDBVector(sqlConn, resultVector);

    clearRDBVector(&logicVector);
    clearRDBVector(&resultVector);
  }

  clearRDBVector(&vectorInfo);
  printf("----------------------------------------\n");

  return 1;
}


/* ------------------------ TestComplexVectors --------------------- */
int TestComplexVectors(MYSQL * sqlConn, int testInsert, int testLoad,
		       int testGetRange, int testGetSparse, 
		       int testGetWDBVec, int testGetWLogic)
{
  rdbVector * vectorInfo;
  initRDBVector(&vectorInfo, 0, 1);
  if( !createNewComplexVectorTable(sqlConn, vectorInfo) )
  {
     printf("ERROR Create Complex Table: %s\n", mysql_error(sqlConn)); 
     return 0;
  }
  
  printf("Complex Table:\t%ld\t%s\n", vectorInfo->metadataID, 
	 vectorInfo->tableName);

  if( testInsert )
  {
    int cSize = 7;
    double realArray[3] = {123.43, 645.346, 76.12345};
    double imagArray[4] = {-12.435, 0, 321.234, 12};
    if( !insertIntoComplexVectorTable(sqlConn, vectorInfo, 
				      realArray, 3, imagArray, 4, cSize) )
    {
      printf("ERROR Insert Complex Table: %s\n", mysql_error(sqlConn));
      return 0;
    }

    if( !printComplexRDBVector(sqlConn, vectorInfo) )
      return 0;

    int index = 5;
    Rcomplex value;
    char byte = 0;
    if( !getComplexElement(sqlConn, vectorInfo, index, &value, &byte) )
    {
      printf("ERROR Get Complex Value: %s\n", mysql_error(sqlConn));
      return 0;
    }

    if( byte == 1 )
      printf("From complex table got elem %d: %lf, %lf\n", index, 
	     value.r, value.i);
    else
      printf("From complex table got elem %d: NA\n", index);
  }

  if( testLoad )
  {
    if(!loadIntoComplexVectorTable(sqlConn, vectorInfo, 
                                   "testData/dataComplex.txt"))
    {
      printf("ERROR Load Complex: %s\n", mysql_error(sqlConn));
      return 0;
    }
    printf("\tLoaded into Complex table\n");

    if( !printComplexRDBVector(sqlConn, vectorInfo) )
      return 0;
  }

  if( testGetRange )
  {
    int gt = 6;
    int lt = 9;
    int i, size = lt - gt + 1;
    Rcomplex values[size];
    char byteArray[size];
    for( i = 0 ; i < size ; i++ )
      byteArray[i] = 0;

    if( !getRangeComplexElements(sqlConn, vectorInfo, gt, lt,
				 values, byteArray) )
    {
      printf("ERROR Getting Range Complex: %s\n", mysql_error(sqlConn));
      return 0;
    }

    printf("Getting Range of Complex: (%d,%d):\n", gt, lt);
    for( i = 0 ; i < size ; i++ )
    {
      if( byteArray[i] == 1 )
	printf("\t%d\t%lf\t%lf\n", gt+i, values[i].r, values[i].i);
      else
	printf("\t%d\tNA\tNA\n", gt+i);
    }    
  }

  if( testGetSparse )
  {
    unsigned int indexes[3] = {3, 9, 5};
    int i, size = 3;
    Rcomplex values[size];
    char byteArray[size];
    for( i = 0 ; i < size ; i++ )
      byteArray[i] = 0;

    if( !getSparseComplexElements(sqlConn, vectorInfo, indexes, 
				  size, values, byteArray) )
    {
       printf("ERROR Getting Sparse Complex: %s\n", mysql_error(sqlConn));
       return 0;
    }

    printf("Getting Sparse Complex\n");
    for( i = 0 ; i < size ; i++ )
    {
      if( byteArray[i] == 1 )
	printf("\t%d\t%lf\t%lf\n", indexes[i], values[i].r, values[i].i);
      else
	printf("\t%d\tNA\tNA\n", indexes[i]);
    }
  }

  if( testGetWDBVec )
  {
    /* Create an index table vector */
    rdbVector * indexVector;
    initRDBVector(&indexVector, 0, 1);
    if( !createNewIntVectorTable(sqlConn, indexVector) )
    {
      printf("ERROR Create Int Table: %s\n", mysql_error(sqlConn)); 
      return 0;
    }
  
    int indexes[5] = {3, 30, 12, 6, 3};
    int indexSize = 5;
    if( !insertIntoIntVectorTable(sqlConn, indexVector, indexes, indexSize) )
    {
      printf("ERROR Insert Int Table: %s\n", mysql_error(sqlConn));
      return 0;
    }

    /* Get the corresponding values */
    rdbVector * resultVector;
    initRDBVector(&resultVector, 1, 1);
    if( !getComplexElementsWithDBVector(sqlConn, vectorInfo, indexVector, 
					resultVector) )
    {
       printf("ERROR Getting Integers With DBVec: %s\n", mysql_error(sqlConn));
       return 0;
    }

    printf("Getting Complex with indexes from %s \n", indexVector->tableName);
    printIntegerRDBVector(sqlConn, indexVector);
    printComplexRDBVector(sqlConn, resultVector);

    clearRDBVector(&indexVector);
    clearRDBVector(&resultVector);

    /* Create an index table vector */
    rdbVector * indexVector2;
    initRDBVector(&indexVector2, 0, 1);
    if( !createNewDoubleVectorTable(sqlConn, indexVector2) )
    {
      printf("ERROR Create Double Table: %s\n", mysql_error(sqlConn)); 
      return 0;
    }
  
    double indexes2[5] = {3.2, 33.623, 17.23, 5.75, 2.9};
    int indexSize2 = 5;
    if( !insertIntoDoubleVectorTable(sqlConn, indexVector2,indexes2,indexSize2))
    {
      printf("ERROR Insert Double Table: %s\n", mysql_error(sqlConn));
      return 0;
    }

    /* Get the corresponding values */
    rdbVector * resultVector2;
    initRDBVector(&resultVector2, 1, 1);
    if( !getComplexElementsWithDBVector(sqlConn, vectorInfo, indexVector2, 
					resultVector2) )
    {
       printf("ERROR Getting Integers With DBVec: %s\n", mysql_error(sqlConn));
       return 0;
    }

    printf("Getting Complex with indexes from %s\n", indexVector2->tableName);
    printDoubleRDBVector(sqlConn, indexVector2);
    printComplexRDBVector(sqlConn, resultVector2);

    clearRDBVector(&indexVector2);
    clearRDBVector(&resultVector2);
  }

  if( testGetWLogic )
  {
    /* Create a logic table vector */
    rdbVector * logicVector;
    initRDBVector(&logicVector, 0, 1);
    if( !createNewLogicVectorTable(sqlConn, logicVector) )
    {
      printf("ERROR Create Logic Table: %s\n", mysql_error(sqlConn)); 
      return 0;
    }
  
    int indexes[5] = {1, 0, 0, 1, 1};
    int indexSize = 5;
    if( !insertIntoLogicVectorTable(sqlConn, logicVector, indexes, indexSize) )
    {
      printf("ERROR Insert Logic Table: %s\n", mysql_error(sqlConn));
      return 0;
    }

    /* Get the corresponding values */
    rdbVector * resultVector;
    initRDBVector(&resultVector, 1, 1);
    if( !getComplexElementsWithLogic(sqlConn, vectorInfo, logicVector, 
				     resultVector) )
    {
       printf("ERROR Getting Complex With Logic: %s\n", mysql_error(sqlConn));
       return 0;
    }

    printf("Getting Complex elems using logic vector %s (1,0,0,1,1)\n",
	   logicVector->tableName);
    printComplexRDBVector(sqlConn, resultVector);

    clearRDBVector(&logicVector);
    clearRDBVector(&resultVector);
  }

  clearRDBVector(&vectorInfo);
  printf("----------------------------------------\n");

  return 1;
}


/* ------------------ TestLogicVectors---------------------------- */
int TestLogicVectors(MYSQL * sqlConn, int testInsert, int testLoad,
		     int testGetRange, int testGetSparse, 
		     int testGetWDBVec, int testGetWLogic)
{ 
  rdbVector * vectorInfo;
  initRDBVector(&vectorInfo, 0, 1);
  if( !createNewLogicVectorTable(sqlConn, vectorInfo) )
  {
     printf("ERROR Create Logic Table: %s\n", mysql_error(sqlConn)); 
     return 0;
  }
  
  printf("Logic Table:\t%ld\t%s\n", vectorInfo->metadataID, 
	 vectorInfo->tableName);

  if( testInsert )
  {
    int isize = 7;
    int iarray[7] = {1, 0, 0, 1, 1, 2, 0};
    if( !insertIntoLogicVectorTable(sqlConn, vectorInfo, iarray, isize) )
    {
      printf("ERROR Insert Logic Table: %s\n", mysql_error(sqlConn));
      return 0;
    }

    if( !printLogicRDBVector(sqlConn, vectorInfo) )
      return 0;

    int index = 3;
    int num = 0;
    char byte = 0;
    if( !getLogicElement(sqlConn, vectorInfo, index, &num, &byte) )
    {
      printf("ERROR Get Logic Value: %s\n", mysql_error(sqlConn));
      return 0;
    }

    if( byte == 1 )
      printf("From logic table got elem %d: %s\n", index, (num)?"TRUE":"FALSE");
    else
      printf("From logic table got elem %d: NA\n", index);
  }

  if( testLoad )
  {
    if( !loadIntoVectorTable(sqlConn, vectorInfo, "testData/dataLogic.txt") )
    {
      printf("ERROR Load Logicss: %s\n", mysql_error(sqlConn));
      return 0;
    }
    printf("\tLoaded into Logic's table\n");

    if( !printLogicRDBVector(sqlConn, vectorInfo) )
      return 0;
  }

  if( testGetRange )
  {
    int gt = 5;
    int lt = 9;
    int i, size = lt - gt + 1;
    int values[size];
    char byteArray[size];
    for( i = 0 ; i < size ; i++ )
      byteArray[i] = 0;

    if( !getRangeLogicElements(sqlConn, vectorInfo, gt, lt, values, byteArray) )
    {
       printf("ERROR Getting Range Logic: %s\n", mysql_error(sqlConn));
       return 0;
    }

    printf("Getting Range of Logic (%d,%d):\n", gt, lt);
    for( i = 0 ; i < size ; i++ )
    {
      if( byteArray[i] == 1 )
	printf("\t%d\t%s\n", gt+i, (values[i]) ? "TRUE" : "FALSE");
      else
	printf("\t%d\tNA\n", gt+i);
    }
  }

  if( testGetSparse )
  {
    unsigned int indexes[4] = {3, 30, 12, 6};
    int i, size = 4;
    int values[size];
    char byteArray[size];
    for( i = 0 ; i < size ; i++ )
      byteArray[i] = 0;

    if( !getSparseLogicElements(sqlConn, vectorInfo, indexes, size, 
			      values, byteArray) )
    {
       printf("ERROR Getting Sparse Logic: %s\n", mysql_error(sqlConn));
       return 0;
    }

    printf("Getting Sparse Logic\n");
    for( i = 0 ; i < size ; i++ )
    {
      if( byteArray[i] == 1 )
	printf("\t%d\t%s\n", indexes[i], (values[i]) ? "TRUE" : "FALSE");
      else
	printf("\t%d\tNA\n", indexes[i]);
    }    
  }

  if( testGetWDBVec )
  {
    /* Create an index table vector */
    rdbVector * indexVector;
    initRDBVector(&indexVector, 0, 1);
    if( !createNewIntVectorTable(sqlConn, indexVector) )
    {
      printf("ERROR Create Int Table: %s\n", mysql_error(sqlConn)); 
      return 0;
    }
  
    int indexes[5] = {3, 30, 12, 6, 3};
    int indexSize = 5;
    if( !insertIntoIntVectorTable(sqlConn, indexVector, indexes, indexSize) )
    {
      printf("ERROR Insert Int Table: %s\n", mysql_error(sqlConn));
      return 0;
    }

    /* Get the corresponding values */
    rdbVector * resultVector;
    initRDBVector(&resultVector, 1, 1);
    if( !getLogicElementsWithDBVector(sqlConn, vectorInfo, indexVector, 
				      resultVector) )
    {
       printf("ERROR Getting Logic With DBVec: %s\n", mysql_error(sqlConn));
       return 0;
    }

    printf("Getting Logic with indexes from %s \n", indexVector->tableName);
    printIntegerRDBVector(sqlConn, indexVector);
    printLogicRDBVector(sqlConn, resultVector);

    clearRDBVector(&indexVector);
    clearRDBVector(&resultVector);

    /* Create an index table vector */
    rdbVector * indexVector2;
    initRDBVector(&indexVector2, 0, 1);
    if( !createNewDoubleVectorTable(sqlConn, indexVector2) )
    {
      printf("ERROR Create Double Table: %s\n", mysql_error(sqlConn)); 
      return 0;
    }
  
    double indexes2[5] = {3.2, 33.623, 17.23, 5.75, 2.9};
    int indexSize2 = 5;
    if( !insertIntoDoubleVectorTable(sqlConn, indexVector2,indexes2,indexSize2))
    {
      printf("ERROR Insert Double Table: %s\n", mysql_error(sqlConn));
      return 0;
    }

    /* Get the corresponding values */
    rdbVector * resultVector2;
    initRDBVector(&resultVector2, 1, 1);
    if( !getLogicElementsWithDBVector(sqlConn, vectorInfo, indexVector2, 
				      resultVector2) )
    {
       printf("ERROR Getting Logic With DBVec: %s\n", mysql_error(sqlConn));
       return 0;
    }

    printf("Getting Logic with indexes from %s\n", indexVector2->tableName);
    printDoubleRDBVector(sqlConn, indexVector2);
    printLogicRDBVector(sqlConn, resultVector2);

    clearRDBVector(&indexVector2);
    clearRDBVector(&resultVector2);
  }

  if( testGetWLogic )
  {
    /* Create a logic table vector */
    rdbVector * logicVector;
    initRDBVector(&logicVector, 0, 1);
    if( !createNewLogicVectorTable(sqlConn, logicVector) )
    {
      printf("ERROR Create Logic Table: %s\n", mysql_error(sqlConn)); 
      return 0;
    }
  
    int indexes[5] = {1, 0, 0, 1, 1};
    int indexSize = 5;
    if( !insertIntoLogicVectorTable(sqlConn, logicVector, indexes, indexSize) )
    {
      printf("ERROR Insert Logic Table: %s\n", mysql_error(sqlConn));
      return 0;
    }

    /* Get the corresponding values */
    rdbVector * resultVector;
    initRDBVector(&resultVector, 1, 1);
    if( !getLogicElementsWithLogic(sqlConn, vectorInfo, logicVector, 
				   resultVector) )
    {
       printf("ERROR Getting Logic With Logic: %s\n", mysql_error(sqlConn));
       return 0;
    }

    printf("Getting Logic elems using logic vector %s (1,0,0,1,1)\n",
	   logicVector->tableName);
    printLogicRDBVector(sqlConn, resultVector);

    clearRDBVector(&logicVector);
    clearRDBVector(&resultVector);
  }

  clearRDBVector(&vectorInfo);
  printf("----------------------------------------\n");

  return 1;
}


/* --------------------- TestLoadAllVectors ------------------------- */
int TestLoadAllVectors(MYSQL * sqlConn, int testDelete)
{
  rdbVector ** arrayVectorInfoObjects;
  unsigned long int numTables;

  if( !loadAllRDBVectors(sqlConn, &arrayVectorInfoObjects, &numTables) )
  {
     printf("ERROR Load RDB Vectors: %s\n", mysql_error(sqlConn));
     return 0;
  }

  printf("\nNumber of tables: %ld\n", numTables);
  int i;
  for( i = 0 ; i < numTables ; i++ )
  {
     printf("\t%ld\t%s", 
	    arrayVectorInfoObjects[i]->metadataID,
	    arrayVectorInfoObjects[i]->tableName);

     if( testDelete )
     {
        deleteRDBVector(sqlConn, arrayVectorInfoObjects[i]);
	printf("\t--Deleted! %s", mysql_error(sqlConn));
     }
     printf("\n");
  }

  for( i = 0 ; i < numTables ; i++ )
  {
     clearRDBVector(&arrayVectorInfoObjects[i]);
  }

  if( numTables != 0 )
  {
     free(arrayVectorInfoObjects);
  }
  printf("----------------------------------------\n");

  return 1;
}


/* ----------------- Test Inserting a sequence ----------------------- */
int TestIntegerSequence(MYSQL* sqlConn, int start, int end, int step)
{
  rdbVector * vectorInfo;
  initRDBVector(&vectorInfo, 0, 1);
  if( !createNewIntVectorTable(sqlConn, vectorInfo) )
  {
     printf("ERROR Create Int Seq Table: %s\n", mysql_error(sqlConn)); 
     return 0;
  }
  
  printf("Integer Seq Table:\t%ld\t%s\n", vectorInfo->metadataID, 
	 vectorInfo->tableName);

  if( !insertSeqIntVectorTable(sqlConn, vectorInfo, start, end, step) )
  {
     printf("ERROR Insert Seq Int Table: %s\n", mysql_error(sqlConn));
     return 0;
  }

  if( !printIntegerRDBVector(sqlConn, vectorInfo) )
    return 0;

  clearRDBVector(&vectorInfo);
  printf("----------------------------------------\n");

  return 1;
}


int TestDoubleSequence(MYSQL* sqlConn, double start, double end, double step)
{
  rdbVector * vectorInfo;
  initRDBVector(&vectorInfo, 0, 1);
  if( !createNewDoubleVectorTable(sqlConn, vectorInfo) )
  {
     printf("ERROR Create Double Seq Table: %s\n", mysql_error(sqlConn)); 
     return 0;
  }
  
  printf("Double Seq Table:\t%ld\t%s\n", vectorInfo->metadataID, 
	 vectorInfo->tableName);

  if( !insertSeqDoubleVectorTable(sqlConn, vectorInfo, start, end, step) )
  {
     printf("ERROR Insert Seq Double Table: %s\n", mysql_error(sqlConn));
     return 0;
  }

  if( !printDoubleRDBVector(sqlConn, vectorInfo) )
    return 0;

  clearRDBVector(&vectorInfo);
  printf("----------------------------------------\n");

  return 1;
}


/* -------------------- Test Binary Ops ---------------------------- */
int TestIntegerBinaryOps(MYSQL* sqlConn, int testAdd, int testMinus,
			 int testMult, int testDiv, int testMod)
{
  /* Create the tables */
  rdbVector *input1, *input2;
  initRDBVector(&input1, 0, 1);
  initRDBVector(&input2, 0, 1);

  if( !createNewIntVectorTable(sqlConn, input1) ||
      !createNewIntVectorTable(sqlConn, input2) )
  {
     printf("ERROR Create Int Table: %s\n", mysql_error(sqlConn)); 
     return 0;
  }
  
  /* Insert some test values */
  int size1 = 5;
  int size2 = 8;
  int array1[5] = {2, -3, 7, 9, 14};
  int array2[8] = {4, 0, 5, -8, 10, -11, 15, 20};
  if( !insertIntoIntVectorTable(sqlConn, input1, array1, size1) ||
      !insertIntoIntVectorTable(sqlConn, input2, array2, size2) )
  {
    printf("ERROR Insert Int Table: %s\n", mysql_error(sqlConn));
    return 0;
  }

  if( testAdd )
  {
    printf("Testing Add for Integers:\n");
    rdbVector * result;
    initRDBVector(&result, 1, 1);

    if( !addNumericVectors(sqlConn, result, input1, input2) )
    {
      printf("ERROR Adding Integers: %s\n", mysql_error(sqlConn));
      return 0;
    }

    if( !printIntegerRDBVector(sqlConn, result) )
      return 0;

    clearRDBVector(&result);
  }

  if( testMinus )
  {
    printf("Testing Minus for Integers:\n");
    rdbVector * result;
    initRDBVector(&result, 1, 1);

    if( !subtractNumericVectors(sqlConn, result, input1, input2) )
    {
      printf("ERROR Subtracting Integers: %s\n", mysql_error(sqlConn));
      return 0;
    }

    if( !printIntegerRDBVector(sqlConn, result) )
      return 0;
    
    clearRDBVector(&result);
  }

  if( testMult )
  {
    printf("Testing Mult for Integers:\n");
    rdbVector * result;
    initRDBVector(&result, 1, 1);

    if( !multiplyNumericVectors(sqlConn, result, input1, input2) )
    {
      printf("ERROR Multiplying Integers: %s\n", mysql_error(sqlConn));
      return 0;
    }

    if( !printIntegerRDBVector(sqlConn, result) )
      return 0;
    
    clearRDBVector(&result);
  }

  if( testDiv )
  {
    printf("Testing Division for Integers:\n");
    rdbVector * result;
    initRDBVector(&result, 1, 1);

    if( !divideNumericVectors(sqlConn, result, input1, input2) )
    {
      printf("ERROR Dividing Integers: %s\n", mysql_error(sqlConn));
      return 0;
    }

    if( !printDoubleRDBVector(sqlConn, result) )
      return 0;
    
    clearRDBVector(&result);
  } 

  if( testMod )
  {
    printf("Testing Mod for Integers:\n");
    rdbVector * result;
    initRDBVector(&result, 1, 1);

    if( !modNumericVectors(sqlConn, result, input1, input2) )
    {
      printf("ERROR Mod Integers: %s\n", mysql_error(sqlConn));
      return 0;
    }

    if( !printIntegerRDBVector(sqlConn, result) )
      return 0;

    clearRDBVector(&result);
  }

  clearRDBVector(&input1);
  clearRDBVector(&input2);
  printf("----------------------------------------\n");

  return 1;

}


int TestDoubleBinaryOps(MYSQL* sqlConn, int testAdd, int testMinus,
			int testMult, int testDiv)
{
  /* Create the tables */
  rdbVector *input1, *input2;
  initRDBVector(&input1, 0, 1);
  initRDBVector(&input2, 0, 1);

  if( !createNewDoubleVectorTable(sqlConn, input1) ||
      !createNewIntVectorTable(sqlConn, input2) )
  {
     printf("ERROR Create Double Table: %s\n", mysql_error(sqlConn)); 
     return 0;
  }
  
  /* Insert some test values */
  int size1 = 5;
  int size2 = 8;
  double array1[5] = {2.5, 3.2, 7.4, 9.1, 14.0};
  int array2[8] = {4, 6, 5, 8, 10, 11, 15, 20};
  if( !insertIntoDoubleVectorTable(sqlConn, input1, array1, size1) ||
      !insertIntoIntVectorTable(sqlConn, input2, array2, size2) )
  {
    printf("ERROR Insert Double Table: %s\n", mysql_error(sqlConn));
    return 0;
  }

  if( testAdd )
  {
    printf("Testing Add for Doubles:\n");
    rdbVector * result;
    initRDBVector(&result, 1, 1);

    if( !addNumericVectors(sqlConn, result, input1, input2) )
    {
      printf("ERROR Adding Doubles: %s\n", mysql_error(sqlConn));
      return 0;
    }

    if( !printDoubleRDBVector(sqlConn, result) )
      return 0;
    
    clearRDBVector(&result);
  }

  if( testMinus )
  {
    printf("Testing Minus for Doubles:\n");
    rdbVector * result;
    initRDBVector(&result, 1, 1);

    if( !subtractNumericVectors(sqlConn, result, input1, input2) )
    {
      printf("ERROR Subtracting Doubles: %s\n", mysql_error(sqlConn));
      return 0;
    }

    if( !printDoubleRDBVector(sqlConn, result) )
      return 0;

    clearRDBVector(&result);
  }

  if( testMult )
  {
    printf("Testing Mult for Doubles:\n");
    rdbVector * result;
    initRDBVector(&result, 1, 1);

    if( !multiplyNumericVectors(sqlConn, result, input1, input2) )
    {
      printf("ERROR Multiplying Doubles: %s\n", mysql_error(sqlConn));
      return 0;
    }

    if( !printDoubleRDBVector(sqlConn, result) )
      return 0;
    
    clearRDBVector(&result);
  }

  if( testDiv )
  {
    printf("Testing Div for Doubles:\n");
    rdbVector * result;
    initRDBVector(&result, 1, 1);

    if( !divideNumericVectors(sqlConn, result, input1, input2) )
    {
      printf("ERROR Dividing Doubles: %s\n", mysql_error(sqlConn));
      return 0;
    }

    if( !printDoubleRDBVector(sqlConn, result) )
      return 0;
    
    clearRDBVector(&result);
  }

  clearRDBVector(&input1);
  clearRDBVector(&input2);
  printf("----------------------------------------\n");

  return 1;
}


int TestComplexBinaryOps(MYSQL* sqlConn, int testAdd, int testMinus,
			 int testMult, int testDiv)
{
  /* Create the tables */
  rdbVector *input1, *input2, *inputInts, *inputDoubles;
  initRDBVector(&input1, 0, 1);
  initRDBVector(&input2, 0, 1);
  initRDBVector(&inputInts, 0, 1);
  initRDBVector(&inputDoubles, 0, 1);

  if( !createNewComplexVectorTable(sqlConn, input1) ||
      !createNewComplexVectorTable(sqlConn, input2) ||
      !createNewIntVectorTable(sqlConn, inputInts) ||
      !createNewDoubleVectorTable(sqlConn, inputDoubles) )
  {
     printf("ERROR Create Table for Complex Ops: %s\n", mysql_error(sqlConn)); 
     return 0;
  }

  /* Insert some test values */
  double reals[5] = {2.5, 3.2, 7.4, 9.1, 14.0};
  double imags[5] = {2.5, 8.2, -7.4, 1.1, 7};
  if( !insertIntoComplexVectorTable(sqlConn, input1, reals, 5, imags, 5, 5) ||
      !loadIntoComplexVectorTable(sqlConn, input2,"testData/dataComplex.txt") ||
      !loadIntoVectorTable(sqlConn, inputInts, "testData/dataInt.txt") ||
      !loadIntoVectorTable(sqlConn, inputDoubles, "testData/dataDouble.txt") )
  {
    printf("ERROR Insert/Load Tables: %s\n", mysql_error(sqlConn));
    return 0;
  }

  printComplexRDBVector(sqlConn, input1);
  printComplexRDBVector(sqlConn, input2);
  printIntegerRDBVector(sqlConn, inputInts);
  printDoubleRDBVector(sqlConn, inputDoubles);

  if( testAdd )
  {
    printf("Testing Add for Complex:\n");
    rdbVector * result;
    initRDBVector(&result, 1, 1);

    if( !addComplexVectors(sqlConn, result, input1, input2) )
    {
      printf("ERROR Adding Complex: %s\n", mysql_error(sqlConn));
      return 0;
    }

    if( !printComplexRDBVector(sqlConn, result) )
      return 0;
    
    clearRDBVector(&result);
  }

  if( testMinus )
  {
    printf("Testing Minus for Complex:\n");
    rdbVector * result;
    initRDBVector(&result, 1, 1);

    if( !subtractComplexVectors(sqlConn, result, input1, inputInts) )
    {
      printf("ERROR Subtracting Complex: %s\n", mysql_error(sqlConn));
      return 0;
    }

    if( !printComplexRDBVector(sqlConn, result) )
      return 0;

    clearRDBVector(&result);
  }

  if( testMult )
  {
    printf("Testing Mult for Complex:\n");
    rdbVector * result;
    initRDBVector(&result, 1, 1);

    if( !multiplyComplexVectors(sqlConn, result, inputDoubles, input2) )
    {
      printf("ERROR Multiplying Complex: %s\n", mysql_error(sqlConn));
      return 0;
    }

    if( !printComplexRDBVector(sqlConn, result) )
      return 0;
    
    clearRDBVector(&result);
  }

  if( testDiv )
  {
    printf("Testing Div for Complex:\n");
    rdbVector * result;
    initRDBVector(&result, 1, 1);

    if( !divideComplexVectors(sqlConn, result, input1, input2) )
    {
      printf("ERROR Dividing Complex: %s\n", mysql_error(sqlConn));
      return 0;
    }

    if( !printComplexRDBVector(sqlConn, result) )
      return 0;
    
    clearRDBVector(&result);
  }

  clearRDBVector(&input1);
  clearRDBVector(&input2);
  printf("----------------------------------------\n");

  return 1;
}


/* ----------------- Test Binary Ops for Logic ------------------------ */
int TestLogicBinaryOps(MYSQL* sqlConn, int testAdd, int testMinus,
			 int testMult, int testDiv)
{
  /* Create the tables */
  rdbVector *input1, *input2, *input3, *input4;
  initRDBVector(&input1, 0, 1);
  initRDBVector(&input2, 0, 1);
  initRDBVector(&input3, 0, 1);
  initRDBVector(&input4, 0, 1);

  if( !createNewLogicVectorTable(sqlConn, input1) ||
      !createNewIntVectorTable(sqlConn, input2) ||
      !createNewDoubleVectorTable(sqlConn, input3) ||
      !createNewComplexVectorTable(sqlConn, input4) )
  {
     printf("ERROR Create all Tables: %s\n",mysql_error(sqlConn)); 
     return 0;
  }
  
  /* Insert some test values */
  int size1 = 5;
  int size2 = 8;
  int size3 = 3;
  int size4 = 4;
  int array1[5] = {1, 0, 0, 1, 0};
  int array2[8] = {4, 6, 5, -8, 10, -11, 15, 20};
  double array3[3] = {12.23, 44.42, -123.99};
  double array4R[4] = {43.23, 564.12, -23.2, 321};
  double array4I[4] = {3.12, 5432.12, 23, 12.3};
  if( !insertIntoLogicVectorTable(sqlConn, input1, array1, size1) ||
      !insertIntoIntVectorTable(sqlConn, input2, array2, size2) ||
      !insertIntoDoubleVectorTable(sqlConn, input3, array3, size3) ||
      !insertIntoComplexVectorTable(sqlConn, input4, array4R, size4, array4I,
				    size4, size4) )
  {
    printf("ERROR Insert all Tables: %s\n", mysql_error(sqlConn));
    return 0;
  }

  if( testAdd )
  {
    printf("Testing Add for Logic/Integers:\n");
    rdbVector * result;
    initRDBVector(&result, 1, 1);

    if( !addNumericVectors(sqlConn, result, input1, input2) )
    {
      printf("ERROR Adding Logic/Integers: %s\n", mysql_error(sqlConn));
      return 0;
    }

    if( !printIntegerRDBVector(sqlConn, result) )
      return 0;

    clearRDBVector(&result);
  }

  if( testMinus )
  {
    printf("Testing Minus for Logic/Integers:\n");
    rdbVector * result;
    initRDBVector(&result, 1, 1);

    if( !subtractNumericVectors(sqlConn, result, input1, input2) )
    {
      printf("ERROR Subtracting Logic/Integers: %s\n", mysql_error(sqlConn));
      return 0;
    }

    if( !printIntegerRDBVector(sqlConn, result) )
      return 0;
    
    clearRDBVector(&result);
  }

  if( testMult )
  {
    printf("Testing Mult for Logic/Complex:\n");
    rdbVector * result;
    initRDBVector(&result, 1, 1);

    if( !multiplyComplexVectors(sqlConn, result, input1, input4) )
    {
      printf("ERROR Multiplying Logic/Comlpex: %s\n", mysql_error(sqlConn));
      return 0;
    }

    if( !printComplexRDBVector(sqlConn, result) )
      return 0;
    
    clearRDBVector(&result);
  }

  if( testDiv )
  {
    printf("Testing Division for Logic/Doubles:\n");
    rdbVector * result;
    initRDBVector(&result, 1, 1);

    if( !divideNumericVectors(sqlConn, result, input1, input3) )
    {
      printf("ERROR Dividing Logic/Doubles: %s\n", mysql_error(sqlConn));
      return 0;
    }

    if( !printDoubleRDBVector(sqlConn, result) )
      return 0;
    
    clearRDBVector(&result);
  } 

  clearRDBVector(&input1);
  clearRDBVector(&input2);
  printf("----------------------------------------\n");

  return 1;

}


/* ------------------------ Test Integer Setters ----------------------- */
int TestIntegerSetters(MYSQL* sqlConn, int testSetElem, int testSetRange, 
		       int testSetSparse, int testSetWDBV, int testSetWLogic)
{
  rdbVector * vectorInfo;
  initRDBVector(&vectorInfo, 0, 1);
  if( !createNewIntVectorTable(sqlConn, vectorInfo) )
  {
     printf("ERROR Create Int Table: %s\n", mysql_error(sqlConn)); 
     return 0;
  }
  
  printf("Integer Table:\t%ld\t%s\n", vectorInfo->metadataID, 
	 vectorInfo->tableName);

  if( !loadIntoVectorTable(sqlConn, vectorInfo, "testData/dataInt.txt") )
  {
    printf("ERROR Load Ints: %s\n", mysql_error(sqlConn));
    return 0;
  }

  if( !printIntegerRDBVector(sqlConn, vectorInfo) )
      return 0;
 
  if( testSetElem )
  {
    int lSize, aSize, flag;
    getLogicalVectorSize(sqlConn, vectorInfo, &lSize);
    getActualVectorSize(sqlConn, vectorInfo, &aSize);
    hasRDBVectorAnyNA(sqlConn, vectorInfo, &flag);
    printf("Logic Size: %d\t Actual Size: %d\tHas NA: %d\n", lSize, aSize, flag);

    printf("Setting Integer Elements (5, 122) & (19, 222)\n");
    if( !setIntElement(sqlConn, vectorInfo, 5, 122) ||
	!setIntElement(sqlConn, vectorInfo, 19, 222))
    {
       printf("ERROR Setting Ints: %s\n", mysql_error(sqlConn));
       return 0;
    }

    if( !printIntegerRDBVector(sqlConn, vectorInfo) )
      return 0;

    getLogicalVectorSize(sqlConn, vectorInfo, &lSize);
    getActualVectorSize(sqlConn, vectorInfo, &aSize);
    hasRDBVectorAnyNA(sqlConn, vectorInfo, &flag);
    printf("Logic Size: %d\t Actual Size: %d\tHas NA: %d\n",
           lSize, aSize, flag);

    rdbVector * resultVector;
    initRDBVector(&resultVector, 0, 1);
    if( !checkRDBVectorForNA(sqlConn, vectorInfo, resultVector) )
    {
       printf("ERROR Checking for NA: %s\n", mysql_error(sqlConn));
       return 0;
    }

    printf("Checking for NA in %s\n", vectorInfo->tableName);
    if( !printLogicRDBVector(sqlConn, resultVector) )
      return 0;
  }

  if( testSetRange )
  {
    int gt = 8;
    int lt = 17;
    int values[3] = {11, 22, 33};
    printf("Setting Integer Elements in range (%d, %d)\n", gt, lt);

    if( !setRangeIntElements(sqlConn, vectorInfo, gt, lt, values, 3) )
    {
       printf("ERROR Setting Range Ints: %s\n", mysql_error(sqlConn));
       return 0;
    }

    if( !printIntegerRDBVector(sqlConn, vectorInfo) )
      return 0;
  }

  if( testSetSparse )
  {
    unsigned int indexes[5] = {8, 3, 25, 22, 12};
    int values[3] = {100, 200, 300};
    printf("Setting Integer Elements with indexes (8, 3, 25, 22, 12)\n");

    if( !setSparseIntElements(sqlConn, vectorInfo, indexes, 5, values, 3) )
    {
       printf("ERROR Setting Range Ints: %s\n", mysql_error(sqlConn));
       return 0;
    }

    if( !printIntegerRDBVector(sqlConn, vectorInfo) )
      return 0;
  }

  if( testSetWDBV )
  {
    rdbVector * indexVector;
    initRDBVector(&indexVector, 0, 1);
    if( !createNewDoubleVectorTable(sqlConn, indexVector) )
    {
      printf("ERROR Create Index Table: %s\n", mysql_error(sqlConn)); 
      return 0;
    }

    int isize = 5;
    double iarray[5] = {6.0, 3.2, 18.9, 1.532, 2.67};
    if( !insertIntoDoubleVectorTable(sqlConn, indexVector, iarray, isize) ||
	!setDoubleElement(sqlConn, indexVector, 8, 8.88) )
    {
      printf("ERROR Insert Int Table: %s\n", mysql_error(sqlConn));
      return 0;
    }

    int values[3] = {100, 200, 300};
    printf("Setting Integer Elements with indexes from DBVector\n");
    if( !setIntElementsWDBVector(sqlConn, vectorInfo, indexVector, values, 3) )
    {
       printf("ERROR Setting Ints w/ RDBV: %s\n", mysql_error(sqlConn));
       return 0;
    }

    if( !printDoubleRDBVector(sqlConn, indexVector) )
      return 0;

    if( !printIntegerRDBVector(sqlConn, vectorInfo) )
      return 0;
  }

  if( testSetWLogic )
  {
    rdbVector * logicVector;
    initRDBVector(&logicVector, 0, 1);
    if( !createNewLogicVectorTable(sqlConn, logicVector) )
    {
      printf("ERROR Create Index Table: %s\n", mysql_error(sqlConn)); 
      return 0;
    }

    int isize = 3;
    int iarray[3] = {1, 0, 0};
    if( !insertIntoLogicVectorTable(sqlConn, logicVector, iarray, isize) )
    {
      printf("ERROR Insert Logic Table: %s\n", mysql_error(sqlConn));
      return 0;
    }

    int values[2] = {1111, 2222};
    printf("Setting Integer Elements with indexes from Logic\n");
    if( !setIntElementsWithLogic(sqlConn, vectorInfo, logicVector, values, 2) )
    {
       printf("ERROR Setting Int w/ Logic: %s\n", mysql_error(sqlConn));
       return 0;
    }

    if( !printLogicRDBVector(sqlConn, logicVector) )
      return 0;

    if( !printIntegerRDBVector(sqlConn, vectorInfo) )
      return 0;
  }

  printf("----------------------------------------\n");

  return 1;
}


/* ------------------------ Test Double Setters ----------------------- */
int TestDoubleSetters(MYSQL* sqlConn, int testSetElem, int testSetRange, 
		      int testSetSparse, int testSetWDBV, int testSetWLogic)
{
  rdbVector * vectorInfo;
  initRDBVector(&vectorInfo, 0, 1);
  if( !createNewDoubleVectorTable(sqlConn, vectorInfo) )
  {
     printf("ERROR Create Double Table: %s\n", mysql_error(sqlConn)); 
     return 0;
  }
  
  printf("Double Table:\t%ld\t%s\n", vectorInfo->metadataID, 
	 vectorInfo->tableName);

  if( !loadIntoVectorTable(sqlConn, vectorInfo, "testData/dataDouble.txt") )
  {
    printf("ERROR Load Doubles: %s\n", mysql_error(sqlConn));
    return 0;
  }

  if( !printDoubleRDBVector(sqlConn, vectorInfo) )
      return 0;
 
  if( testSetElem )
  {
    printf("Setting Double Elements (5, 122.99) & (12, 222.001)\n");
    if( !setDoubleElement(sqlConn, vectorInfo, 5, 122.99) ||
	!setDoubleElement(sqlConn, vectorInfo, 12, 222.001))
    {
       printf("ERROR Setting Doubles: %s\n", mysql_error(sqlConn));
       return 0;
    }

    if( !printDoubleRDBVector(sqlConn, vectorInfo) )
      return 0;

    rdbVector * resultVector;
    initRDBVector(&resultVector, 0, 1);
    if( !checkRDBVectorForNA(sqlConn, vectorInfo, resultVector) )
    {
       printf("ERROR Checking for NA: %s\n", mysql_error(sqlConn));
       return 0;
    }

    printf("Checking for NA in %s\n", vectorInfo->tableName);
    if( !printLogicRDBVector(sqlConn, resultVector) )
      return 0;
  }

  if( testSetRange )
  {
    int gt = 4;
    int lt = 8;
    double values[3] = {11.1, 22.2, 33.3};
    printf("Setting Double Elements in range (%d, %d)\n", gt, lt);

    if( !setRangeDoubleElements(sqlConn, vectorInfo, gt, lt, values, 3) )
    {
       printf("ERROR Setting Range Doubles: %s\n", mysql_error(sqlConn));
       return 0;
    }

    if( !printDoubleRDBVector(sqlConn, vectorInfo) )
      return 0;
  }

  if( testSetSparse )
  {
    unsigned int indexes[5] = {8, 3, 15, 12, 2};
    double values[3] = {100, 200, 300};
    printf("Setting Double Elements with indexes (8, 3, 15, 12, 2)\n");

    if( !setSparseDoubleElements(sqlConn, vectorInfo, indexes, 5, values, 3) )
    {
       printf("ERROR Setting Range Doubles: %s\n", mysql_error(sqlConn));
       return 0;
    }

    if( !printDoubleRDBVector(sqlConn, vectorInfo) )
      return 0;
  }

  if( testSetWDBV )
  {
    rdbVector * indexVector;
    initRDBVector(&indexVector, 0, 1);
    if( !createNewDoubleVectorTable(sqlConn, indexVector) )
    {
      printf("ERROR Create Index Table: %s\n", mysql_error(sqlConn)); 
      return 0;
    }

    int isize = 5;
    double iarray[5] = {6.0, 3.2, 18.9, 1.532, 2.67};
    if( !insertIntoDoubleVectorTable(sqlConn, indexVector, iarray, isize) ||
	!setDoubleElement(sqlConn, indexVector, 8, 8.88) )
    {
      printf("ERROR Insert Index Table: %s\n", mysql_error(sqlConn));
      return 0;
    }

    double values[2] = {111111.111, 22222.222};
    printf("Setting DOuble Elements with indexes from DBVector\n");
    if( !setDoubleElementsWDBVector(sqlConn, vectorInfo,indexVector, values,2))
    {
       printf("ERROR Setting Doubles w/ RDBV: %s\n", mysql_error(sqlConn));
       return 0;
    }

    if( !printDoubleRDBVector(sqlConn, indexVector) )
      return 0;

    if( !printDoubleRDBVector(sqlConn, vectorInfo) )
      return 0;
  }

  if( testSetWLogic )
  {
    rdbVector * logicVector;
    initRDBVector(&logicVector, 0, 1);
    if( !createNewLogicVectorTable(sqlConn, logicVector) )
    {
      printf("ERROR Create Index Table: %s\n", mysql_error(sqlConn)); 
      return 0;
    }

    int isize = 3;
    int iarray[3] = {1, 0, 0};
    if( !insertIntoLogicVectorTable(sqlConn, logicVector, iarray, isize) )
    {
      printf("ERROR Insert Logic Table: %s\n", mysql_error(sqlConn));
      return 0;
    }

    double values[2] = {0.00001, 0.00002};
    printf("Setting Double Elements with indexes from Logic\n");
    if( !setDoubleElementsWithLogic(sqlConn, vectorInfo, logicVector, values,2))
    {
       printf("ERROR Setting Double w/ Logic: %s\n", mysql_error(sqlConn));
       return 0;
    }

    if( !printLogicRDBVector(sqlConn, logicVector) )
      return 0;

    if( !printDoubleRDBVector(sqlConn, vectorInfo) )
      return 0;
  }


  printf("----------------------------------------\n");

  return 1;
}


/* ------------------------ Test String Setters ----------------------- */
int TestStringSetters(MYSQL* sqlConn, int testSetElem, int testSetRange, 
		      int testSetSparse)
{
  rdbVector * vectorInfo;
  initRDBVector(&vectorInfo, 0, 1);
  if( !createNewStringVectorTable(sqlConn, vectorInfo) )
  {
     printf("ERROR Create String Table: %s\n", mysql_error(sqlConn)); 
     return 0;
  }
  
  printf("String Table:\t%ld\t%s\n", vectorInfo->metadataID, 
	 vectorInfo->tableName);

  if( !loadIntoVectorTable(sqlConn, vectorInfo, "testData/dataStrings.txt") )
  {
    printf("ERROR Load Strings: %s\n", mysql_error(sqlConn));
    return 0;
  }

  if( !printStringRDBVector(sqlConn, vectorInfo) )
      return 0;
 
  if( testSetElem )
  {
    printf("Setting String Elements (2, 'into2') & (9, 'into6!')\n");
    if( !setStringElement(sqlConn, vectorInfo, 2, "into2") ||
	!setStringElement(sqlConn, vectorInfo, 9, "into6!"))
    {
       printf("ERROR Setting Strings: %s\n", mysql_error(sqlConn));
       return 0;
    }

    if( !printStringRDBVector(sqlConn, vectorInfo) )
      return 0;

    rdbVector * resultVector;
    initRDBVector(&resultVector, 0, 1);
    if( !checkRDBVectorForNA(sqlConn, vectorInfo, resultVector) )
    {
       printf("ERROR Checking for NA: %s\n", mysql_error(sqlConn));
       return 0;
    }

    printf("Checking for NA in %s\n", vectorInfo->tableName);
    if( !printLogicRDBVector(sqlConn, resultVector) )
      return 0;
  }

  if( testSetRange )
  {
    int gt = 3;
    int lt = 4;
    char* values[3] = {"AAA", "BBBB", "CCCCC"};
    printf("Setting String Elements in range (%d, %d)\n", gt, lt);

    if( !setRangeStringElements(sqlConn, vectorInfo, gt, lt, values, 3) )
    {
       printf("ERROR Setting Range Strings: %s\n", mysql_error(sqlConn));
       return 0;
    }

    if( !printStringRDBVector(sqlConn, vectorInfo) )
      return 0;
  }

  if( testSetSparse )
  {
    unsigned int indexes[5] = {8, 3, 12, 10, 2};
    char* values[3] = {"KKK", "LL", "M"};
    printf("Setting String Elements with indexes (8, 3, 12, 10, 2)\n");

    if( !setSparseStringElements(sqlConn, vectorInfo, indexes, 5, values, 3) )
    {
       printf("ERROR Setting Range Strings: %s\n", mysql_error(sqlConn));
       return 0;
    }

    if( !printStringRDBVector(sqlConn, vectorInfo) )
      return 0;
  }

  printf("----------------------------------------\n");

  return 1;
}


/* ------------------------ Test Complex Setters ----------------------- */
int TestComplexSetters(MYSQL* sqlConn, int testSetElem, int testSetRange, 
		       int testSetSparse)
{
  rdbVector * vectorInfo;
  initRDBVector(&vectorInfo, 0, 1);
  if( !createNewComplexVectorTable(sqlConn, vectorInfo) )
  {
     printf("ERROR Create Complex Table: %s\n", mysql_error(sqlConn)); 
     return 0;
  }
  
  printf("Complex Table:\t%ld\t%s\n", vectorInfo->metadataID, 
	 vectorInfo->tableName);

  if( !loadIntoComplexVectorTable(sqlConn, vectorInfo,
                                  "testData/dataComplex.txt") )
  {
    printf("ERROR Load Complex: %s\n", mysql_error(sqlConn));
    return 0;
  }

  if( !printComplexRDBVector(sqlConn, vectorInfo) )
      return 0;
 
  if( testSetElem )
  {
    printf("Setting Complex Elements (1, 12.12, 44.5) & (10, 1.234, 9.876)\n");
    Rcomplex c1 = {12.12, 44.5};
    Rcomplex c2 = {1.234, 9.876};
    if( !setComplexElement(sqlConn, vectorInfo, 1, c1) ||
	!setComplexElement(sqlConn, vectorInfo, 10, c2))
    {
       printf("ERROR Setting Complex: %s\n", mysql_error(sqlConn));
       return 0;
    }

    if( !printComplexRDBVector(sqlConn, vectorInfo) )
      return 0;

    rdbVector * resultVector;
    initRDBVector(&resultVector, 0, 1);
    if( !checkRDBVectorForNA(sqlConn, vectorInfo, resultVector) )
    {
       printf("ERROR Checking for NA: %s\n", mysql_error(sqlConn));
       return 0;
    }

    printf("Checking for NA in %s\n", vectorInfo->tableName);
    if( !printLogicRDBVector(sqlConn, resultVector) )
      return 0;
  }


  if( testSetRange )
  {
    int gt = 5;
    int lt = 7;
    Rcomplex values[3] = {{1, 2}, {3, 4}, {5, 6}};
    printf("Setting Complex Elements in range (%d, %d)\n", gt, lt);

    if( !setRangeComplexElements(sqlConn, vectorInfo, gt, lt, values, 3) )
    {
       printf("ERROR Setting Range Complex: %s\n", mysql_error(sqlConn));
       return 0;
    }

    if( !printComplexRDBVector(sqlConn, vectorInfo) )
      return 0;
  }

  if( testSetSparse )
  {
    unsigned int indexes[5] = {8, 3, 15, 12, 2};
    Rcomplex values[3] = {{10, 20}, {30, 40}, {50, 60}};
    printf("Setting Complex Elements with indexes (8, 3, 15, 12, 2)\n");

    if( !setSparseComplexElements(sqlConn, vectorInfo, indexes, 5, values, 3) )
    {
       printf("ERROR Setting Range Complex: %s\n", mysql_error(sqlConn));
       return 0;
    }

    if( !printComplexRDBVector(sqlConn, vectorInfo) )
      return 0;
  }

  printf("----------------------------------------\n");

  return 1;
}


/* ------------------------ Test Logic Setters ----------------------- */
int TestLogicSetters(MYSQL* sqlConn, int testSetElem, int testSetRange, 
		     int testSetSparse)
{
  rdbVector * vectorInfo;
  initRDBVector(&vectorInfo, 0, 1);
  if( !createNewLogicVectorTable(sqlConn, vectorInfo) )
  {
     printf("ERROR Create Logic Table: %s\n", mysql_error(sqlConn)); 
     return 0;
  }
  
  printf("Logic Table:\t%ld\t%s\n", vectorInfo->metadataID, 
	 vectorInfo->tableName);

  if( !loadIntoVectorTable(sqlConn, vectorInfo, "testData/dataLogic.txt") )
  {
    printf("ERROR Load Logic: %s\n", mysql_error(sqlConn));
    return 0;
  }

  if( !printLogicRDBVector(sqlConn, vectorInfo) )
      return 0;
 
  if( testSetElem )
  {
    printf("Setting Logic Elements (3, TRUE) & (5, FALSE)\n");
    if( !setLogicElement(sqlConn, vectorInfo, 3, 1) ||
	!setLogicElement(sqlConn, vectorInfo, 5, 0))
    {
       printf("ERROR Setting Logic: %s\n", mysql_error(sqlConn));
       return 0;
    }

    if( !printLogicRDBVector(sqlConn, vectorInfo) )
      return 0;

    rdbVector * resultVector;
    initRDBVector(&resultVector, 0, 1);
    if( !checkRDBVectorForNA(sqlConn, vectorInfo, resultVector) )
    {
       printf("ERROR Checking for NA: %s\n", mysql_error(sqlConn));
       return 0;
    }

    printf("Checking for NA in %s\n", vectorInfo->tableName);
    if( !printLogicRDBVector(sqlConn, resultVector) )
      return 0;
  }

  if( testSetRange )
  {
    int gt = 5;
    int lt = 8;
    int values[3] = {1, 0, 1};
    printf("Setting Logic Elements in range (%d, %d) - {1,0,1}\n", gt, lt);

    if( !setRangeLogicElements(sqlConn, vectorInfo, gt, lt, values, 3) )
    {
       printf("ERROR Setting Range Logic: %s\n", mysql_error(sqlConn));
       return 0;
    }

    if( !printLogicRDBVector(sqlConn, vectorInfo) )
      return 0;
  }

  if( testSetSparse )
  {
    unsigned int indexes[5] = {8, 3, 12, 4, 6};
    int values[3] = {1, 0, 1};
    printf("Setting Logic Elements with indexes (8, 3, 12, 4, 6) - {1,0,1}\n");

    if( !setSparseLogicElements(sqlConn, vectorInfo, indexes, 5, values, 3) )
    {
       printf("ERROR Setting Range Logic: %s\n", mysql_error(sqlConn));
       return 0;
    }

    if( !printLogicRDBVector(sqlConn, vectorInfo) )
      return 0;
  }

  printf("----------------------------------------\n");

  return 1;
}


/* -------------------- Test Materialization -------------------------- */
int TestMaterialization(MYSQL* sqlConn, int testMat1, int testMat2,
			int testMat3, int testMat4)
{
  /* Create the tables */
  rdbVector *input1, *input2, *inputComplex, *inputDoubles;
  initRDBVector(&input1, 0, 1);
  initRDBVector(&input2, 0, 1);
  initRDBVector(&inputComplex, 0, 1);
  initRDBVector(&inputDoubles, 0, 1);

  if( !createNewIntVectorTable(sqlConn, input1) ||
      !createNewIntVectorTable(sqlConn, input2) ||
      !createNewComplexVectorTable(sqlConn, inputComplex) ||
      !createNewDoubleVectorTable(sqlConn, inputDoubles) )
  {
     printf("ERROR Create Int Table: %s\n", mysql_error(sqlConn)); 
     return 0;
  }

  /* Insert some test values */
  int size1 = 5;
  int size2 = 8;
  int array1[5] = {2, -3, 7, 9, 14};
  int array2[8] = {4, 6, 5, -8, 10, -11, 15, 20};
  if( !insertIntoIntVectorTable(sqlConn, input1, array1, size1) ||
      !insertIntoIntVectorTable(sqlConn, input2, array2, size2) ||
      !loadIntoComplexVectorTable(sqlConn, inputComplex,
                                  "testData/dataComplex.txt")||
      !loadIntoVectorTable(sqlConn, inputDoubles, "testData/dataDouble.txt") )
  {
    printf("ERROR Insert data in tables: %s\n", mysql_error(sqlConn));
    return 0;
  }

  /* Testing materialization from modifying a referenced table */
  if( testMat1 )
  {
     printf("Testing Materialization of references table\n");  
     rdbVector * result1;
     initRDBVector(&result1, 1, 1);
     addNumericVectors(sqlConn, result1, input1, input2);

     printIntegerRDBVector(sqlConn, input1);
     printIntegerRDBVector(sqlConn, input2);
     printIntegerRDBVector(sqlConn, result1);

     printf("Modify input1: %s\n", input1->tableName);
     setIntElement(sqlConn, input1, 7, 777);

     printIntegerRDBVector(sqlConn, input1);
     printIntegerRDBVector(sqlConn, input2);
     printIntegerRDBVector(sqlConn, result1);

     printf("----------------------------------------\n");
     clearRDBVector(&result1);
  }

  /* Testing materialization from modifying a view */  
  if( testMat2 )
  {
     printf("Testing Materialization of view\n");
     rdbVector * result2;
     initRDBVector(&result2, 1, 1);
     subtractNumericVectors(sqlConn, result2, input1, input2);

     printIntegerRDBVector(sqlConn, input1);
     printIntegerRDBVector(sqlConn, input2);
     printIntegerRDBVector(sqlConn, result2);

     printf("Modify result: %s\n", result2->tableName);
     int gt = 7;
     int lt = 10;
     int values[3] = {11, 22, 33};
     setRangeIntElements(sqlConn, result2, gt, lt, values, 3);

     printIntegerRDBVector(sqlConn, input1);
     printIntegerRDBVector(sqlConn, input2);
     printIntegerRDBVector(sqlConn, result2);

     printf("----------------------------------------\n");
     clearRDBVector(&result2);
  }

  if( testMat3 )
  {
     printf("Testing Materialization of modifying double in complex op\n");
     rdbVector * result3;
     initRDBVector(&result3, 1, 1);

     multiplyComplexVectors(sqlConn, result3, inputComplex, inputDoubles);

     printComplexRDBVector(sqlConn, inputComplex);
     printDoubleRDBVector(sqlConn, inputDoubles);
     printComplexRDBVector(sqlConn, result3);

     printf("Modify inputDoubles: %s\n", inputDoubles->tableName);
     setDoubleElement(sqlConn, inputDoubles, 4, 44.444);

     printComplexRDBVector(sqlConn, inputComplex);
     printDoubleRDBVector(sqlConn, inputDoubles);
     printComplexRDBVector(sqlConn, result3);
    
     printf("----------------------------------------\n");
     clearRDBVector(&result3);
  }

  if( testMat4 )
  {
     printf("Testing Materialization of complex view\n");
     rdbVector * result4;
     initRDBVector(&result4, 1, 1);

     multiplyComplexVectors(sqlConn, result4, inputComplex, inputDoubles);

     printComplexRDBVector(sqlConn, inputComplex);
     printDoubleRDBVector(sqlConn, inputDoubles);
     printComplexRDBVector(sqlConn, result4);

     printf("Modify complex view: %s\n", result4->tableName);
     Rcomplex cValue;
     cValue.r = 55.55;
     cValue.i = 5.555;
     setComplexElement(sqlConn, result4, 5, cValue);

     printComplexRDBVector(sqlConn, inputComplex);
     printDoubleRDBVector(sqlConn, inputDoubles);
     printComplexRDBVector(sqlConn, result4);
    
     printf("----------------------------------------\n");
     clearRDBVector(&result4);
  }
  
  clearRDBVector(&input1);
  clearRDBVector(&input2);
  clearRDBVector(&inputComplex);
  clearRDBVector(&inputDoubles);

  printf("----------------------------------------\n");

  return 1;
}


/* -------------------- Test Duplicate Tables -------------------------- */
int TestDuplicateTable(MYSQL* sqlConn, int testFromTable, int testFromView)
{
  /* Create the tables */
  rdbVector *inputInt, *inputDouble, *inputComplex;
  initRDBVector(&inputInt, 0, 1);
  initRDBVector(&inputDouble, 0, 1);
  initRDBVector(&inputComplex, 0, 1);

  if( !createNewIntVectorTable(sqlConn, inputInt) ||
      !createNewDoubleVectorTable(sqlConn, inputDouble) ||
      !createNewComplexVectorTable(sqlConn, inputComplex) )
  {
     printf("ERROR Create Tables: %s\n", mysql_error(sqlConn)); 
     return 0;
  }

  /* Insert some test values */
  int size1 = 5;
  int array1[5] = {2, -3, 7, 9, 14};
  if( !insertIntoIntVectorTable(sqlConn, inputInt, array1, size1) ||
      !loadIntoVectorTable(sqlConn, inputDouble, "testData/dataDouble.txt") ||
      !loadIntoComplexVectorTable(sqlConn, inputComplex,
                                  "testData/dataComplex.txt"))
  {
    printf("ERROR Insert data in tables: %s\n", mysql_error(sqlConn));
    return 0;
  }

  /* Testing Dublication of table */
  if( testFromTable )
  {
     printf("Testing Dublication of double table\n");  
     rdbVector * copyVector;
     initRDBVector(&copyVector, 0, 1);
     duplicateVectorTable(sqlConn, inputDouble, copyVector);

     printDoubleRDBVector(sqlConn, inputDouble);
     printDoubleRDBVector(sqlConn, copyVector);

     clearRDBVector(&copyVector);
  }

  if( testFromView )
  {
     printf("Testing Dublication of view\n");  
     rdbVector * copyVector;
     rdbVector * resultVector;
     initRDBVector(&copyVector, 0, 1);
     initRDBVector(&resultVector, 0, 1);

     addComplexVectors(sqlConn, resultVector, inputInt, inputComplex);
     duplicateVectorTable(sqlConn, resultVector, copyVector);

     printComplexRDBVector(sqlConn, resultVector);
     printComplexRDBVector(sqlConn, copyVector);

     clearRDBVector(&copyVector);
     clearRDBVector(&resultVector);
  }


  clearRDBVector(&inputInt);
  clearRDBVector(&inputDouble);
  clearRDBVector(&inputComplex);

  printf("----------------------------------------\n");

  return 1;

}


/* ----------------------- Test Comparisons ----------------------------- */
int TestComparisons(MYSQL* sqlConn, int testCompInts, int testCompDoubles, 
		    int testCompStrings, int testCompDBVectors)
{
  if( testCompInts )
  {
     /* Create the tables */
     rdbVector *dataVector, *resultVector;
     initRDBVector(&dataVector, 0, 1);
     initRDBVector(&resultVector, 1, 1);

     if( !createNewIntVectorTable(sqlConn, dataVector) )
     {
       printf("ERROR Create Table: %s\n", mysql_error(sqlConn)); 
       return 0;
     }

     /* Insert test values */
     int size1 = 5;
     int array1[5] = {10, 20, 30, 8, 18};
     if( !insertIntoIntVectorTable(sqlConn, dataVector, array1, size1) ||
	 !setIntElement(sqlConn, dataVector, 9, 6) )
     {
       printf("ERROR Insert data in table: %s\n", mysql_error(sqlConn));
       return 0;
     }

     /* Perform the comparison */
     int sizeComp = 3;
     int arrayComp[3] = {12, 16, 10};
     if( !compareWithIntegerValues(sqlConn, resultVector, dataVector,
				   arrayComp, sizeComp, ">") )
     {
        printf("ERROR comparing data: %s\n", mysql_error(sqlConn));
        return 0;
     }

     printf("Testing Comparison of Integer Values (>)\n");
     printIntegerRDBVector(sqlConn, dataVector);
     printf("Comparison: > {12, 16, 10}\n");
     printLogicRDBVector(sqlConn, resultVector);
  }

  if( testCompDoubles )
  {
     /* Create the tables */
     rdbVector *dataVector, *resultVector;
     initRDBVector(&dataVector, 0, 1);
     initRDBVector(&resultVector, 1, 1);

     if( !createNewDoubleVectorTable(sqlConn, dataVector) )
     {
       printf("ERROR Create Table: %s\n", mysql_error(sqlConn)); 
       return 0;
     }

     /* Insert test values */
     int size1 = 5;
     double array1[5] = {10.0, 20.0, 30.0, 8.0, 18.0};
     if( !insertIntoDoubleVectorTable(sqlConn, dataVector, array1, size1) ||
	 !setDoubleElement(sqlConn, dataVector, 9, 6.0) )
     {
       printf("ERROR Insert data in table: %s\n", mysql_error(sqlConn));
       return 0;
     }

     /* Perform the comparison */
     int sizeComp = 3;
     double arrayComp[3] = {12.0, 16.0, 10.0};
     if( !compareWithDoubleValues(sqlConn, resultVector, dataVector,
				   arrayComp, sizeComp, "<=") )
     {
        printf("ERROR comparing data: %s\n", mysql_error(sqlConn));
        return 0;
     }

     printf("Testing Comparison of Double Values (<=)\n");
     printDoubleRDBVector(sqlConn, dataVector);
     printf("Comparison: <= {12.0, 16.0, 10.0}\n");
     printLogicRDBVector(sqlConn, resultVector);
  }

  if( testCompStrings )
  {
     /* Create the tables */
     rdbVector *dataVector, *resultVector;
     initRDBVector(&dataVector, 0, 1);
     initRDBVector(&resultVector, 1, 1);

     if( !createNewStringVectorTable(sqlConn, dataVector) )
     {
       printf("ERROR Create Table: %s\n", mysql_error(sqlConn)); 
       return 0;
     }

     /* Insert test values */
     if( !loadIntoVectorTable(sqlConn, dataVector, "testData/dataStrings.txt") )
     {
       printf("ERROR Load data in table: %s\n", mysql_error(sqlConn));
       return 0;
     }

     /* Perform the comparison */
     int sizeComp = 3;
     char* arrayComp[3] = {"Yeap123", "zzz", "{}[]()"};
     if( !compareWithStringValues(sqlConn, resultVector, dataVector,
				  arrayComp, sizeComp, "=") )
     {
        printf("ERROR comparing data: %s\n", mysql_error(sqlConn));
        return 0;
     }

     printf("Testing Comparison of String Values (=)\n");
     printStringRDBVector(sqlConn, dataVector);
     printf("Comparison: = {\"Yeap123\", \"zzz\", \"{}[]()\"};\n");
     printLogicRDBVector(sqlConn, resultVector);
  }

  if( testCompDBVectors )
  {
     /* Create the tables */
     rdbVector *dataVector, *compareVector, *resultVector;
     initRDBVector(&dataVector, 0, 1);
     initRDBVector(&compareVector, 0, 1);
     initRDBVector(&resultVector, 1, 1);

     if( !createNewIntVectorTable(sqlConn, dataVector) ||
	 !createNewIntVectorTable(sqlConn, compareVector) )
     {
       printf("ERROR Create Table: %s\n", mysql_error(sqlConn)); 
       return 0;
     }

     /* Insert test values */
     int size1 = 5;
     int array1[5] = {10, 20, 30, 8, 18};
     if( !insertIntoIntVectorTable(sqlConn, dataVector, array1, size1) ||
	 !setIntElement(sqlConn, dataVector, 9, 6) )
     {
       printf("ERROR Insert data in table: %s\n", mysql_error(sqlConn));
       return 0;
     }

     /* Perform the comparison */
     int sizeComp = 3;
     int arrayComp[3] = {8, 20, 10};
     if( !insertIntoIntVectorTable(sqlConn, compareVector, arrayComp, sizeComp))
     {
       printf("ERROR Insert data in table: %s\n", mysql_error(sqlConn));
       return 0;
     }


     if( !compareWithDBVector(sqlConn, resultVector, dataVector,
			      compareVector, "!=") )
     {
        printf("ERROR comparing data: %s\n", mysql_error(sqlConn));
        return 0;
     }

     printf("Testing Comparison with DBVector (!=)\n");
     printIntegerRDBVector(sqlConn, dataVector);
     printIntegerRDBVector(sqlConn, compareVector);
     printLogicRDBVector(sqlConn, resultVector);
  }

  return 1;
}


/* ----------------------- Test Functions ----------------------------- */
int TestFunctions(MYSQL* sqlConn, int testFuncInts, int testFuncDoubles, 
		   int testFuncComplex, int testFuncStrings)
{
  if( testFuncInts )
  {
     /* Create the table */
     rdbVector *dataVector;
     initRDBVector(&dataVector, 0, 1);

     if( !createNewIntVectorTable(sqlConn, dataVector) ||
	 !loadIntoVectorTable(sqlConn, dataVector, "testData/dataInt.txt") )
     {
       printf("ERROR Create/Load into Table: %s\n", mysql_error(sqlConn)); 
       return 0;
     }

     /* Perform the tests */
     int sum = 0;
     if( !getIntegerVectorSum(sqlConn, dataVector, &sum) )
     {
        printf("ERROR getting int sum: %s\n", mysql_error(sqlConn));
        return 0;
     }

     double avg = 0;
     if( !getIntegerVectorAvg(sqlConn, dataVector, &avg) )
     {
        printf("ERROR getting int avg: %s\n", mysql_error(sqlConn));
        return 0;
     }

     int max = 0;
     if( !getIntegerVectorMax(sqlConn, dataVector, &max) )
     {
        printf("ERROR getting int max: %s\n", mysql_error(sqlConn));
        return 0;
     }

     int min = 0;
     if( !getIntegerVectorMin(sqlConn, dataVector, &min) )
     {
        printf("ERROR getting int min: %s\n", mysql_error(sqlConn));
        return 0;
     }

     printf("Testing Functions for Integer Table %s\n", dataVector->tableName);
     printIntegerRDBVector(sqlConn, dataVector);
     printf("Functions:\n\tSUM: %d\n", sum);
     printf("\tAVG: %lf\n", avg);
     printf("\tMAX: %d\n", max);
     printf("\tMIN: %d\n", min);

     /* Test sin/cos */
     rdbVector *sinVector, *cosVector;
     initRDBVector(&sinVector, 0, 1);
     initRDBVector(&cosVector, 0, 1);

     if( !performNumericSin(sqlConn, dataVector, sinVector) ||
	 !performNumericCos(sqlConn, dataVector, cosVector) )
     {
        printf("ERROR performing sin/cos on Int: %s\n", mysql_error(sqlConn));
        return 0;
     }

     printf("Testing Sin/Cos for Integer Table %s\n", dataVector->tableName);
     printDoubleRDBVector(sqlConn, sinVector);
     printDoubleRDBVector(sqlConn, cosVector);

     clearRDBVector(&sinVector);
     clearRDBVector(&cosVector);
     clearRDBVector(&dataVector);
  }

  if( testFuncDoubles )
  {
     /* Create the table */
     rdbVector *dataVector;
     initRDBVector(&dataVector, 0, 1);

     if( !createNewDoubleVectorTable(sqlConn, dataVector) ||
	 !loadIntoVectorTable(sqlConn, dataVector, "testData/dataDouble.txt") )
     {
       printf("ERROR Create/Load into Table: %s\n", mysql_error(sqlConn)); 
       return 0;
     }

     /* Perform the tests */
     double sum = 0;
     if( !getDoubleVectorSum(sqlConn, dataVector, &sum) )
     {
        printf("ERROR getting double sum: %s\n", mysql_error(sqlConn));
        return 0;
     }

     double avg = 0;
     if( !getDoubleVectorAvg(sqlConn, dataVector, &avg) )
     {
        printf("ERROR getting double avg: %s\n", mysql_error(sqlConn));
        return 0;
     }

     double max = 0;
     if( !getDoubleVectorMax(sqlConn, dataVector, &max) )
     {
        printf("ERROR getting double max: %s\n", mysql_error(sqlConn));
        return 0;
     }

     double min = 0;
     if( !getDoubleVectorMin(sqlConn, dataVector, &min) )
     {
        printf("ERROR getting double min: %s\n", mysql_error(sqlConn));
        return 0;
     }

     printf("Testing Functions for Double Table %s\n", dataVector->tableName);
     printDoubleRDBVector(sqlConn, dataVector);
     printf("Functions:\n\tSUM: %lf\n", sum);
     printf("\tAVG: %lf\n", avg);
     printf("\tMAX: %lf\n", max);
     printf("\tMIN: %lf\n", min);


     /* Test sin/cos */
     rdbVector *sinVector, *cosVector;
     initRDBVector(&sinVector, 0, 1);
     initRDBVector(&cosVector, 0, 1);

     if( !performNumericSin(sqlConn, dataVector, sinVector) ||
	 !performNumericCos(sqlConn, dataVector, cosVector) )
     {
        printf("ERROR performing sin/cos on Double: %s\n",
               mysql_error(sqlConn));
        return 0;
     }

     printf("Testing Sin/Cos for Double Table %s\n", dataVector->tableName);
     printDoubleRDBVector(sqlConn, sinVector);
     printDoubleRDBVector(sqlConn, cosVector);

     clearRDBVector(&sinVector);
     clearRDBVector(&cosVector);
     clearRDBVector(&dataVector);
  }

  if( testFuncStrings )
  {
     /* Create the table */
     rdbVector *dataVector;
     initRDBVector(&dataVector, 0, 1);

     if( !createNewStringVectorTable(sqlConn, dataVector) ||
	 !loadIntoVectorTable(sqlConn, dataVector, "testData/dataStrings.txt") )
     {
       printf("ERROR Create/Load into Table: %s\n", mysql_error(sqlConn)); 
       return 0;
     }

     /* Perform the tests */
     char * max;
     if( !getStringVectorMax(sqlConn, dataVector, &max) )
     {
        printf("ERROR getting string max: %s\n", mysql_error(sqlConn));
        return 0;
     }

     char * min;
     if( !getStringVectorMin(sqlConn, dataVector, &min) )
     {
        printf("ERROR getting string min: %s\n", mysql_error(sqlConn));
        return 0;
     }

     printf("Testing Functions for String Table %s\n", dataVector->tableName);
     printStringRDBVector(sqlConn, dataVector);
     printf("Functions:\n\tMAX: %s\n", max);
     printf("\tMIN: %s\n", min);

     clearRDBVector(&dataVector);
  }

  if( testFuncComplex )
  {
     /* Create the table */
     rdbVector *dataVector;
     initRDBVector(&dataVector, 0, 1);

     if( !createNewComplexVectorTable(sqlConn, dataVector) ||
	 !loadIntoComplexVectorTable(sqlConn, dataVector,
				     "testData/dataComplex.txt") )
     {
       printf("ERROR Create/Load into Table: %s\n", mysql_error(sqlConn)); 
       return 0;
     }

     /* Perform the tests */
     Rcomplex sum;
     if( !getComplexVectorSum(sqlConn, dataVector, &sum) )
     {
        printf("ERROR getting complex sum: %s\n", mysql_error(sqlConn));
        return 0;
     }

     Rcomplex avg;
     if( !getComplexVectorAvg(sqlConn, dataVector, &avg) )
     {
        printf("ERROR getting complex avg: %s\n", mysql_error(sqlConn));
        return 0;
     }

     printf("Testing Functions for Complex Table %s\n", dataVector->tableName);
     printComplexRDBVector(sqlConn, dataVector);
     printf("Functions:\n\tSUM: %lf\t %lf\n", sum.r, sum.i);
     printf("\tAVG: %lf\t %lf\n", avg.r, avg.i);

     /* Test sin/cos */
     rdbVector *sinVector, *cosVector;
     initRDBVector(&sinVector, 0, 1);
     initRDBVector(&cosVector, 0, 1);

     if( !performComplexSin(sqlConn, dataVector, sinVector) ||
	 !performComplexCos(sqlConn, dataVector, cosVector) )
     {
        printf("ERROR performing sin/cos on Int: %s\n", mysql_error(sqlConn));
        return 0;
     }

     printf("Testing Sin/Cos for Complex Table %s\n", dataVector->tableName);
     printComplexRDBVector(sqlConn, sinVector);
     printComplexRDBVector(sqlConn, cosVector);

     clearRDBVector(&sinVector);
     clearRDBVector(&cosVector);
     clearRDBVector(&dataVector);
  }

  return 1;
}

int TestConvertions(MYSQL* sqlConn)
{
  /* Create the tables */
  rdbVector *inputInt, *inputDouble, *inputLogic;
  initRDBVector(&inputInt, 0, 1);
  initRDBVector(&inputDouble, 0, 1);
  initRDBVector(&inputLogic, 0, 1);

  if( !createNewIntVectorTable(sqlConn, inputInt) ||
      !createNewDoubleVectorTable(sqlConn, inputDouble) ||
      !createNewLogicVectorTable(sqlConn, inputLogic) )
  {
     printf("ERROR Create Tables: %s\n", mysql_error(sqlConn)); 
     return 0;
  }

  /* Insert some test values */
  if( !loadIntoVectorTable(sqlConn, inputInt, "testData/dataInt.txt") ||
      !loadIntoVectorTable(sqlConn, inputDouble, "testData/dataDouble.txt") ||
      !loadIntoVectorTable(sqlConn, inputLogic, "testData/dataLogic.txt"))
  {
    printf("ERROR Load data in tables: %s\n", mysql_error(sqlConn));
    return 0;
  }

  /* Create the output tables */
  rdbVector *outInt1, *outInt2, *outDouble1, *outDouble2;
  initRDBVector(&outInt1, 1, 1);
  initRDBVector(&outInt2, 1, 1);
  initRDBVector(&outDouble1, 1, 1);
  initRDBVector(&outDouble2, 1, 1);

  if(!convertDoubleToInteger(sqlConn, inputDouble, outInt1) ||
     !convertLogicToInteger(sqlConn, inputLogic, outInt2) ||
     !convertIntegerToDouble(sqlConn, inputInt, outDouble1) ||
     !convertLogicToDouble(sqlConn, inputLogic, outDouble2) )
  {
    printf("ERROR converting tables: %s\n", mysql_error(sqlConn));
    return 0;
  }

  printIntegerRDBVector(sqlConn, inputInt);
  printDoubleRDBVector (sqlConn, outDouble1);

  printDoubleRDBVector (sqlConn, inputDouble);
  printIntegerRDBVector(sqlConn, outInt1);

  printLogicRDBVector  (sqlConn, inputLogic);
  printIntegerRDBVector(sqlConn, outInt2);
  printDoubleRDBVector(sqlConn, outDouble2);

  clearRDBVector(&inputInt);
  clearRDBVector(&inputDouble);
  clearRDBVector(&inputLogic);
  clearRDBVector(&outInt1);
  clearRDBVector(&outInt2);
  clearRDBVector(&outDouble1);
  clearRDBVector(&outDouble2);

  return 1;
}


/************************************************************************/

int printIntegerRDBVector(MYSQL* sqlConn, rdbVector * vectorInfo)
{
    int i, size = vectorInfo->size;
    int values[size];
    char byteArray[size];
    for( i = 0 ; i < size ; i++ )
      byteArray[i] = 0;

    if( !getAllIntElements(sqlConn, vectorInfo, values, byteArray) )
    {
      printf("ERROR Getting Int values: %s\n", mysql_error(sqlConn));
      return 0;
    }

    printf("Printing All Integer: %s (ID: %ld)\n", vectorInfo->tableName,
	   vectorInfo->metadataID);
    for( i = 0 ; i < size ; i++ )
    {
      if( byteArray[i] == 1 )
	printf("\t%d\t%d\n", i+1, values[i]);
      else
	printf("\t%d\tNA\n", i+1);
    }

    return 1;
}


int printDoubleRDBVector(MYSQL* sqlConn, rdbVector * vectorInfo)
{
    int i, size = vectorInfo->size;
    double values[size];
    char byteArray[size];
    for( i = 0 ; i < size ; i++ )
      byteArray[i] = 0;

    if( !getAllDoubleElements(sqlConn, vectorInfo, values, byteArray) )
    {
      printf("ERROR Getting All Double: %s\n", mysql_error(sqlConn));
      return 0;
    }

    printf("Printing Double Vector: %s (ID: %ld)\n", vectorInfo->tableName,
	   vectorInfo->metadataID);
    for( i = 0 ; i < size ; i++ )
    {
      if( byteArray[i] == 1 )
	printf("\t%d\t%lf\n", i+1, values[i]);
      else
	printf("\t%d\tNA\n", i+1);
    }

    return 1;
}


int printStringRDBVector(MYSQL* sqlConn, rdbVector * vectorInfo)
{
    int i, size = vectorInfo->size;
    char* values[size];
    char byteArray[size];
    for( i = 0 ; i < size ; i++ )
      byteArray[i] = 0;

    if( !getAllStringElements(sqlConn, vectorInfo, values, byteArray) )
    {
      printf("ERROR Getting All Strings: %s\n", mysql_error(sqlConn));
      return 0;
    }

    printf("Printing String Vector: %s (ID: %ld)\n", vectorInfo->tableName,
	   vectorInfo->metadataID);
    for( i = 0 ; i < size ; i++ )
    {
      if( byteArray[i] == 1 )
	printf("\t%d\t%s\n", i+1, values[i]);
      else
	printf("\t%d\tNA\n", i+1);
    }

    return 1;
}

int printComplexRDBVector(MYSQL* sqlConn, rdbVector * vectorInfo)
{

    int i, size = vectorInfo->size;
    Rcomplex values[size];
    char byteArray[size];
    for( i = 0 ; i < size ; i++ )
      byteArray[i] = 0;

    if( !getAllComplexElements(sqlConn, vectorInfo, values, byteArray) )
    {
      printf("ERROR Getting All Complex: %s\n", mysql_error(sqlConn));
      return 0;
    }

    printf("Printing Complex Vector: %s (ID: %ld)\n", vectorInfo->tableName,
	   vectorInfo->metadataID);
    for( i = 0 ; i < size ; i++ )
    {
      if( byteArray[i] == 1 )
	printf("\t%d\t%lf\t%lf\n", i+1, values[i].r, values[i].i);
      else
	printf("\t%d\tNA\tNA\n", i+1);
    }

    return 1;
}


int printLogicRDBVector(MYSQL* sqlConn, rdbVector * vectorInfo)
{
    int i, size = vectorInfo->size;
    int values[size];
    char byteArray[size];
    for( i = 0 ; i < size ; i++ )
      byteArray[i] = 0;

    if( !getAllLogicElements(sqlConn, vectorInfo, values, byteArray) )
    {
      printf("ERROR Getting Logic values: %s\n", mysql_error(sqlConn));
      return 0;
    }

    printf("Printing All Logic: %s (ID: %ld)\n", vectorInfo->tableName,
	   vectorInfo->metadataID);
    for( i = 0 ; i < size ; i++ )
    {
      if( byteArray[i] == 1 )
	printf("\t%d\t%s\n", i+1, (values[i] == 0) ? "FALSE" : "TRUE");
      else
	printf("\t%d\tNA\n", i+1);
    }

    return 1;
}

