#include <stdio.h>
#include <stdlib.h>
#include <mysql.h>

#include "Rinternals.h"

#include "basics.h"
#include "handle_vector_tables.h"
#include "handle_matrix_tables.h"

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

  /* --------------------- Load all vectors --------------------------- */
  rdbVector ** arrayVectorInfoObjects;
  unsigned long int numTables;

  if( !loadAllRDBVectors(sqlConn, &arrayVectorInfoObjects, &numTables) )
  {
     printf("ERROR Load RDB Vectors: %s\n", mysql_error(sqlConn));
     return 0;
  }

  /* Delete all vectors */
  printf("\nNumber of vector tables: %ld\n", numTables);
  int i;
  for( i = 0 ; i < numTables ; i++ )
  {
     printf("\t%ld\t%s", 
	    arrayVectorInfoObjects[i]->metadataID,
	    arrayVectorInfoObjects[i]->tableName);

     deleteRDBVector(sqlConn, arrayVectorInfoObjects[i]);
     printf("\t--Deleted! %s\n", mysql_error(sqlConn));
  }

  /* Clean up */
  for( i = 0 ; i < numTables ; i++ )
  {
     clearRDBVector(&arrayVectorInfoObjects[i]);
  }

  if( numTables != 0 )
  {
     free(arrayVectorInfoObjects);
  }
  printf("----------------------------------------\n");

  /* --------------------- Load all matrices --------------------------- */
  rdbMatrix ** arrayMatrixInfoObjects;
  unsigned long int numMatrixTables;

  if( !loadAllRDBMatrices(sqlConn, &arrayMatrixInfoObjects, &numMatrixTables))
  {
     printf("ERROR Load RDB Matrices: %s\n", mysql_error(sqlConn));
     return 0;
  }

  /* Delete all matricess */
  printf("\nNumber of matrix tables: %ld\n", numMatrixTables);
  for( i = 0 ; i < numMatrixTables ; i++ )
  {
     printf("\t%ld\t%s", 
	    arrayMatrixInfoObjects[i]->metadataID,
	    arrayMatrixInfoObjects[i]->tableName);

     deleteRDBMatrix(sqlConn, arrayMatrixInfoObjects[i]);
     printf("\t--Deleted! %s\n", mysql_error(sqlConn));
  }

  for( i = 0 ; i < numMatrixTables ; i++ )
  {
     clearRDBMatrix(&arrayMatrixInfoObjects[i]);
  }

  if( numMatrixTables != 0 )
  {
     free(arrayMatrixInfoObjects);
  }
  printf("----------------------------------------\n");


  /* Close Connection */
  mysql_close(sqlConn);

  printf("Completed Execution!\n");

  return 0;

}

