/*
** print.c
** 
** Made by (Jun Yang)
** Login   <junyang@terrier.cs.duke.edu>
** 
** Started on  Fri Feb 26 13:59:22 2010 Jun Yang
** Last update Sun May 12 01:17:25 2002 Speed Blue
*/

#include "riotdb.h"

SEXP print_dbvector(SEXP x) {
  MYSQL *sqlConn = NULL;
  if (!connectToLocalDB(&sqlConn) || sqlConn == NULL) {
    error("cannot connect to local db\n");
    return R_NilValue;
  }
  rdbVector *info = getInfo(x);
  const char *sqlTemplate = "SELECT vIndex, vValue FROM %s ORDER BY 1";
  char sql[strlen(sqlTemplate) + strlen(info->tableName) + 1];
  sprintf(sql, sqlTemplate, info->tableName);
  if (mysql_query(sqlConn, sql) != 0)
    return R_NilValue;
  MYSQL_RES *sqlRes = mysql_use_result(sqlConn);
  MYSQL_ROW sqlRow = NULL;
  while ((sqlRow = mysql_fetch_row(sqlRes)) != NULL) {
    Rprintf("[%s] %s\n", sqlRow[0], sqlRow[1]);
  }
  mysql_free_result(sqlRes);
  mysql_close(sqlConn);
  return R_NilValue;
}

SEXP print_dbmatrix(SEXP A, SEXP byrow) {
  MYSQL *sqlConn = NULL;
  if (!connectToLocalDB(&sqlConn) || sqlConn == NULL) {
    error("cannot connect to local db\n");
    return R_NilValue;
  }
  rdbMatrix *info = getMatrixInfo(A);
  const char *sqlTemplate = (asInteger(byrow))?
    "SELECT mRow, mCol, mValue FROM %s ORDER BY 1, 2" :
    "SELECT mRow, mCol, mValue FROM %s ORDER BY 2, 1";
  char sql[strlen(sqlTemplate) + strlen(info->tableName) + 1];
  sprintf(sql, sqlTemplate, info->tableName);
  if (mysql_query(sqlConn, sql) != 0)
    return R_NilValue;
  MYSQL_RES *sqlRes = mysql_use_result(sqlConn);
  MYSQL_ROW sqlRow = NULL;
  while ((sqlRow = mysql_fetch_row(sqlRes)) != NULL) {
    Rprintf("[%s,%s] %s\n", sqlRow[0], sqlRow[1], sqlRow[2]);
  }
  mysql_free_result(sqlRes);
  mysql_close(sqlConn);
  return R_NilValue;
}
