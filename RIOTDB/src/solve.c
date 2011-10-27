/****************************************************************************
Functions for solving linear systems.
Author: Jun Yang
Date: Feb 24, 2010
****************************************************************************/
#include "riotdb.h"

SEXP do_mat_inv(SEXP a) {
  rdbMatrix *infoA, *infoRet;
  SEXP retSEXP, infoRetSEXP, tableNameSEXP;

  MYSQL *sqlconn = NULL;
  int success = connectToLocalDB(&sqlconn);
  if (!success || sqlconn == NULL) {
    error("cannot connect to local db %s\n", mysql_error(sqlconn));
    return R_NilValue;
  }

  infoA = getMatrixInfo(a);
  PROTECT(retSEXP = R_do_new_object(R_getClassDef("dbmatrix")));
  PROTECT(infoRetSEXP = allocVector(RAWSXP, sizeof(rdbMatrix)));
  infoRet = (rdbMatrix*) RAW(infoRetSEXP);
  initRDBMatrix(infoRet);

  if (!computeDoubleMatrixInverse(sqlconn, infoRet, infoA)) {
    error("inversion failed");
    mysql_close(sqlconn);
    UNPROTECT(2);
    return R_NilValue;
  }

  mysql_close(sqlconn);

  PROTECT(tableNameSEXP = allocVector(STRSXP, 1));
  SET_STRING_ELT(tableNameSEXP, 0, mkChar(infoRet->tableName));
  R_do_slot_assign(retSEXP, install("tablename"), tableNameSEXP);
  R_do_slot_assign(retSEXP, install("info"), infoRetSEXP);

  /* Register finalizer: */
  SEXP ptrSEXP;
  rdbMatrix *ptr = malloc(sizeof(rdbMatrix));
  *ptr = *infoRet;
  PROTECT(ptrSEXP = R_MakeExternalPtr(ptr, R_NilValue, R_NilValue));
  R_RegisterCFinalizerEx(ptrSEXP, rdbMatrixFinalizer, TRUE);
  R_do_slot_assign(retSEXP, install("ext"), ptrSEXP);

  UNPROTECT(4);
  free(infoRet->tableName);

  return retSEXP;
}

SEXP do_mat_t(SEXP a) {
  rdbMatrix *infoA, *infoRet;
  SEXP retSEXP, infoRetSEXP, tableNameSEXP;

  MYSQL *sqlconn = NULL;
  int success = connectToLocalDB(&sqlconn);
  if (!success || sqlconn == NULL) {
    error("cannot connect to local db %s\n", mysql_error(sqlconn));
    return R_NilValue;
  }

  infoA = getMatrixInfo(a);
  PROTECT(retSEXP = R_do_new_object(R_getClassDef("dbmatrix")));
  PROTECT(infoRetSEXP = allocVector(RAWSXP, sizeof(rdbMatrix)));
  infoRet = (rdbMatrix*) RAW(infoRetSEXP);
  initRDBMatrix(infoRet);

  if (!computeMatrixeTranspose(sqlconn, infoRet, infoA)) {
    error("inversion failed");
    mysql_close(sqlconn);
    UNPROTECT(2);
    return R_NilValue;
  }

  mysql_close(sqlconn);

  PROTECT(tableNameSEXP = allocVector(STRSXP, 1));
  SET_STRING_ELT(tableNameSEXP, 0, mkChar(infoRet->tableName));
  R_do_slot_assign(retSEXP, install("tablename"), tableNameSEXP);
  R_do_slot_assign(retSEXP, install("info"), infoRetSEXP);

  /* Register finalizer: */
  SEXP ptrSEXP;
  rdbMatrix *ptr = malloc(sizeof(rdbMatrix));
  *ptr = *infoRet;
  PROTECT(ptrSEXP = R_MakeExternalPtr(ptr, R_NilValue, R_NilValue));
  R_RegisterCFinalizerEx(ptrSEXP, rdbMatrixFinalizer, TRUE);
  R_do_slot_assign(retSEXP, install("ext"), ptrSEXP);

  UNPROTECT(4);
  free(infoRet->tableName);

  return retSEXP;
}

SEXP do_mat_sol(SEXP a, SEXP b) {
  rdbMatrix *infoA, *infoB, *infoRet;
  SEXP retSEXP, infoRetSEXP, tableNameSEXP;

  MYSQL *sqlconn = NULL;
  int success = connectToLocalDB(&sqlconn);
  if (!success || sqlconn == NULL) {
    error("cannot connect to local db %s\n", mysql_error(sqlconn));
    return R_NilValue;
  }

  infoA = getMatrixInfo(a);
  infoB = getMatrixInfo(b);
  PROTECT(retSEXP = R_do_new_object(R_getClassDef("dbmatrix")));
  PROTECT(infoRetSEXP = allocVector(RAWSXP, sizeof(rdbMatrix)));
  infoRet = (rdbMatrix*) RAW(infoRetSEXP);
  initRDBMatrix(infoRet);

  if (!solveByGuassianElimination(sqlconn, infoRet, infoA, infoB)) {
    error("solve failed");
    mysql_close(sqlconn);
    UNPROTECT(2);
    return R_NilValue;
  }

  mysql_close(sqlconn);

  PROTECT(tableNameSEXP = allocVector(STRSXP, 1));
  SET_STRING_ELT(tableNameSEXP, 0, mkChar(infoRet->tableName));
  R_do_slot_assign(retSEXP, install("tablename"), tableNameSEXP);
  R_do_slot_assign(retSEXP, install("info"), infoRetSEXP);

  /* Register finalizer: */
  SEXP ptrSEXP;
  rdbMatrix *ptr = malloc(sizeof(rdbMatrix));
  *ptr = *infoRet;
  PROTECT(ptrSEXP = R_MakeExternalPtr(ptr, R_NilValue, R_NilValue));
  R_RegisterCFinalizerEx(ptrSEXP, rdbMatrixFinalizer, TRUE);
  R_do_slot_assign(retSEXP, install("ext"), ptrSEXP);

  UNPROTECT(4);
  free(infoRet->tableName);

  return retSEXP;
}
