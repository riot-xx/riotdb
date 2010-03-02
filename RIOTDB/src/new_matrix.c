/**************************************************************************** 
Functions for creating dbmatrices.
Author: Yi Zhang
Date: Sep 8, 2008
****************************************************************************/ 
#include "riotdb.h"

SEXP dbmatrix_from_dbvector(SEXP from, SEXP nrows, SEXP ncols, SEXP byrow) {
  rdbVector *infoFrom = getInfo(from);
  if (infoFrom->sxp_type != SXP_TYPE_INTEGER &&
      infoFrom->sxp_type != SXP_TYPE_DOUBLE &&
      infoFrom->sxp_type != SXP_TYPE_LOGIC) {
    error("you can only convert from an integer, double, or logic dbvector");
    return R_NilValue;
  }

  MYSQL *sqlconn = NULL;
  int success = connectToLocalDB(&sqlconn);
  if (!success || sqlconn == NULL) {
    error("cannot connect to local db %s\n", mysql_error(sqlconn));
    return R_NilValue;
  }

  rdbMatrix *infoRet;
  SEXP retSEXP, infoRetSEXP, tableNameSEXP;
  PROTECT(retSEXP = R_do_new_object(R_getClassDef("dbmatrix")));
  PROTECT(infoRetSEXP = allocVector(RAWSXP, sizeof(rdbMatrix)));
  infoRet = (rdbMatrix*) RAW(infoRetSEXP);
  initRDBMatrix(infoRet);

  if (!restructureMatrixFromVector(sqlconn, infoRet, infoFrom, asInteger(nrows), asInteger(ncols), asInteger(byrow)))
      return FALSE;

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

SEXP dbmatrix_from_seq(SEXP rows, SEXP cols, SEXP begin, SEXP end, SEXP by)
{
	SEXP ret,tablename,info;
	double rbegin, rend, rby;
	int nRows, nCols;
	if ((isComplex(begin) || isReal(begin)) && (isReal(end) || isComplex(end)))
	{
		rbegin = asReal(begin);
		rend = asReal(end);
	}
	else
		error("from and to must be integer or real values");

	if (!isReal(by))
	{
		error("by must be an integer or real value");
	}
	else
		rby = asReal(by);
	
	nRows = asInteger(rows);
	nCols = asInteger(cols);

	Rboolean useInt = FALSE;
	PROTECT(ret = R_do_new_object(R_getClassDef("dbmatrix")));

	useInt = (((int)rbegin) == rbegin);
	useInt &= (rbegin >= INT_MIN && rbegin <= INT_MAX && ((int)rby)==rby && rend >= INT_MIN && rend <= INT_MAX);

	MYSQL *sqlconn = NULL;
	int success = connectToLocalDB(&sqlconn);

	if(!success || sqlconn == NULL)
	{
		error("cannot connect to local db\n");
		return R_NilValue;
	}

	PROTECT(info = allocVector(RAWSXP,sizeof(rdbMatrix)));
	rdbMatrix *vec = (rdbMatrix*)RAW(info);
        initRDBMatrix(vec);

	/*if (useInt)
	{
	}
	else */
	{
		if (!createNewDoubleMatrixTable(sqlconn, vec))
		{
			error("cannot create double table: %s\n",mysql_error(sqlconn));
			return info;
		}
		if (!insertSeqDoubleMatrixTable(sqlconn, vec, rbegin, rend, rby, nRows, nCols))
		{
			error("cannot insert into double table: %s\n", mysql_error(sqlconn));
			return info;
		}
	}	

	mysql_close(sqlconn);

	PROTECT(tablename= allocVector(STRSXP, 1));
	SET_STRING_ELT(tablename, 0, mkChar(vec->tableName));
	R_do_slot_assign(ret, install("tablename"), tablename);
	R_do_slot_assign(ret, install("info"), info);

	
	vec->sxp_spare = 0;
	/* register finalizer */
	SEXP rptr;
	rdbMatrix *ptr = malloc(sizeof(rdbMatrix));
	*ptr= *vec;
	PROTECT(rptr = R_MakeExternalPtr(ptr, R_NilValue, R_NilValue));
	R_do_slot_assign(ret, install("ext"), rptr);
	R_RegisterCFinalizerEx(rptr, rdbMatrixFinalizer, TRUE);
	
	UNPROTECT(4);
        free(vec->tableName);
	return ret;
}

