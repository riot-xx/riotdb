/****************************************************************************
Functions for loading dbvectors and dbmatrices.
Author: Yi Zhang
Date: Sep 8, 2008
****************************************************************************/
#include "riotdb.h"

SEXP load_dbvector(SEXP mid)
{
	SEXP ans, info;

	int id = INTEGER(mid)[0];

	MYSQL *sqlconn = NULL;
	int success = connectToLocalDB(&sqlconn);

	if(!success || sqlconn == NULL)
	{
		error("cannot connect to local db\n");
		return R_NilValue;
	}


	PROTECT(ans = R_do_new_object(R_getClassDef("dbvector")));
	PROTECT(info = allocVector(RAWSXP,sizeof(rdbVector)));
	rdbVector *vec = (rdbVector*)RAW(info);
        initRDBVector(vec);
	loadRDBVector(sqlconn, vec, id);
	vec->sxp_spare = 0;

	mysql_close(sqlconn);

	SEXP tablename;
	PROTECT(tablename= allocVector(STRSXP, 1));
	SET_STRING_ELT(tablename, 0, mkChar(vec->tableName));
	R_do_slot_assign(ans, install("tablename"), tablename);
	R_do_slot_assign(ans, install("info"), info);
	free(vec->tableName);
	/* do NOT register finalizer for a loaded dbvector*/
	/*rdbVector *ptr = malloc(sizeof(rdbVector));
	*ptr = *vec;
	SEXP rptr;
	PROTECT(rptr = R_MakeExternalPtr(ptr, R_NilValue, R_NilValue));
	R_do_slot_assign(ans, install("ext"), rptr);
	R_RegisterCFinalizerEx(rptr, rdbvectorFinalizer, TRUE);
	*/

	UNPROTECT(3);
	return ans;
}

SEXP load_dbmatrix(SEXP mid)
{
	SEXP ans, info;

	int id = INTEGER(mid)[0];

	MYSQL *sqlconn = NULL;
	int success = connectToLocalDB(&sqlconn);

	if(!success || sqlconn == NULL)
	{
		error("cannot connect to local db\n");
		return R_NilValue;
	}


	PROTECT(ans = R_do_new_object(R_getClassDef("dbmatrix")));
	PROTECT(info = allocVector(RAWSXP,sizeof(rdbMatrix)));
	rdbMatrix *vec = (rdbMatrix*)RAW(info);
        initRDBMatrix(vec);
	loadRDBMatrix(sqlconn, vec, id);
	vec->sxp_spare = 0;

	mysql_close(sqlconn);

	SEXP tablename;
	PROTECT(tablename= allocVector(STRSXP, 1));
	SET_STRING_ELT(tablename, 0, mkChar(vec->tableName));
	R_do_slot_assign(ans, install("tablename"), tablename);
	R_do_slot_assign(ans, install("info"), info);
	free(vec->tableName);
	/* do NOT register finalizer for a loaded dbvector*/

	UNPROTECT(3);
	return ans;
}

/* TODO: should sxp_spare be updated to 1 when persisting?
should sxp_spare be part of the condition when loading?
*/
SEXP persist_dbvector(SEXP x)
{
	SEXP extptr;
	SEXP ans;
	rdbVector *info;
	if ((extptr=R_do_slot(x, install("ext"))) == R_NilValue)
		error("persist() can only be called on objects with ext attribute\n");
	info = (rdbVector*)R_ExternalPtrAddr(extptr);
	free(info->tableName);
	free(info);

	R_RegisterCFinalizerEx(extptr, NULL, FALSE);
	R_do_slot_assign(x, install("ext"), R_NilValue);

	PROTECT(ans = allocVector(LGLSXP, 1));
	LOGICAL(ans)[0] = 1;
	UNPROTECT(1);

	return ans;
}

SEXP persist_dbmatrix(SEXP x)
{
	SEXP extptr;
	SEXP ans;
	rdbMatrix *info;
	if ((extptr=R_do_slot(x, install("ext"))) == R_NilValue)
		error("persist() can only be called on objects with ext attribute\n");
	info = (rdbMatrix*)R_ExternalPtrAddr(extptr);
	free(info->tableName);
	free(info);

	R_RegisterCFinalizerEx(extptr, NULL, FALSE);
	R_do_slot_assign(x, install("ext"), R_NilValue);

        PROTECT(ans = allocVector(LGLSXP, 1));
        LOGICAL(ans)[0] = 1;
        UNPROTECT(1);
        return ans;
}

SEXP materialize_dbmatrix(SEXP x)
{

	MYSQL *sqlconn = NULL;
	int success = connectToLocalDB(&sqlconn);

	if(!success || sqlconn == NULL)
	{
		error("cannot connect to local db\n");
		return R_NilValue;
	}

	rdbMatrix *info = getMatrixInfo(x);
	materializeMatrixView(sqlconn, info);

	mysql_close(sqlconn);

	/* copy info to the finalizer structure */
	rdbMatrix *temp = (rdbMatrix*)R_ExternalPtrAddr(R_do_slot(x,install("ext")));

	*temp = *info;
	return R_NilValue;
}


SEXP materialize_dbvector(SEXP x)
{

	MYSQL *sqlconn = NULL;
	int success = connectToLocalDB(&sqlconn);

	if(!success || sqlconn == NULL)
	{
		error("cannot connect to local db\n");
		return R_NilValue;
	}

	rdbVector *info = getInfo(x);
	materializeVectorView(sqlconn, info);

	mysql_close(sqlconn);

	/* copy info to the finalizer structure */
	rdbVector *temp = (rdbVector*)R_ExternalPtrAddr(R_do_slot(x,install("ext")));

	*temp = *info;
	return R_NilValue;
}
