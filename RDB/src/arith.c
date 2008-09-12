#include "dbvector.h"

/**
The returned result of all the following aritchmetic operations is
intermediate. So finalizers must be registered so that if the result is not
assigned to a variable, it could be finalized in the future.
*/

/* Add two dbvectors */
SEXP add_dbvectors(SEXP x, SEXP y)
{
	rdbVector *xinfo, *yinfo;
	SEXP ans = R_NilValue;
	SEXP info,tablename;

	xinfo = getInfo(x);/*(rdbVector*) RAW(R_do_slot(x, install("info")));*/
	yinfo = getInfo(y);/*(rdbVector*) RAW(R_do_slot(y, install("info")));*/

	MYSQL *sqlconn = NULL;
	int success = connectToLocalDB(&sqlconn);
	if(!success || sqlconn == NULL)
	{
		error("cannot connect to local db %s\n", mysql_error(sqlconn));
		return ans;
	}


	PROTECT(ans = R_do_new_object(R_getClassDef("dbvector")));
	PROTECT(info = allocVector(RAWSXP,sizeof(rdbVector)));
	rdbVector *vec = (rdbVector*)RAW(info);
	vec->tableName = calloc(MAX_TABLE_NAME,sizeof(char));
	
	if (IS_DBCOMPLEX(x) || IS_DBCOMPLEX(y))
		addComplexVectors(sqlconn, vec, xinfo, yinfo);
	else
		addNumericVectors(sqlconn, vec, xinfo, yinfo);
	PROTECT(tablename= allocVector(STRSXP, 1));
	SET_STRING_ELT(tablename, 0, mkChar(vec->tableName));
	R_do_slot_assign(ans, install("tablename"), tablename);
	R_do_slot_assign(ans, install("info"), info);
	/* register finalizer */
	SEXP rptr;
	rdbVector *ptr = malloc(sizeof(rdbVector));
	*ptr = *vec;
	PROTECT(rptr = R_MakeExternalPtr(ptr, R_NilValue, R_NilValue));
	R_RegisterCFinalizerEx(rptr, rdbvectorFinalizer, TRUE);
	R_do_slot_assign(ans, install("ext"), rptr);
	UNPROTECT(4);
	return ans;
}

SEXP subtract_dbvectors(SEXP x, SEXP y)
{
	rdbVector *xinfo, *yinfo;
	SEXP ans = R_NilValue;
	SEXP info,tablename;

	xinfo = getInfo(x);/*(rdbVector*) RAW(R_do_slot(x, install("info")));*/
	yinfo = getInfo(y);/*(rdbVector*) RAW(R_do_slot(y, install("info")));*/

	MYSQL *sqlconn = NULL;
	int success = connectToLocalDB(&sqlconn);
	if(!success || sqlconn == NULL)
	{
		error("cannot connect to local db %s\n", mysql_error(sqlconn));
		return ans;
	}


	PROTECT(ans = R_do_new_object(R_getClassDef("dbvector")));
	PROTECT(info = allocVector(RAWSXP,sizeof(rdbVector)));
	rdbVector *vec = (rdbVector*)RAW(info);
	vec->tableName = Calloc(MAX_TABLE_NAME,char);
	
	if (IS_DBCOMPLEX(x) || IS_DBCOMPLEX(y))
		subtractComplexVectors(sqlconn, vec, xinfo, yinfo);
	else
		subtractNumericVectors(sqlconn, vec, xinfo, yinfo);

	PROTECT(tablename= allocVector(STRSXP, 1));
	SET_STRING_ELT(tablename, 0, mkChar(vec->tableName));
	R_do_slot_assign(ans, install("tablename"), tablename);
	R_do_slot_assign(ans, install("info"), info);
	/*free(vec->tableName);*/
	UNPROTECT(3);
	Free(vec->tableName);
	return ans;
}

SEXP multiply_dbvectors(SEXP x, SEXP y)
{
	rdbVector *xinfo, *yinfo;
	SEXP ans = R_NilValue;
	SEXP info,tablename;

	xinfo = getInfo(x);/*(rdbVector*) RAW(R_do_slot(x, install("info")));*/
	yinfo = getInfo(y);/*(rdbVector*) RAW(R_do_slot(y, install("info")));*/

	MYSQL *sqlconn = NULL;
	int success = connectToLocalDB(&sqlconn);
	if(!success || sqlconn == NULL)
	{
		error("cannot connect to local db %s\n", mysql_error(sqlconn));
		return ans;
	}


	PROTECT(ans = R_do_new_object(R_getClassDef("dbvector")));
	PROTECT(info = allocVector(RAWSXP,sizeof(rdbVector)));
	rdbVector *vec = (rdbVector*)RAW(info);
	vec->tableName = Calloc(MAX_TABLE_NAME,char);
	
	if (IS_DBCOMPLEX(x) || IS_DBCOMPLEX(y))
		multiplyComplexVectors(sqlconn, vec, xinfo, yinfo);
	else
		multiplyNumericVectors(sqlconn, vec, xinfo, yinfo);

	PROTECT(tablename= allocVector(STRSXP, 1));
	SET_STRING_ELT(tablename, 0, mkChar(vec->tableName));
	R_do_slot_assign(ans, install("tablename"), tablename);
	R_do_slot_assign(ans, install("info"), info);
	/*free(vec->tableName);*/
	UNPROTECT(3);
	Free(vec->tableName);
	return ans;
}
SEXP divide_dbvectors(SEXP x, SEXP y)
{
	rdbVector *xinfo, *yinfo;
	SEXP ans = R_NilValue;
	SEXP info,tablename;

	xinfo = getInfo(x);/*(rdbVector*) RAW(R_do_slot(x, install("info")));*/
	yinfo = getInfo(y);/*(rdbVector*) RAW(R_do_slot(y, install("info")));*/

	MYSQL *sqlconn = NULL;
	int success = connectToLocalDB(&sqlconn);
	if(!success || sqlconn == NULL)
	{
		error("cannot connect to local db %s\n", mysql_error(sqlconn));
		return ans;
	}


	PROTECT(ans = R_do_new_object(R_getClassDef("dbvector")));
	PROTECT(info = allocVector(RAWSXP,sizeof(rdbVector)));
	rdbVector *vec = (rdbVector*)RAW(info);
	vec->tableName = Calloc(MAX_TABLE_NAME,char);
	
	if (IS_DBCOMPLEX(x) || IS_DBCOMPLEX(y))
		divideComplexVectors(sqlconn, vec, xinfo, yinfo);
	else
		divideNumericVectors(sqlconn, vec, xinfo, yinfo);

	PROTECT(tablename= allocVector(STRSXP, 1));
	SET_STRING_ELT(tablename, 0, mkChar(vec->tableName));
	R_do_slot_assign(ans, install("tablename"), tablename);
	R_do_slot_assign(ans, install("info"), info);
	/*free(vec->tableName);*/
	UNPROTECT(3);
	Free(vec->tableName);
	return ans;
}

SEXP sqrt_dbvector(SEXP x)
{
	rdbVector *xinfo, *yinfo;
	SEXP ans = R_NilValue;
	SEXP info,tablename;

	xinfo = getInfo(x);/*(rdbVector*) RAW(R_do_slot(x, install("info")));*/

	MYSQL *sqlconn = NULL;
	int success = connectToLocalDB(&sqlconn);
	if(!success || sqlconn == NULL)
	{
		error("cannot connect to local db %s\n", mysql_error(sqlconn));
		return ans;
	}


	PROTECT(ans = R_do_new_object(R_getClassDef("dbvector")));
	PROTECT(info = allocVector(RAWSXP,sizeof(rdbVector)));
	rdbVector *vec = (rdbVector*)RAW(info);
	vec->tableName = calloc(MAX_TABLE_NAME,sizeof(char));
	
	if (IS_DBCOMPLEX(x) )
		addComplexVectors(sqlconn, vec, xinfo, yinfo);
	else
		sqrtNumericVector(sqlconn, vec, xinfo);
	PROTECT(tablename= allocVector(STRSXP, 1));
	SET_STRING_ELT(tablename, 0, mkChar(vec->tableName));
	R_do_slot_assign(ans, install("tablename"), tablename);
	R_do_slot_assign(ans, install("info"), info);
	/* register finalizer */
	SEXP rptr;
	rdbVector *ptr = malloc(sizeof(rdbVector));
	*ptr = *vec;
	PROTECT(rptr = R_MakeExternalPtr(ptr, R_NilValue, R_NilValue));
	R_RegisterCFinalizerEx(rptr, rdbvectorFinalizer, TRUE);
	R_do_slot_assign(ans, install("ext"), rptr);
	UNPROTECT(4);
	return ans;
}

SEXP pow_dbvector(SEXP x, SEXP e)
{
	rdbVector *xinfo, *yinfo;
	SEXP ans = R_NilValue;
	SEXP info,tablename;
double exponent = asReal(e);

	xinfo = getInfo(x);/*(rdbVector*) RAW(R_do_slot(x, install("info")));*/

	MYSQL *sqlconn = NULL;
	int success = connectToLocalDB(&sqlconn);
	if(!success || sqlconn == NULL)
	{
		error("cannot connect to local db %s\n", mysql_error(sqlconn));
		return ans;
	}


	PROTECT(ans = R_do_new_object(R_getClassDef("dbvector")));
	PROTECT(info = allocVector(RAWSXP,sizeof(rdbVector)));
	rdbVector *vec = (rdbVector*)RAW(info);
	vec->tableName = calloc(MAX_TABLE_NAME,sizeof(char));
	
		powNumericVector(sqlconn, vec, xinfo, exponent);
	PROTECT(tablename= allocVector(STRSXP, 1));
	SET_STRING_ELT(tablename, 0, mkChar(vec->tableName));
	R_do_slot_assign(ans, install("tablename"), tablename);
	R_do_slot_assign(ans, install("info"), info);
	/* register finalizer */
	SEXP rptr;
	rdbVector *ptr = malloc(sizeof(rdbVector));
	*ptr = *vec;
	PROTECT(rptr = R_MakeExternalPtr(ptr, R_NilValue, R_NilValue));
	R_RegisterCFinalizerEx(rptr, rdbvectorFinalizer, TRUE);
	R_do_slot_assign(ans, install("ext"), rptr);
	UNPROTECT(4);
	return ans;
}
SEXP subtract_dbvector_numeric(SEXP x, SEXP e)
{
	rdbVector *xinfo;
	SEXP ans = R_NilValue;
	SEXP info,tablename;
double y= asReal(e);

	xinfo = getInfo(x);/*(rdbVector*) RAW(R_do_slot(x, install("info")));*/

	MYSQL *sqlconn = NULL;
	int success = connectToLocalDB(&sqlconn);
	if(!success || sqlconn == NULL)
	{
		error("cannot connect to local db %s\n", mysql_error(sqlconn));
		return ans;
	}


	PROTECT(ans = R_do_new_object(R_getClassDef("dbvector")));
	PROTECT(info = allocVector(RAWSXP,sizeof(rdbVector)));
	rdbVector *vec = (rdbVector*)RAW(info);
	vec->tableName = calloc(MAX_TABLE_NAME,sizeof(char));
	
		subtractVectorNumeric(sqlconn, vec, xinfo, y);
	PROTECT(tablename= allocVector(STRSXP, 1));
	SET_STRING_ELT(tablename, 0, mkChar(vec->tableName));
	R_do_slot_assign(ans, install("tablename"), tablename);
	R_do_slot_assign(ans, install("info"), info);
	/* register finalizer */
	SEXP rptr;
	rdbVector *ptr = malloc(sizeof(rdbVector));
	*ptr = *vec;
	PROTECT(rptr = R_MakeExternalPtr(ptr, R_NilValue, R_NilValue));
	R_RegisterCFinalizerEx(rptr, rdbvectorFinalizer, TRUE);
	R_do_slot_assign(ans, install("ext"), rptr);
	UNPROTECT(4);
	return ans;
}
