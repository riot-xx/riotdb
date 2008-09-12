#include "dbvector.h"

SEXP dbvector_sort(SEXP from, SEXP desc)
{
	SEXP ans, info;

	rdbVector *fromInfo;

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
	initRDBVector(&vec, 0, 0);
	vec->tableName = malloc(MAX_TABLE_NAME*sizeof(char));

	fromInfo = getInfo(from);

	sortVector(sqlconn, fromInfo, vec, asInteger(desc));

	mysql_close(sqlconn);

	vec->sxp_spare = 0;
	SEXP tablename;
	PROTECT(tablename= allocVector(STRSXP, 1));
	SET_STRING_ELT(tablename, 0, mkChar(vec->tableName));
	R_do_slot_assign(ans, install("tablename"), tablename);
	R_do_slot_assign(ans, install("info"), info);
	/* register finalizer */
	rdbVector *ptr = malloc(sizeof(rdbVector));
	*ptr = *vec;
	SEXP rptr;
	PROTECT(rptr = R_MakeExternalPtr(ptr, R_NilValue, R_NilValue));
	R_do_slot_assign(ans, install("ext"), rptr);
	R_RegisterCFinalizerEx(rptr, rdbvectorFinalizer, TRUE);

	UNPROTECT(4);
	return ans;
}

