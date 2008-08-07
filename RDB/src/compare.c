#include "dbvector.h"

SEXP compare_dbvector_numeric(SEXP x, SEXP y, SEXP op)
{
	SEXP ans, info, tablename;
	PROTECT(ans = R_do_new_object(R_getClassDef("dbvector")));
	PROTECT(info = allocVector(RAWSXP, sizeof(rdbVector)));
	rdbVector *vec = (rdbVector*)RAW(info);
	vec->tableName = calloc(MAX_TABLE_NAME, sizeof(char));
	initRDBVector(&vec, 0, 0);

	MYSQL *sqlconn = NULL;
	int success = connectToLocalDB(&sqlconn);

	if(!success || sqlconn == NULL)
	{
		error("cannot connect to local db\n");
		return R_NilValue;
	}

	rdbVector *xinfo = getInfo(x);

	switch(TYPEOF(y))
	{
		case REALSXP:
			compareWithDoubleValues(sqlconn, vec, xinfo, REAL(y), length(y), CHAR(asChar(op)));
			break;
		case INTSXP:
			compareWithIntegerValues(sqlconn, vec, xinfo, INTEGER(y), length(y), CHAR(asChar(op)));
			break;
		default:
			mysql_close(sqlconn);
			free(vec->tableName);
			UNPROTECT(2);
			error("wrong type for comparison");
	}

	mysql_close(sqlconn);

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
