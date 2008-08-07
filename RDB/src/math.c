#include "dbvector.h"

SEXP math_dbvector(SEXP x, SEXP op)
{
	SEXP ans, rinfo, rtablename, rptr;

	rdbVector *xinfo = getInfo(x);

	MYSQL *sqlconn = NULL;
	int success = connectToLocalDB(&sqlconn);

	if(!success || sqlconn == NULL)
	{
		error("cannot connect to local db\n");
		return R_NilValue;
	}

	PROTECT(ans = R_do_new_object(R_getClassDef("dbvector")));
	PROTECT(rinfo = allocVector(RAWSXP,sizeof(rdbVector)));
	rdbVector *vec = (rdbVector*)RAW(rinfo);
	vec->tableName = malloc(MAX_TABLE_NAME*sizeof(char));
	initRDBVector(&vec, 0, 0);

	if (strcmp(CHAR(asChar(op)), "sin") == 0)
	{
		switch(xinfo->sxp_type)
		{
			case INTSXP:
			case REALSXP:
				performNumericSin(sqlconn, xinfo, vec);
				break;
			case CPLXSXP:
				performComplexSin(sqlconn, xinfo, vec);
				break;
			default:
				free(vec->tableName);
				mysql_close(sqlconn);
				UNPROTECT(2);
				error("wrong type");
		}
	}
	else if (strcmp(CHAR(asChar(op)), "cos") == 0)
	{
		switch(xinfo->sxp_type)
		{
			case INTSXP:
			case REALSXP:
				performNumericCos(sqlconn, xinfo, vec);
				break;
			case CPLXSXP:
				performComplexCos(sqlconn, xinfo, vec);
				break;
			default:
				free(vec->tableName);
				mysql_close(sqlconn);
				UNPROTECT(2);
				error("wrong type");
		}
	}
	else
	{
		free(vec->tableName);
		mysql_close(sqlconn);
		UNPROTECT(2);
		error("wrong type");
	}

	PROTECT(rtablename= allocVector(STRSXP, 1));
	SET_STRING_ELT(rtablename, 0, mkChar(vec->tableName));
	R_do_slot_assign(ans, install("tablename"), rtablename);
	R_do_slot_assign(ans, install("info"), rinfo);
	/* register finalizer */
	rdbVector *ptr = malloc(sizeof(rdbVector));
	*ptr = *vec;
	PROTECT(rptr = R_MakeExternalPtr(ptr, R_NilValue, R_NilValue));
	R_do_slot_assign(ans, install("ext"), rptr);
	R_RegisterCFinalizerEx(rptr, rdbvectorFinalizer, TRUE);

	UNPROTECT(4);
	return ans;
}
		

