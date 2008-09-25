#include "dbvector.h"

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
	initRDBMatrix(&vec, 0, 0);
	vec->tableName = calloc(MAX_TABLE_NAME,sizeof(char));


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
	return ret;
}

