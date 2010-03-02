/**************************************************************************** 
Functions for creating dbvectors.
Author: Yi Zhang
Date: Sep 8, 2008
****************************************************************************/ 
#include "riotdb.h"

SEXP dbvector_copy(SEXP from)
{
	SEXP ans, info;
	int i;
	double *real, *imaginary;

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

	int len = length(from);

/* there could be NA's in the 'from' vector */
	switch(TYPEOF(from))
	{
		case INTSXP:
		createNewIntVectorTable(sqlconn, vec);
		insertIntoIntVectorTable(sqlconn, vec, INTEGER(from), len);
		break;

		case REALSXP:
		createNewDoubleVectorTable(sqlconn, vec);
		insertIntoDoubleVectorTable(sqlconn, vec, REAL(from), len);
		break;

		case CPLXSXP:
		real = (double*)malloc(sizeof(double)*len);
		imaginary = (double*)malloc(sizeof(double)*len);
		Rcomplex *data = COMPLEX(from);
		for (i=0; i<len; i++) {
			/*if (data[i].r == NA_REAL && data[i].i == NA_REAL)*/
			real[i] = data[i].r;
			imaginary[i] = data[i].i;
		}
		createNewComplexVectorTable(sqlconn, vec);
		insertIntoComplexVectorTable(sqlconn, vec, real, len, imaginary, len, len);
		free(real);
		free(imaginary);
		break;

		default:
		error("cannot initialize a dbvector from the given type");
		break;
	}
	
	mysql_close(sqlconn);

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
	R_RegisterCFinalizerEx(rptr, rdbVectorFinalizer, TRUE);

	UNPROTECT(4);
        free(vec->tableName);
	return ans;
}

/* construct a new logical type dbvector.
len: integer
*/
SEXP logical_db(SEXP len)
{
	SEXP ans, info;
	int nprotect = 0;
	if (!isInteger(len) && !isReal(len)) {
		PROTECT(len = coerceVector(len, REALSXP));
		nprotect++;
	}
	if (asReal(len) > UINT_MAX)
		error("length exceeds UINT_MAX");
	unsigned int length = (unsigned int)(asReal(len));

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

	createNewLogicVectorTable(sqlconn, vec);
	insertSeqLogicVectorTable(sqlconn, vec, 0, length);

	mysql_close(sqlconn);

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
	R_RegisterCFinalizerEx(rptr, rdbVectorFinalizer, TRUE);

	UNPROTECT(4+nprotect);
        free(vec->tableName);

	return ans;
}
/* create a new complex type dbvector
if type==0, x:real part, y:imaginary part
if type==1, x:modulus part, y:argument part
returns a dbvector object
 UPDATE: caller makes sure type always is 0
*/
SEXP dbcomplex(SEXP len, SEXP x, SEXP y, SEXP type)
{
	SEXP ans, info;
	if(!isReal(len)) error("length must be a real");
	unsigned int length = (unsigned int)(asReal(len));

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

	int k = asInteger(type);
	double *real, *imaginary;

	int nprotect = 0;
	if (!isReal(x)) {
		PROTECT(x = coerceVector(x, REALSXP));
		nprotect++;
	}
	if (!isReal(y)) {
		PROTECT(y = coerceVector(y, REALSXP));
		nprotect++;
	}

	switch(k)
	{
		case 0:
		real = REAL(x);
		imaginary = REAL(y);
		createNewComplexVectorTable(sqlconn, vec);
		insertIntoComplexVectorTable(sqlconn, vec, real, length(x), imaginary, length(y), length);
		break;

		default:break;
	}

	mysql_close(sqlconn);

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
	R_RegisterCFinalizerEx(rptr, rdbVectorFinalizer, TRUE);

	UNPROTECT(4+nprotect);
        free(vec->tableName);
	return ans;
}

SEXP dbvector_from_seq(SEXP begin, SEXP end, SEXP by)
{
	SEXP ret,tablename,info;
	double rbegin, rend, rby;
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

	Rboolean useInt = FALSE;
	PROTECT(ret = R_do_new_object(R_getClassDef("dbvector")));
	SET_NAMED(ret, 0);

	useInt = (((int)rbegin) == rbegin);
	useInt &= (rbegin >= INT_MIN && rbegin <= INT_MAX && ((int)rby)==rby && rend >= INT_MIN && rend <= INT_MAX);

	MYSQL *sqlconn = NULL;
	int success = connectToLocalDB(&sqlconn);

	if(!success || sqlconn == NULL)
	{
		error("cannot connect to local db\n");
		return R_NilValue;
	}

	PROTECT(info = allocVector(RAWSXP,sizeof(rdbVector)));
	rdbVector *vec = (rdbVector*)RAW(info);
        initRDBVector(vec);


	if (useInt)
	{
		if (!createNewIntVectorTable(sqlconn, vec))
		{
			error("cannot create integer table: %s\n",mysql_error(sqlconn));
			return info;
		}

		if (!insertSeqIntVectorTable(sqlconn, vec, rbegin, rend, rby))
		{
			error("cannot insert into integer table: %s\n", mysql_error(sqlconn));
			return info;
		}
	}
	else 
	{
		if (!createNewDoubleVectorTable(sqlconn, vec))
		{
			error("cannot create double table: %s\n",mysql_error(sqlconn));
			return info;
		}
		if (!insertSeqDoubleVectorTable(sqlconn, vec, rbegin, rend, rby))
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
	rdbVector *ptr = malloc(sizeof(rdbVector));
	*ptr= *vec;
	PROTECT(rptr = R_MakeExternalPtr(ptr, R_NilValue, R_NilValue));
	R_do_slot_assign(ret, install("ext"), rptr);
	R_RegisterCFinalizerEx(rptr, rdbVectorFinalizer, TRUE);
	
	UNPROTECT(4);
        free(vec->tableName);
	return ret;
}

