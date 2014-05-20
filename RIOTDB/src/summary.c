/****************************************************************************
Functions for calculating summaries of dbvectors.
Author: Yi Zhang
Date: Sep 8, 2008
****************************************************************************/
#include "riotdb.h"

SEXP max_dbvector(SEXP x, SEXP narm)
{
	SEXP ans;
	rdbVector *info = getInfo(x);

	MYSQL *sqlconn = NULL;
	int success = connectToLocalDB(&sqlconn);

	if(!success || sqlconn == NULL)
	{
		error("cannot connect to local db\n");
		return R_NilValue;
	}

	int flag;
	hasRDBVectorAnyNA(sqlconn, info, &flag);


	if (!asLogical(narm) && flag)
	{
		PROTECT(ans = allocVector(LGLSXP, 1));
		LOGICAL(ans)[0] = NA_INTEGER;
		UNPROTECT(1);
	}
	else
	{
		int imax;
		double rmax;
		switch(info->sxp_type)
		{
			case INTSXP:
				getIntegerVectorMax(sqlconn, info, &imax);
				PROTECT(ans = allocVector(INTSXP, 1));
				INTEGER(ans)[0] = imax;
				UNPROTECT(1);
				break;

			case REALSXP:
				getDoubleVectorMax(sqlconn, info, &rmax);
				PROTECT(ans = allocVector(REALSXP, 1));
				REAL(ans)[0] = rmax;
				UNPROTECT(1);
				break;

			default:
				error("wrong type");
		}
	}
	mysql_close(sqlconn);
	return ans;
}

SEXP min_dbvector(SEXP x, SEXP narm)
{
	SEXP ans;
	rdbVector *info = getInfo(x);

	MYSQL *sqlconn = NULL;
	int success = connectToLocalDB(&sqlconn);

	if(!success || sqlconn == NULL)
	{
		error("cannot connect to local db\n");
		return R_NilValue;
	}

	int flag;
	hasRDBVectorAnyNA(sqlconn, info, &flag);


	if (!asLogical(narm) && flag)
	{
		PROTECT(ans = allocVector(LGLSXP, 1));
		LOGICAL(ans)[0] = NA_INTEGER;
		UNPROTECT(1);
	}
	else
	{
		int imax;
		double rmax;
		switch(info->sxp_type)
		{
			case INTSXP:
				getIntegerVectorMin(sqlconn, info, &imax);
				PROTECT(ans = allocVector(INTSXP, 1));
				INTEGER(ans)[0] = imax;
				UNPROTECT(1);
				break;

			case REALSXP:
				getDoubleVectorMin(sqlconn, info, &rmax);
				PROTECT(ans = allocVector(REALSXP, 1));
				REAL(ans)[0] = rmax;
				UNPROTECT(1);
				break;

			default:
				error("wrong type");
		}
	}
	mysql_close(sqlconn);
	return ans;
}

SEXP sum_dbvector(SEXP x, SEXP narm)
{
	SEXP ans;
	rdbVector *info = getInfo(x);

	MYSQL *sqlconn = NULL;
	int success = connectToLocalDB(&sqlconn);

	if(!success || sqlconn == NULL)
	{
		error("cannot connect to local db\n");
		return R_NilValue;
	}

	int flag;
	hasRDBVectorAnyNA(sqlconn, info, &flag);


	if (!asLogical(narm) && flag)
	{
		PROTECT(ans = allocVector(LGLSXP, 1));
		LOGICAL(ans)[0] = NA_INTEGER;
		UNPROTECT(1);
	}
	else
	{
		int imax;
		double rmax;
		Rcomplex cmax;
		switch(info->sxp_type)
		{
			case INTSXP:
				/* FIXME: sum of int could be double */
				getIntegerVectorSum(sqlconn, info, &imax);
				PROTECT(ans = allocVector(INTSXP, 1));
				INTEGER(ans)[0] = imax;
				UNPROTECT(1);
				break;

			case REALSXP:
				getDoubleVectorSum(sqlconn, info, &rmax);
				PROTECT(ans = allocVector(REALSXP, 1));
				REAL(ans)[0] = rmax;
				UNPROTECT(1);
				break;
			case CPLXSXP:
				getComplexVectorSum(sqlconn, info, &cmax);
				PROTECT(ans = allocVector(CPLXSXP, 1));
				COMPLEX(ans)[0] = cmax;
				UNPROTECT(1);
				break;
			default:
				error("wrong type");
		}
	}
	mysql_close(sqlconn);
	return ans;
}

SEXP mean_dbvector(SEXP x, SEXP narm)
{
	SEXP ans;
	rdbVector *info = getInfo(x);

	MYSQL *sqlconn = NULL;
	int success = connectToLocalDB(&sqlconn);

	if(!success || sqlconn == NULL)
	{
		error("cannot connect to local db\n");
		return R_NilValue;
	}

	int flag;
	hasRDBVectorAnyNA(sqlconn, info, &flag);


	if (!asLogical(narm) && flag)
	{
		PROTECT(ans = allocVector(LGLSXP, 1));
		LOGICAL(ans)[0] = NA_INTEGER;
		UNPROTECT(1);
	}
	else
	{
		double rmax;
		Rcomplex cmax;
		switch(info->sxp_type)
		{
			case INTSXP:
				getIntegerVectorAvg(sqlconn, info, &rmax);
				PROTECT(ans = allocVector(REALSXP, 1));
				REAL(ans)[0] = rmax;
				UNPROTECT(1);
				break;

			case REALSXP:
				getDoubleVectorAvg(sqlconn, info, &rmax);
				PROTECT(ans = allocVector(REALSXP, 1));
				REAL(ans)[0] = rmax;
				UNPROTECT(1);
				break;
			case CPLXSXP:
				getComplexVectorAvg(sqlconn, info, &cmax);
				PROTECT(ans = allocVector(CPLXSXP, 1));
				COMPLEX(ans)[0] = cmax;
				UNPROTECT(1);
				break;
			default:
				error("wrong type");
		}
	}
	mysql_close(sqlconn);
	return ans;
}
