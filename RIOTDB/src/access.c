/****************************************************************************
Functions that access information from dbvectors and dbmatrices
Author: Yi Zhang
Date: Sep 8, 2008
****************************************************************************/

#include "riotdb.h"


SEXP get_dim(SEXP x)
{
	SEXP dim;
	rdbMatrix *info = getMatrixInfo(x);
	PROTECT(dim = allocVector(INTSXP, 2));
	INTEGER(dim)[0] = info->numRows;
	INTEGER(dim)[1] = info->numCols;
	UNPROTECT(1);
	return dim;
}

SEXP show_dbmatrix(SEXP x)
{
	static char* types[26] = {0,0,0,0,0,0,0,0,0,0,"logical",0,0,"integer","real","complex"};
	rdbMatrix *info = getMatrixInfo(x);

	/*Rprintf("named %d\n",NAMED(x));*/
	Rprintf("Type: %s\n", types[info->sxp_type]);
	Rprintf("Size: [%d,%d]\n", info->numRows, info->numCols);
	Rprintf("Table Name: %s\n", info->tableName);
	Rprintf("Shared by %d\n",info->sxp_spare);
	Rprintf("isView %d\n",info->isView);
	return R_NilValue;
}

SEXP show_dbvector(SEXP x)
{
	static char* types[26] = {0,0,0,0,0,0,0,0,0,0,"logical",0,0,"integer","real","complex"};
	rdbVector *info = getInfo(x);

	/*Rprintf("named %d\n",NAMED(x));*/
	Rprintf("Type: %s\n", types[info->sxp_type]);
	Rprintf("Size: %d\n", info->size);
	Rprintf("Table Name: %s\n", info->tableName);
	Rprintf("Shared by %d\n",info->sxp_spare);
	Rprintf("isView %d\n",info->isView);
	/*SEXP my;
	if ((my=GET_SLOT(x,install("my"))) != R_NilValue)
	Rprintf("my %s\n", R_ExternalPtrAddr(my));
*/
	return R_NilValue;
}

/* Access elements from a dbvector.
   index: could be a normal vector or dbvector
   dbvector: to be accessed
*/
SEXP get_by_index(SEXP dbvector, SEXP index)
{
	unsigned int len;
	rdbVector *info;
	SEXP ans = R_NilValue;
	unsigned int *indices;
	char *flags;
	int i;

	info = getInfo(dbvector);

	MYSQL *sqlconn = NULL;
	int success = connectToLocalDB(&sqlconn);
	if(!success || sqlconn == NULL)
	{
		error("cannot connect to local db %s\n", mysql_error(sqlconn));
		return ans;
	}

	/* index is a dbvector */
	if (IS_DBVECTOR(index))
	{
		SEXP rinfo, rtablename, rptr;

		PROTECT(ans=R_do_new_object(R_getClassDef("dbvector")));
		PROTECT(rinfo = allocVector(RAWSXP, sizeof(rdbVector)));
		rdbVector *vec = (rdbVector*)RAW(rinfo);
                initRDBVector(vec);
		rdbVector *ivec = getInfo(index);
		len = ivec->size;
		/* logical index */
		if (ivec->sxp_type == LGLSXP)
		{
			switch(info->sxp_type)
			{
				case INTSXP:
				getIntElementsWithLogic(sqlconn, info, ivec, vec);
				break;
				case LGLSXP:
				getLogicElementsWithLogic(sqlconn, info, ivec, vec);
				break;
				case REALSXP:
				getDoubleElementsWithLogic(sqlconn, info, ivec, vec);
				break;
				case CPLXSXP:
				getComplexElementsWithLogic(sqlconn, info, ivec, vec);
				break;
				default:
				mysql_close(sqlconn);
				UNPROTECT(2);
				error("this type of access is not supported");
				break;
			}
		}
		else if (ivec->sxp_type == INTSXP || ivec->sxp_type == REALSXP)
		{
			switch(info->sxp_type)
			{
				case INTSXP:
				getIntElementsWithDBVector(sqlconn, info, ivec, vec);
				break;
				case LGLSXP:
				getLogicElementsWithDBVector(sqlconn, info, ivec, vec);
				break;
				case REALSXP:
				getDoubleElementsWithDBVector(sqlconn, info, ivec, vec);
				break;
				case CPLXSXP:
				getComplexElementsWithDBVector(sqlconn, info, ivec, vec);
				break;
				default:
				mysql_close(sqlconn);
				UNPROTECT(2);
				error("this type of access is not supported");
				break;
			}
		}
		else {
			mysql_close(sqlconn);
			UNPROTECT(2);
			error("this type of inde type is not supported");
		}
		PROTECT(rtablename = ScalarString(mkChar(vec->tableName)));
		R_do_slot_assign(ans, install("tablename"), rtablename);
		R_do_slot_assign(ans, install("info"), rinfo);
		rdbVector *ptr = malloc(sizeof(rdbVector));
		*ptr = *vec;
		PROTECT(rptr = R_MakeExternalPtr(ptr, R_NilValue, R_NilValue));
		R_RegisterCFinalizerEx(rptr, rdbVectorFinalizer, TRUE);
		R_do_slot_assign(ans, install("ext"), rptr);
		UNPROTECT(4);
                free(vec->tableName);
		mysql_close(sqlconn);
		return ans;
	}

	len = length(index);
	indices = calloc(len , sizeof(unsigned int));
	flags = calloc(len , sizeof(char));

	if (isInteger(index))
		for (i = 0; i < len; i++)
		{
			indices[i] = (unsigned int)INTEGER(index)[i];
		}
	else if (isReal(index))
		for (i = 0; i < len; i++)
		{
			indices[i] = (unsigned int)REAL(index)[i];
		}

		switch (info->sxp_type)
		{
			case LGLSXP:
			PROTECT(ans = allocVector(LGLSXP, len));
			for (i=0; i<len; i++)
				INTEGER(ans)[i] = NA_INTEGER;
			getSparseLogicElements(sqlconn, info, indices, len, INTEGER(ans), flags);
			break;

			case INTSXP:
			PROTECT(ans = allocVector(INTSXP,len));
			/* initialize with NA */
			for (i=0; i<len; i++)
				INTEGER(ans)[i]= NA_INTEGER;
			getSparseIntElements(sqlconn, info, indices, len, INTEGER(ans), flags);
			break;

			case REALSXP:
			PROTECT(ans = allocVector(REALSXP, len));
			/* initialize with NA */
			for (i=0; i<len; i++)
				REAL(ans)[i]= NA_REAL;
			getSparseDoubleElements(sqlconn, info, indices, len, REAL(ans), flags);
			break;

			case CPLXSXP:
			PROTECT(ans = allocVector(CPLXSXP, len));
			for (i=0; i<len; i++)
				COMPLEX(ans)[i].r = COMPLEX(ans)[i].i = NA_REAL;
			getSparseComplexElements(sqlconn, info, indices, len, COMPLEX(ans), flags);
			break;

			default:
			break;
		}
	free(indices);
	free(flags);
	UNPROTECT(1);
	return ans;
}

/* Return the length of a dbvector */
SEXP length_dbvector(SEXP x)
{
	SEXP ans;
	rdbVector *info = getInfo(x);
	PROTECT(ans = allocVector(INTSXP,1));
	INTEGER(ans)[0] = info->size;
	UNPROTECT(1);
	return ans;
}

SEXP get_matrix_by_index(SEXP dbmatrix, SEXP index1, SEXP index2)
{
	unsigned int len;
	rdbMatrix *info;
	SEXP ans = R_NilValue;
	unsigned int row, col;
	char flags[1];
	int i, len1, len2;
	int onedim = 0;

	info = getMatrixInfo(dbmatrix);

	MYSQL *sqlconn = NULL;
	int success = connectToLocalDB(&sqlconn);
	if(!success || sqlconn == NULL)
	{
		error("cannot connect to local db %s\n", mysql_error(sqlconn));
		return ans;
	}

	len1 = length(index1);
	len2 = length(index2);
	if (len1!=1 || len2!=1) {
		error("only single element extraction is supported now");
	}
	if (isInteger(index1))
		row = INTEGER(index1)[0];
	else
		row = (unsigned int)REAL(index1)[0];
	if (isInteger(index2))
		col = INTEGER(index2)[0];
	else
		col = (unsigned int)REAL(index2)[0];

	switch (info->sxp_type)
	{
		case LGLSXP:
			break;

		case INTSXP:
			/*PROTECT(ans = allocVector(INTSXP,len));
			for (i=0; i<len; i++)
				INTEGER(ans)[i]= NA_INTEGER;
			getSparseIntElements(sqlconn, info, indices, len, INTEGER(ans), flags);
			*/
			break;

		case REALSXP:
			PROTECT(ans = allocVector(REALSXP, 1));
			REAL(ans)[0]= NA_REAL;
			getDoubleMatrixElement(sqlconn, info, row, col, REAL(ans), flags);
			break;

		case CPLXSXP:
			break;

		default:
			break;
	}
	UNPROTECT(1);
	return ans;
}
