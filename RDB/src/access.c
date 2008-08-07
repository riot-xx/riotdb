#include "dbvector.h"


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
		vec->tableName = calloc(MAX_TABLE_NAME, sizeof(char));
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
				free(vec->tableName);
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
				free(vec->tableName);
				UNPROTECT(2);
				error("this type of access is not supported");
				break;
			}
		}
		else {
			mysql_close(sqlconn);
			free(vec->tableName);
			UNPROTECT(2);
			error("this type of inde type is not supported");
		}
		PROTECT(rtablename = ScalarString(mkChar(vec->tableName)));
		R_do_slot_assign(ans, install("tablename"), rtablename);
		R_do_slot_assign(ans, install("info"), rinfo);
		rdbVector *ptr = malloc(sizeof(rdbVector));
		*ptr = *vec;
		PROTECT(rptr = R_MakeExternalPtr(ptr, R_NilValue, R_NilValue));
		R_RegisterCFinalizerEx(rptr, rdbvectorFinalizer, TRUE);
		R_do_slot_assign(ans, install("ext"), rptr);
		UNPROTECT(4);
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
