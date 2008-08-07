#include "dbvector.h"

SEXP duplicateOrConvert(SEXP orig, int (*func)(MYSQL*,rdbVector*,rdbVector*), MYSQL *sqlconn, int needConvert)
{
	SEXP x, tname;
	rdbVector *info;
	rdbVector *origInfo = getInfo(orig);

	/* if this dbvector is shared by more than one variable
	we should copy before set */
	if (origInfo->sxp_spare > 1 || needConvert)
	{
		PROTECT(x = duplicate(orig));
		Rprintf("new address %p \n",x);
		SEXP s = R_do_slot(x,install("info"));
		info = (rdbVector*)RAW(s);
		info->tableName = calloc(MAX_TABLE_NAME, sizeof(char));
		info->sxp_spare = 0;
		func(sqlconn, origInfo, info);
		Rprintf("duplicated into table %s\n",info->tableName);

		/* set tablename slot */
		PROTECT(tname = ScalarString(mkChar(info->tableName)));
		R_do_slot_assign(x, install("tablename"), tname);


		/* register finalizer for the new duplicate */
		rdbVector *ptr = malloc(sizeof(rdbVector));
		*ptr = *info;
		SEXP rptr;
		R_do_slot_assign(x, install("ext"), (rptr=R_MakeExternalPtr(ptr, R_NilValue, R_NilValue)));
		R_RegisterCFinalizerEx(rptr, rdbvectorFinalizer, TRUE);
		
		origInfo->sxp_spare--;
		UNPROTECT(2);
	}
	else
	{
		x = orig;
		/* set ref counter to 0 because it'll be incremented when the result is bound to a name */
		origInfo->sxp_spare = 0;
		/*
		rdbVector oldInfo = *origInfo;
		origInfo->tableName = calloc(MAX_TABLE_NAME, sizeof(char));
		func(sqlconn, &oldInfo, origInfo);
		*/
		/* save new table name */
		/*
		PROTECT(tname = ScalarString(mkChar(origInfo->tableName)));
		R_do_slot_assign(x, install("tablename"), tname);
		UNPROTECT(1);
		*/

		/* update finalizer */
		/*
		SEXP rptr = R_do_slot(x, install("ext"));
		rdbVector *ptr = R_ExternalPtrAddr(rptr);
		rdbVector *ptr = malloc(sizeof(rdbVector));
		*ptr = *info;
		rep->tableName = calloc(MAX_TABLE_NAME, sizeof(char));
		strcpy(ptr->tableName, info->tableName);
		SEXP rptr;
		R_do_slot_assign(x, install("ext"), (rptr=R_MakeExternalPtr(ptr, R_NilValue, R_NilValue)));
		R_RegisterCFinalizerEx(rptr, rdbvectorFinalizer, TRUE);
		*/
	}
	return x;
}


/* Set elements of a dbvector. When the new value is NA, the original element
   deleted from the table.
*/
SEXP set(SEXP x, SEXP index, SEXP value)
{
	SEXP ans = R_NilValue;
	rdbVector *origInfo, *info;
	unsigned int len, i;
	unsigned int *_index;

	origInfo = getInfo(x);

	/* check value type */
	if (!(isInteger(value)||isReal(value)||isComplex(value)))
	{
		error("wrong value type");
	}

	/* connect to database */
	MYSQL *sqlconn = NULL;
	int success = connectToLocalDB(&sqlconn);
	if(!success || sqlconn == NULL)
	{
		error("cannot connect to local db\n");
		return ans;
	}

	/* convert index to int */
	if (isInteger(index))
	{
		len = length(index);
		Rprintf(" integer length %d\n",len);
		_index = calloc(len, sizeof(unsigned int));
		_index = INTEGER(index);
	}
	else if (isReal(index))
	{
		len = length(index);
		_index = calloc(len, sizeof(unsigned int));
		for (i=0;i<len;i++)
			_index[i] = (unsigned int)REAL(index)[i];
	}
	else if (IS_DBVECTOR(index))
	{
		rdbVector *iinfo = getInfo(index);
		if (iinfo->sxp_type != INTSXP && iinfo->sxp_type != REALSXP &&  iinfo->sxp_type!=LGLSXP) {
			mysql_close(sqlconn);
			error("index must be of logical, integer or numeric type");
		}

		int settype = origInfo->sxp_type * 100 + TYPEOF(value);
		if (iinfo->sxp_type == LGLSXP){
		switch(settype)
		{
		case 1010: /* logical, logical */
			x = duplicateOrConvert(x, duplicateVectorTable, sqlconn, 0);
			info = getInfo(x);
			setLogicElementsWithLogic(sqlconn, info, iinfo, INTEGER(value), length(value));
			break;
		case 1013: /* logical, int */
			x = duplicateOrConvert(x, convertLogicToInteger, sqlconn, 1);
			info = getInfo(x);
			setIntElementsWithLogic(sqlconn, info, iinfo, INTEGER(value), length(value));
			break;
		case 1014: /* logical, real */
			x = duplicateOrConvert(x, convertLogicToDouble, sqlconn, 1);
			info = getInfo(x);
			setDoubleElementsWithLogic(sqlconn, info, iinfo, REAL(value), length(value));
			break;
		case 1313: /* int,int*/
			x = duplicateOrConvert(x, duplicateVectorTable, sqlconn, 0);
			info = getInfo(x);
			setIntElementsWithLogic(sqlconn, info, iinfo, INTEGER(value), length(value));
			break;
		case 1314: /* int,real */
			x = duplicateOrConvert(x, convertIntegerToDouble, sqlconn, 1);
			info = getInfo(x);
			setDoubleElementsWithLogic(sqlconn, info, iinfo, REAL(value), length(value));
			break;
		case 1315: /* int, cplx */
			x = duplicateOrConvert(x,convertNumericToComplex, sqlconn, 1);
			info = getInfo(x);
			setComplexElementsWithLogic(sqlconn, info, iinfo, COMPLEX(value), length(value));
			break;
		case 1413:
			x = duplicateOrConvert(x, duplicateVectorTable, sqlconn, 0);
			info = getInfo(x);
			setDoubleElementsWithLogic(sqlconn, info, iinfo, REAL(coerceVector(value,REALSXP)), length(value));
			break;
		case 1414:
			x = duplicateOrConvert(x, duplicateVectorTable, sqlconn, 0);
			info = getInfo(x);
			setDoubleElementsWithLogic(sqlconn, info, iinfo, REAL(value), length(value));
			break;
		case 1415:
			x = duplicateOrConvert(x, convertNumericToComplex, sqlconn, 1);
			info = getInfo(x);
			setComplexElementsWithLogic(sqlconn, info, iinfo, COMPLEX(value), length(value));
			break;
		case 1513:
		case 1514:
			x = duplicateOrConvert(x, duplicateVectorTable, sqlconn, 0);
			info = getInfo(x);
			setComplexElementsWithLogic(sqlconn, info, iinfo, COMPLEX(coerceVector(value,CPLXSXP)), length(value));
			break;
		case 1515:
			x = duplicateOrConvert(x, duplicateVectorTable, sqlconn, 0);
			info = getInfo(x);
			setComplexElementsWithLogic(sqlconn, info, iinfo, COMPLEX(value), length(value));
			break;
		default:
			error("Setting elements of this type of dbvector is not supported");
			break;
		}
		} 
		else { /* int or real type index */
		switch(settype)
		{
		case 1010: /* logical, logical */
			x = duplicateOrConvert(x, duplicateVectorTable, sqlconn, 0);
			info = getInfo(x);
			setLogicElementsWDBVector(sqlconn, info, iinfo, INTEGER(value), length(value));
			break;
		case 1013: /* logical, int */
			x = duplicateOrConvert(x, convertLogicToInteger, sqlconn, 1);
			info = getInfo(x);
			setIntElementsWDBVector(sqlconn, info, iinfo, INTEGER(value), length(value));
			break;
		case 1014: /* logical, real */
			x = duplicateOrConvert(x, convertLogicToDouble, sqlconn, 1);
			info = getInfo(x);
			setDoubleElementsWDBVector(sqlconn, info, iinfo, REAL(value), length(value));
			break;
		case 1313: /* int,int*/
			x = duplicateOrConvert(x, duplicateVectorTable, sqlconn, 0);
			info = getInfo(x);
			setIntElementsWDBVector(sqlconn, info, iinfo, INTEGER(value), length(value));
			break;
		case 1314: /* int,real */
			x = duplicateOrConvert(x, convertIntegerToDouble, sqlconn, 1);
			info = getInfo(x);
			setDoubleElementsWDBVector(sqlconn, info, iinfo, REAL(value), length(value));
			break;
		case 1315: /* int, cplx */
			x = duplicateOrConvert(x,convertNumericToComplex, sqlconn, 1);
			info = getInfo(x);
			setComplexElementsWDBVector(sqlconn, info, iinfo, COMPLEX(value), length(value));
			break;
		case 1413:
			x = duplicateOrConvert(x, duplicateVectorTable, sqlconn, 0);
			info = getInfo(x);
			setDoubleElementsWDBVector(sqlconn, info, iinfo, REAL(coerceVector(value,REALSXP)), length(value));
			break;
		case 1414:
			x = duplicateOrConvert(x, duplicateVectorTable, sqlconn, 0);
			info = getInfo(x);
			setDoubleElementsWDBVector(sqlconn, info, iinfo, REAL(value), length(value));
			break;
		case 1415:
			x = duplicateOrConvert(x, convertNumericToComplex, sqlconn, 1);
			info = getInfo(x);
			setComplexElementsWDBVector(sqlconn, info, iinfo, COMPLEX(value), length(value));
			break;
		case 1513:
		case 1514:
			x = duplicateOrConvert(x, duplicateVectorTable, sqlconn, 0);
			info = getInfo(x);
			setComplexElementsWDBVector(sqlconn, info, iinfo, COMPLEX(coerceVector(value,CPLXSXP)), length(value));
			break;
		case 1515:
			x = duplicateOrConvert(x, duplicateVectorTable, sqlconn, 0);
			info = getInfo(x);
			setComplexElementsWDBVector(sqlconn, info, iinfo, COMPLEX(value), length(value));
			break;
		default:
			error("Setting elements of this type of dbvector is not supported");
			break;
		}
		} /* end if */
		mysql_close(sqlconn);
		free(_index);

		/* info has possibly been updated */
		rdbVector *temp = (rdbVector*)R_ExternalPtrAddr(R_do_slot(x,install("ext")));
		temp->isView = info->isView;
		temp->refCounter = info->refCounter;

		return x;
	}


	/* detect type */
	int settype = origInfo->sxp_type *100 + TYPEOF(value);
	switch(settype)
	{
		case 1010: /* logical, logical */
			x = duplicateOrConvert(x, duplicateVectorTable, sqlconn, 0);
			info = getInfo(x);
			setSparseLogicElements(sqlconn, info, _index, len, INTEGER(value), length(value));
			break;
		case 1013: /* logical, int */
			x = duplicateOrConvert(x, convertLogicToInteger, sqlconn, 1);
			info = getInfo(x);
			setSparseIntElements(sqlconn, info, _index, len, INTEGER(value), length(value));
			break;
		case 1014: /* logical, real */
			x = duplicateOrConvert(x, convertLogicToDouble, sqlconn, 1);
			info = getInfo(x);
			setSparseDoubleElements(sqlconn, info, _index, len, REAL(value), length(value));
			break;
			
		case 1313: /* int,int*/
			x = duplicateOrConvert(x, duplicateVectorTable, sqlconn, 0);
			info = getInfo(x);
			setSparseIntElements(sqlconn, info, _index, len, INTEGER(value), length(value));
			break;
		case 1314: /* int,real */
			x = duplicateOrConvert(x, convertIntegerToDouble, sqlconn, 1);
			info = getInfo(x);
			setSparseDoubleElements(sqlconn, info, _index, len, REAL(value), length(value));
			break;
		case 1315: /* int, cplx */
			x = duplicateOrConvert(x,convertNumericToComplex, sqlconn, 1);
			info = getInfo(x);
			Rprintf("new table name %s\n",info->tableName);
			setSparseComplexElements(sqlconn, info, _index, len, COMPLEX(value), length(value));
			break;
		case 1413:
			x = duplicateOrConvert(x, duplicateVectorTable, sqlconn, 0);
			info = getInfo(x);
			setSparseDoubleElements(sqlconn, info, _index, len, REAL(coerceVector(value,REALSXP)), length(value));
			break;
		case 1414:
			x = duplicateOrConvert(x, duplicateVectorTable, sqlconn, 0);
			info = getInfo(x);
			setSparseDoubleElements(sqlconn, info, _index, len, REAL(value), length(value));
			break;
		case 1415:
			x = duplicateOrConvert(x, convertNumericToComplex, sqlconn, 1);
			info = getInfo(x);
			setSparseComplexElements(sqlconn, info, _index, len, COMPLEX(value), length(value));
			break;
		case 1513:
		case 1514:
			x = duplicateOrConvert(x, duplicateVectorTable, sqlconn, 0);
			info = getInfo(x);
			setSparseComplexElements(sqlconn, info, _index, len, COMPLEX(coerceVector(value,CPLXSXP)), length(value));
			break;
		case 1515:
			x = duplicateOrConvert(x, duplicateVectorTable, sqlconn, 0);
			info = getInfo(x);
			setSparseComplexElements(sqlconn, info, _index, len, COMPLEX(value), length(value));
			break;
		default:
			error("Setting elements of this type of dbvector is not supported");
			break;
	}

	mysql_close(sqlconn);
	free(_index);

	/* info has possibly been updated */
	rdbVector *temp = (rdbVector*)R_ExternalPtrAddr(R_do_slot(x,install("ext")));
	temp->isView = info->isView;
	temp->refCounter = info->refCounter;

	return x;
}

/* Delete elements from a dbvector. Returns the result after deletion */
/* TODO: dbvector type of index: int/real or logical */
SEXP delete_from_dbvector(SEXP x, SEXP index)
{
	rdbVector *info = getInfo(x);

	if (IS_DBVECTOR(index)) {
		error("Setting to NA with dbvector-typed index is not supported");
	}

	MYSQL *sqlconn = NULL;
	int success = connectToLocalDB(&sqlconn);
	if(!success || sqlconn == NULL)
	{
		error("cannot connect to local db\n");
		return R_NilValue;
	}

	int *p;
	int start, stop, count;
	int len = length(index);
	switch(TYPEOF(index))
	{
		case REALSXP:
			index = coerceVector(index, INTSXP);
		case INTSXP:
			p = INTEGER(index);
			start = stop = *p;
			count = 1;
			while (count < len)
			{
				if (stop+1==*(p+count))
				{
					count++;
					stop++;
					continue;
				}
				/* start -> stop is a continuous range */
				deleteRangeElements(sqlconn, info, start, stop);
				stop = start = *(p+count);
				count++;
			}
			/* last range */
			deleteRangeElements(sqlconn, info, start, stop);

			break;


		default:
			error("This type of index is not supported");
	}

	mysql_close(sqlconn);
	return x;
}
