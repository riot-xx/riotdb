#include "dbvector.h"
#include "utils.h"

int IS_DBVECTOR(SEXP x)
{
	return (strcmp(CHAR(asChar(getAttrib(x,R_ClassSymbol))),"dbvector")==0);
}

rdbVector *getInfo(SEXP x)
{
	rdbVector *info = (rdbVector*)RAW(R_do_slot(x,install("info")));
	info->tableName = CHAR(STRING_ELT(R_do_slot(x,install("tablename")),0));
	return info;
}

rdbMatrix *getMatrixInfo(SEXP x)
{
	rdbMatrix *info = (rdbMatrix*)RAW(R_do_slot(x,install("info")));
	info->tableName = CHAR(STRING_ELT(R_do_slot(x,install("tablename")),0));
	return info;
}

int getType(SEXP x)
{
	rdbVector *vec = getInfo(x);
	return vec->sxp_type;
}

void rdbMatrixFinalizer(SEXP p)
{
	Rprintf("Finalizing %p\n",p);
	rdbMatrix * info = R_ExternalPtrAddr(p);
	MYSQL *sqlconn = NULL;
	int success = connectToLocalDB(&sqlconn);

	if(!success || sqlconn == NULL)
	{
		error("cannot connect to local db\n");
		return ;
	}
	/* deleteRDBVector will decrement the counter for this invocation of finalizer*/
	if (deleteRDBMatrix(sqlconn, info))
	{
		free(info->tableName);
		free(info);
	}
	mysql_close(sqlconn);
}

void rdbVectorFinalizer(SEXP p)
{
	Rprintf("Finalizing %p\n", p);
	rdbVector * info = R_ExternalPtrAddr(p);
	MYSQL *sqlconn = NULL;
	int success = connectToLocalDB(&sqlconn);

	if(!success || sqlconn == NULL)
	{
		error("cannot connect to local db\n");
		return ;
	}
	/* deleteRDBVector will decrement the counter for this invocation of finalizer*/
	if (deleteRDBVector(sqlconn, info))
	{
		free(info->tableName);
		free(info);
	}
	mysql_close(sqlconn);
}

/*SEXP initInfo(SEXP x)
{
	SEXP t;
	rdbVector *info;
	if ((t=GET_SLOT(x,install("infoptr"))) == R_NilValue
		|| R_ExternalPtrAddr(t) == NULL)
	{
		Rprintf("initializing...\n");
		info = malloc(sizeof(rdbVector));
		rdbVector *internal = getInfo(x);
		*info = *internal;
		info->tableName = malloc(sizeof(char)*MAX_TABLE_NAME);
		strcpy(info->tableName, internal->tableName);
		SEXP ptr;
		SET_SLOT(x, install("infoptr"), (ptr = R_MakeExternalPtr(info, R_NilValue, R_NilValue)));
		R_RegisterCFinalizerEx(ptr, rdbvectorFinalizer, TRUE);
	}
	else {
	}
	return x;
}
*/
/*void setInfo(SEXP x, rdbVector *vec)
{
	SEXP ptr;
	SET_SLOT(x, install("infoptr"), (ptr = R_MakeExternalPtr(vec, R_NilValue, R_NilValue)));
	R_RegisterCFinalizerEx(ptr, rdbvectorFinalizer, TRUE);
}
*/
