#ifndef UTILS_H
#define UTILS_H
int IS_DBVECTOR(SEXP x);
rdbVector *getInfo(SEXP x);
rdbMatrix *getMatrixInfo(SEXP x);
int getType(SEXP x);
void rdbMatrixFinalizer(SEXP);
void rdbVectorFinalizer(SEXP);
#endif
