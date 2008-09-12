#ifndef RDB_SORT_VECTOR
#define RDB_SORT_VECTOR

#include "rdb_basics.h"
#define sqlTemplateCreateSortVector "create table %s(vIndex int(10) unsigned auto_increment, vValue %s, primary key(vIndex))"
#define sqlTemplateSortVector "insert into %s(vValue) select vValue from %s order by vValue %s"

int sortVector(MYSQL *sqlConn, rdbVector *vectorInfo,rdbVector *bInfo, int desc);

#endif
