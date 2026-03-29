#pragma once
#include "parser/ast.h"
#include "common/types.h"
#include <string>

class Executor {
public:
    QueryResult execute(const Statement& stmt);
    QueryResult execute(Statement&& stmt);

private:
    QueryResult exec_create(const CreateTableStmt& s);
    QueryResult exec_insert(const InsertStmt& s);
    QueryResult exec_insert(InsertStmt&& s);
    QueryResult exec_select(const SelectStmt& s);
    QueryResult exec_group_by(const SelectStmt& s);
    QueryResult exec_delete(const DeleteStmt& s);
    QueryResult exec_join(const SelectStmt& s);
    QueryResult exec_drop(const DropTableStmt& s);
    QueryResult exec_show_tables();
    QueryResult exec_show_databases();
    QueryResult exec_describe(const DescribeStmt& s);
    QueryResult exec_update(const UpdateStmt& s);
    QueryResult exec_truncate(const TruncateStmt& s);
    QueryResult exec_use_database(const UseDatabaseStmt& s);
    QueryResult exec_alter(const AlterTableStmt& s);
};
