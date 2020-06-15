#include "TableKey.h"
#include <utility>
#include <tuple>

namespace slave
{
    TableKey::TableKey(std::string db_name, std::string table_name)
        : db_name(std::move(db_name)), table_name(std::move(table_name)) {}

    bool operator<(const TableKey& lhs, const TableKey& rhs)
    {
        auto tie = [](const TableKey& tableKey) { return std::tie(tableKey.table_name, tableKey.db_name); };
        return tie(lhs) < tie(rhs);
    }
}
