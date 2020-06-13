#pragma once

#include <string>

namespace slave
{
    struct TableKey
    {
        TableKey() = default;
        TableKey(std::string db_name, std::string table_name);
        std::string db_name;
        std::string table_name;
    };

    bool operator<(const TableKey& lhs, const TableKey& rhs);
}
