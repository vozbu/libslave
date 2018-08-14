/* Copyright 2011 ZAO "Begun".
 *
 * This library is free software; you can redistribute it and/or modify it under
 * the terms of the GNU Lesser General Public License as published by the Free
 * Software Foundation; either version 3 of the License, or (at your option)
 * any later version.
 * This library is distributed in the hope that it will be useful, but WITHOUT
 * ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS
 * FOR A PARTICULAR PURPOSE. See the GNU Lesser General Public License for more
 * details.
 * You should have received a copy of the GNU Lesser General Public License
 * along with this library.  If not, see <http://www.gnu.org/licenses/>.
*/

#ifndef __SLAVE_TABLE_H_
#define __SLAVE_TABLE_H_


#include <functional>
#include <string>
#include <vector>
#include <map>
#include <memory>

#include "field.h"
#include "recordset.h"
#include "SlaveStats.h"


namespace slave
{

typedef std::unique_ptr<Field> PtrField;
typedef std::function<void (RecordSet&)> callback;
typedef EventKind filter;


inline bool should_process(EventKind filter, EventKind kind) { return (filter & kind) == kind; }

class Table {

public:

    std::vector<PtrField> fields;
    std::vector<unsigned char> column_filter;
    std::vector<unsigned> column_filter_fields;
    unsigned column_filter_count;
    RowType  row_type;

    callback m_callback;
    EventKind m_filter;

    void call_callback(slave::RecordSet& _rs, ExtStateIface &ext_state) const
    {
        // Some stats
        ext_state.incTableCount(full_name);
        ext_state.setLastFilteredUpdateTime();

        m_callback(_rs);
    }

    void set_column_filter(const std::vector<std::string> &_column_filter) {
        if (_column_filter.empty()) {
            column_filter.clear();
            column_filter_fields.clear();
            column_filter_count = 0;
            return;
        }

        column_filter.resize((fields.size() + 7)/8);
        column_filter_fields.resize(fields.size());
        column_filter_count = _column_filter.size();
        for (unsigned i = 0; i < column_filter.size(); i++) {
            column_filter[i] = 0;
            column_filter_fields[i] = 0;
        }

        // FIXME: this loop has complexity factor of O(N*M)
        for (auto i = _column_filter.begin(); i != _column_filter.end(); ++i) {
            const auto &field_name = *i;
            for (auto j = fields.begin(); j != fields.end(); ++j) {
                const auto &field = *j;
                if (field->getFieldName() == field_name) {
                    const int index = j - fields.begin();
                    column_filter[index>>3] |= (1<<(index&7));
                    column_filter_fields[index] = i - _column_filter.begin();
                    break;
                }
            }
        }
    }

    const std::string table_name;
    const std::string database_name;

    std::string full_name;

    Table(const std::string& db_name, const std::string& tbl_name) :
        column_filter_count(0),
        table_name(tbl_name), database_name(db_name),
        full_name(database_name + "." + table_name)
        {}

    Table() {}

};

}

#endif
