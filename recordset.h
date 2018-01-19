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

#ifndef __SLAVE_RECORDSET_H_
#define __SLAVE_RECORDSET_H_

#include <map>
#include <string>

#include "types.h"

namespace slave
{

// One row in a table. Key -- field name, value - pair of (field type, value)
typedef std::vector<std::pair<std::string, FieldValue>> RowVector;
typedef std::map<std::string, std::pair<std::string, FieldValue>> Row;

struct RecordSet
{
    Row       m_row;
    Row       m_old_row;
    RowVector m_row_vec;
    RowVector m_old_row_vec;
    RowType   row_type = RowType::Map;

    std::string tbl_name;
    std::string db_name;

    time_t when;

    enum TypeEvent { Update, Delete, Write };

    TypeEvent type_event;

    // Root master ID from which this record originated
    unsigned int master_id = 0;
};

}// slave

#endif
