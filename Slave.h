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

/*
 *
 * Guaranteed to work with MySQL5.1.23
 *
 */

#ifndef __SLAVE_SLAVE_H_
#define __SLAVE_SLAVE_H_


#include <functional>
#include <string>
#include <vector>
#include <map>
#include <set>
#include <memory>
#include <mutex>

#include <pthread.h>

#include <mysql/mysql.h>

#include "binlog_pos.h"
#include "slave_log_event.h"
#include "SlaveStats.h"
#include "TableKey.h"


namespace slave
{


class Slave
{
public:

    typedef std::set<TableKey> table_order_t;
    typedef std::map<TableKey, callback> callbacks_t;
    typedef std::map<TableKey, filter> filters_t;
    typedef std::map<TableKey, ddl_callback> ddl_callbacks_t;

    typedef std::vector<std::string> cols_t;
    typedef std::map<TableKey, cols_t> column_filters_t;
    typedef std::map<TableKey, RowType> row_types_t;

private:
    static inline bool falseFunction() { return false; };

    MYSQL mysql;

    int m_server_id;
    int m_master_version = 0;

    MasterInfo m_master_info;
    EmptyExtState empty_ext_state;
    ExtStateIface &ext_state;
    EventStatIface* event_stat = nullptr;
    bool m_gtid_enabled = false;

    table_order_t m_table_order;
    callbacks_t m_callbacks;
    ddl_callbacks_t m_ddl_callbacks;
    filters_t m_filters;
    column_filters_t m_column_filters;
    row_types_t m_row_types;

    typedef std::function<void (unsigned int)> xid_callback_t;
    xid_callback_t m_xid_callback;

    RelayLogInfo m_rli;

    pthread_t m_slave_thread_id = 0;
    std::mutex m_slave_thread_mutex;

    void createDatabaseStructure_(table_order_t& tabs, RelayLogInfo& rli) const;

public:

    Slave() : ext_state(empty_ext_state) {}
    Slave(ExtStateIface &state) : ext_state(state) {}
    Slave(const MasterInfo& _master_info) : Slave(_master_info, empty_ext_state) {}

    Slave(const MasterInfo& _master_info, ExtStateIface &state)
    : m_master_info(_master_info)
    , ext_state(state)
    , m_gtid_enabled(m_master_info.conn_options.mysql_slave_gtid_enabled)
    {
        ext_state.setMasterPosition(_master_info.position);
    }

    void linkEventStat(EventStatIface* _event_stat)
    {
        event_stat = _event_stat;
    }

    // Makes sense only when get_remote_binlog is not started
    void setMasterInfo(const MasterInfo& aMasterInfo)
    {
        m_master_info = aMasterInfo;
        m_gtid_enabled = m_master_info.conn_options.mysql_slave_gtid_enabled;
        ext_state.setMasterPosition(aMasterInfo.position);
    }
    const MasterInfo& masterInfo() const { return m_master_info; }

    // Reads current binlog position from database
    Position getLastBinlogPos() const;

    void setCallback(const std::string& _db_name, const std::string& _tbl_name, callback _callback,
                     const cols_t& column_filter, RowType row_type = RowType::Map, EventKind filter = eAll)
    {
        setCallback(_db_name, _tbl_name, _callback, row_type, filter);
        m_column_filters[{_db_name, _tbl_name}] = column_filter;
    }

    void setCallback(const std::string& _db_name, const std::string& _tbl_name, callback _callback,
                     RowType row_type = RowType::Map, EventKind filter = eAll)
    {
        const TableKey key{_db_name, _tbl_name};
        m_table_order.insert(key);
        m_callbacks[key] = _callback;
        m_filters[key] = filter;
        m_column_filters[key] = cols_t();
        m_row_types[key] = row_type;

        ext_state.initTableCount(_db_name + "." + _tbl_name);
    }

    void setDDLCallback(const std::string& _db_name, const std::string& _tbl_name, ddl_callback _callback)
    {
        m_ddl_callbacks[{_db_name, _tbl_name}] = _callback;
    }

    void setXidCallback(xid_callback_t _callback)
    {
        m_xid_callback = _callback;
    }

    void get_remote_binlog(const std::function<bool()>& _interruptFlag = &Slave::falseFunction);

    void createDatabaseStructure() {

        m_rli.clear();

        createDatabaseStructure_(m_table_order, m_rli);

        for (RelayLogInfo::name_to_table_t::iterator i = m_rli.m_table_map.begin(); i != m_rli.m_table_map.end(); ++i) {
            i->second->m_callback = m_callbacks[i->first];
            i->second->m_filter = m_filters[i->first];
            i->second->set_column_filter(m_column_filters[i->first]);
            i->second->row_type = m_row_types[i->first];
        }
    }

    table_order_t getTableOrder() const {
        return m_table_order;
    }

    void init();

    int serverId() const { return m_server_id; }
    int masterVersion() const { return m_master_version; }
    bool masterGe56() const { return m_master_version >= 50600; }

    // Closes connection, opened in get_remotee_binlog. Should be called if your have get_remote_binlog
    // blocked on reading data from mysql server in the separate thread and you want to stop this thread.
    // You should take care that interruptFlag will return 'true' after connection is closed.
    void close_connection();

protected:


    void check_master_version();

    void check_master_binlog_format();
    void check_master_gtid_mode();

    void check_slave_gtid_mode();

    int process_event(const slave::Basic_event_info& bei, RelayLogInfo& rli);

    void request_dump_wo_gtid(const std::string& logname, unsigned long start_position, MYSQL* mysql);
    void request_dump(const Position& pos, MYSQL* mysql);

    ulong read_event(MYSQL* mysql);

    void createTable(RelayLogInfo& rli,
                     const std::string& db_name, const std::string& tbl_name,
                     const collate_map_t& collate_map, nanomysql::Connection& conn) const;

    void register_slave_on_master(MYSQL* mysql);
    void deregister_slave_on_master(MYSQL* mysql);
    void do_checksum_handshake(MYSQL* mysql);

    void generateSlaveId();

};

}// slave

#endif
