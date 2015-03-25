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


#ifndef __SLAVE_SLAVESTATS_H_
#define __SLAVE_SLAVESTATS_H_


/*
 * WARNING WARNING WARNING
 *
 * This file is intended as a demonstration only.
 * The code here works, but is not thread-safe or production-ready.
 * Please provide your own implementation to fill the gaps according
 * to your particular project's needs.
 */


#include <boost/shared_ptr.hpp>
#include <string>
#include <sys/time.h>


namespace slave
{

struct MasterInfo {

    std::string host;
    unsigned int port;
    std::string user;
    std::string password;
    std::string master_log_name;
    unsigned long master_log_pos;
    unsigned int connect_retry;

    MasterInfo() : port(3306), master_log_pos(0), connect_retry(10) {}

    MasterInfo(std::string host_, unsigned int port_, std::string user_,
               std::string password_, unsigned int connect_retry_) :
        host(host_),
        port(port_),
        user(user_),
        password(password_),
        master_log_name(),
        master_log_pos(0),
        connect_retry(connect_retry_)
        {}
};

struct State {
    time_t          connect_time;
    time_t          last_filtered_update;
    time_t          last_event_time;
    time_t          last_update;
    std::string     master_log_name;
    unsigned long   master_log_pos;
    unsigned long   intransaction_pos;
    unsigned int    connect_count;
    bool            state_processing;

    State() :
        connect_time(0),
        last_filtered_update(0),
        last_event_time(0),
        last_update(0),
        master_log_pos(0),
        intransaction_pos(0),
        connect_count(0),
        state_processing(false)
    {}
};

struct ExtStateIface {
    virtual State getState() = 0;
    virtual void setConnecting() = 0;
    virtual time_t getConnectTime() = 0;
    virtual void setLastFilteredUpdateTime() = 0;
    virtual time_t getLastFilteredUpdateTime() = 0;
    virtual void setLastEventTimePos(time_t t, unsigned long pos) = 0;
    virtual time_t getLastUpdateTime() = 0;
    virtual time_t getLastEventTime() = 0;
    virtual unsigned long getIntransactionPos() = 0;
    virtual void setMasterLogNamePos(const std::string& log_name, unsigned long pos) = 0;
    virtual unsigned long getMasterLogPos() = 0;
    virtual std::string getMasterLogName() = 0;

    // Saves master info into persistent storage, i.e. file or database.
    // In case of error will try to save master info until success.
    virtual void saveMasterInfo() = 0;

    // Reads master info from persistent storage.
    // If master info was not saved earlier (for i.e. it is the first daemon start),
    // position is cleared (pos = 0, name = ""). In such case library reads binlogs
    // from the current position.
    // Returns true if saved position was read, false otherwise.
    // In case of read error function will retry to read until success which means
    // known position or known absence of saved position.
    virtual bool loadMasterInfo(std::string& logname, unsigned long& pos) = 0;

    // Works like loadMasterInfo() but writes last position inside transaction if presented.
    bool getMasterInfo(std::string& logname, unsigned long& pos)
    {
        unsigned long in_trans_pos = getIntransactionPos();
        std::string master_logname = getMasterLogName();

        if(in_trans_pos!=0 && !master_logname.empty()) {
            logname = master_logname;
            pos = in_trans_pos;
            return true;
        } else
            return loadMasterInfo(logname, pos);
    }
    virtual unsigned int getConnectCount() = 0;
    virtual void setStateProcessing(bool _state) = 0;
    virtual bool getStateProcessing() = 0;
    // There is no standard format for events distribution in the tables,
    // so there is no function for getting this statistics.
    virtual void initTableCount(const std::string& t) = 0;
    virtual void incTableCount(const std::string& t) = 0;

    virtual ~ExtStateIface() {}
};


// Stub object for answers on stats requests through StateHolder while libslave is not initialized yet.
struct EmptyExtState: public ExtStateIface {
    EmptyExtState() : master_log_pos(0), intransaction_pos(0) {}

    virtual State getState() { return State(); }
    virtual void setConnecting() {}
    virtual time_t getConnectTime() { return 0; }
    virtual void setLastFilteredUpdateTime() {}
    virtual time_t getLastFilteredUpdateTime() { return 0; }
    virtual void setLastEventTimePos(time_t t, unsigned long pos) { intransaction_pos = pos; }
    virtual time_t getLastUpdateTime() { return 0; }
    virtual time_t getLastEventTime() { return 0; }
    virtual unsigned long getIntransactionPos() { return intransaction_pos; }
    virtual void setMasterLogNamePos(const std::string& log_name, unsigned long pos) { master_log_name = log_name; master_log_pos = intransaction_pos = pos;}
    virtual unsigned long getMasterLogPos() { return master_log_pos; }
    virtual std::string getMasterLogName() { return master_log_name; }
    virtual void saveMasterInfo() {}
    virtual bool loadMasterInfo(std::string& logname, unsigned long& pos) { logname.clear(); pos = 0; return false; }
    virtual unsigned int getConnectCount() { return 0; }
    virtual void setStateProcessing(bool _state) {}
    virtual bool getStateProcessing() { return false; }
    virtual void initTableCount(const std::string& t) {}
    virtual void incTableCount(const std::string& t) {}

private:
    std::string     master_log_name;
    unsigned long   master_log_pos;
    unsigned long   intransaction_pos;
};

// Saves ExtStateIface or it's descendants.
// Used in singleton.
struct StateHolder {
    typedef boost::shared_ptr<ExtStateIface> PExtState;
    PExtState ext_state;
    StateHolder() :
        ext_state(new EmptyExtState)
    {}
    ExtStateIface& operator()()
    {
        return *ext_state;
    }
};

enum EventKind
{
    eNone   = 0,
    eInsert = (1 << 0),
    eUpdate = (1 << 1),
    eDelete = (1 << 2),
    eAll    = (1 << 0) | (1 << 1) | (1 << 2)
};

typedef EventKind EventKindList[3];

inline const EventKindList& eventKindList()
{
    static EventKindList result = {eInsert, eUpdate, eDelete};
    return result;
}

// All stats calls are called independently.
// E. g., processing UPDATE on a table, tick() + one of tickModifyIgnored/tickModifyDone/tickModifyFailed will be called.
class EventStatIface
{
public:
    virtual ~EventStatIface()     {}
    // TABLE_MAP event.
    virtual void processTableMap(const unsigned long /*id*/, const std::string& /*table*/, const std::string& /*database*/) {}
    // All events.
    virtual void tick(time_t when) {}
    // FORMAT_DESCRIPTION events.
    virtual void tickFormatDescription() {}
    // QUERY events.
    virtual void tickQuery() {}
    // ROTATE events.
    virtual void tickRotate() {}
    // XID events.
    virtual void tickXid() {}
    // Unprocessed libslave events.
    virtual void tickOther() {}
    // UPDATE/INSERT/DELETE missed (there are not callbacks on given type of operation).
    virtual void tickModifyIgnored(const unsigned long /*id*/, EventKind /*kind*/) {}
    // UPDATE/INSERT/DELETE successfully processed.
    virtual void tickModifyDone(const unsigned long /*id*/, EventKind /*kind*/, uint64_t /*callbackWorkTimeNanoSeconds*/) {}
    // UPDATE/INSERT/DELETE processed with errors (caught exception).
    virtual void tickModifyFailed(const unsigned long /*id*/, EventKind /*kind*/, uint64_t /*callbackWorkTimeNanoSeconds*/) {}
};
}


#endif
