#ifndef __SLAVE_DEFAULTEXTSTATE_H_
#define __SLAVE_DEFAULTEXTSTATE_H_

#include <mutex>

#include "SlaveStats.h"

namespace slave
{
class DefaultExtState: public ExtStateIface, protected State {
    std::mutex m_mutex;

public:
    virtual State getState()
    {
        std::lock_guard<std::mutex> lock(m_mutex);
        return *this;
    }
    virtual void setConnecting()
    {
        std::lock_guard<std::mutex> lock(m_mutex);
        connect_time = ::time(NULL);
        ++connect_count;
    }
    virtual time_t getConnectTime()
    {
        std::lock_guard<std::mutex> lock(m_mutex);
        return connect_time;
    }
    virtual void setLastFilteredUpdateTime()
    {
        std::lock_guard<std::mutex> lock(m_mutex);
        last_filtered_update = ::time(NULL);
    }
    virtual time_t getLastFilteredUpdateTime()
    {
        std::lock_guard<std::mutex> lock(m_mutex);
        return last_filtered_update;
    }
    virtual void setLastEventTimePos(time_t t, unsigned long pos)
    {
        std::lock_guard<std::mutex> lock(m_mutex);
        last_event_time = t; intransaction_pos = pos; last_update = ::time(NULL);
    }
    virtual time_t getLastUpdateTime()
    {
        std::lock_guard<std::mutex> lock(m_mutex);
        return last_update;
    }
    virtual time_t getLastEventTime()
    {
        std::lock_guard<std::mutex> lock(m_mutex);
        return last_event_time;
    }
    virtual unsigned long getIntransactionPos()
    {
        std::lock_guard<std::mutex> lock(m_mutex);
        return intransaction_pos;
    }
    virtual void setMasterLogNamePos(const std::string& log_name, unsigned long pos)
    {
        std::lock_guard<std::mutex> lock(m_mutex);
        master_log_name = log_name; master_log_pos = pos;
        intransaction_pos = pos;
    }
    virtual unsigned long getMasterLogPos()
    {
        std::lock_guard<std::mutex> lock(m_mutex);
        return master_log_pos;
    }
    virtual std::string getMasterLogName()
    {
        std::lock_guard<std::mutex> lock(m_mutex);
        return master_log_name;
    }
    virtual void saveMasterInfo() {}
    virtual bool loadMasterInfo(std::string& logname, unsigned long& pos)
    {
        std::lock_guard<std::mutex> lock(m_mutex);
        logname.clear();
        pos = 0;
        return false;
    }
    virtual unsigned int getConnectCount()
    {
        std::lock_guard<std::mutex> lock(m_mutex);
        return connect_count;
    }
    virtual void setStateProcessing(bool _state)
    {
        std::lock_guard<std::mutex> lock(m_mutex);
        state_processing = _state;
    }
    virtual bool getStateProcessing()
    {
        std::lock_guard<std::mutex> lock(m_mutex);
        return state_processing;
    }
    virtual void initTableCount(const std::string& t) {}
    virtual void incTableCount(const std::string& t) {}
};

}// slave

#endif
