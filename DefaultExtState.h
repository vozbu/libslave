#ifndef __SLAVE_DEFAULTEXTSTATE_H_
#define __SLAVE_DEFAULTEXTSTATE_H_

#include <mutex>

#include "SlaveStats.h"

namespace slave
{
class DefaultExtState: public ExtStateIface, protected State {
    std::mutex m_mutex;

public:
    State getState() override
    {
        std::lock_guard<std::mutex> lock(m_mutex);
        return *this;
    }
    void setConnecting() override
    {
        std::lock_guard<std::mutex> lock(m_mutex);
        connect_time = ::time(NULL);
        ++connect_count;
    }
    time_t getConnectTime() override
    {
        std::lock_guard<std::mutex> lock(m_mutex);
        return connect_time;
    }
    void setLastFilteredUpdateTime() override
    {
        std::lock_guard<std::mutex> lock(m_mutex);
        last_filtered_update = ::time(NULL);
    }
    time_t getLastFilteredUpdateTime() override
    {
        std::lock_guard<std::mutex> lock(m_mutex);
        return last_filtered_update;
    }
    void setLastEventTimePos(time_t t, unsigned long pos) override
    {
        std::lock_guard<std::mutex> lock(m_mutex);
        last_event_time = t; intransaction_pos = pos; last_update = ::time(NULL);
    }
    time_t getLastUpdateTime() override
    {
        std::lock_guard<std::mutex> lock(m_mutex);
        return last_update;
    }
    time_t getLastEventTime() override
    {
        std::lock_guard<std::mutex> lock(m_mutex);
        return last_event_time;
    }
    unsigned long getIntransactionPos() override
    {
        std::lock_guard<std::mutex> lock(m_mutex);
        return intransaction_pos;
    }
    void setMasterPosition(const Position& pos) override
    {
        std::lock_guard<std::mutex> lock(m_mutex);
        position = pos;
        intransaction_pos = pos.log_pos;
    }
    void saveMasterPosition() override {}

private:
    bool loadMasterPositionNoLock(Position& pos)
    {
        // we could use std::recursive_mutex instead,
        // but wicked toungues say it is a bad design
        pos.clear();
        return false;
    }

public:
    bool loadMasterPosition(Position& pos) override
    {
        std::lock_guard<std::mutex> lock(m_mutex);
        return loadMasterPositionNoLock(pos);
    }
    bool getMasterPosition(Position& pos) override
    {
        std::lock_guard<std::mutex> lock(m_mutex);
        if (!position.empty())
        {
            pos = position;
            if (intransaction_pos)
                pos.log_pos = intransaction_pos;
            return true;
        }
        return loadMasterPositionNoLock(pos);
    }
    unsigned int getConnectCount() override
    {
        std::lock_guard<std::mutex> lock(m_mutex);
        return connect_count;
    }
    void setStateProcessing(bool _state) override
    {
        std::lock_guard<std::mutex> lock(m_mutex);
        state_processing = _state;
    }
    bool getStateProcessing() override
    {
        std::lock_guard<std::mutex> lock(m_mutex);
        return state_processing;
    }
    void initTableCount(const std::string& t) override {}
    void incTableCount(const std::string& t) override {}
};

}// slave

#endif
