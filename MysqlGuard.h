#ifndef __SLAVE_MYSQL_GUARD_H__
#define __SLAVE_MYSQL_GUARD_H__

#include <pthread.h>

#include <mutex>
#include <stdexcept>

#include <mysql/mysql.h>

namespace mysql_guard
{

// mysql_init wrapper that calls mysql_library_init(..) and mysql_thread_init(..)
// if necessary and schedules mysql_thread_end() and mysql_library_end() on
// thread/process exit.
// Actually calls MysqlGuard::init() and mysql_init().
inline MYSQL* mysql_safe_init(MYSQL *mysql);

// mysql_real_connect thread-safe wrapper.
// mysql_real_connect is not thread-safe now at least when using SSL cecurity.
// https://bugs.mysql.com/bug.php?id=88994
// Actually surrounds mysql_real_query(..) call with MysqlGuard::connectMutex() lock.
inline MYSQL* mysql_safe_connect(MYSQL* mysql, const char* host, const char* user,
                                 const char* passwd, const char* db, unsigned int port,
                                 const char* unix_socket, unsigned long client_flag);

class MysqlGuard
{
public:
    /**
     * Call mysql_library_init and mysql_thread_init if necessary
     * (both must be called before any mysql_* call).
     * Schedule mysql library cleanup at thread and/or process exit.
     * Is already used by mysql_safe_init(..).
     */
    static void init()
    {
        static Global sGlobal;
        thread_local Local sLocal(sGlobal.m_DestructionKey);
    };

    /**
     * mysql_real_connect is not thread-safe now at least when using SSL security.
     * https://bugs.mysql.com/bug.php?id=88994
     * Lock it before connect or use mysql_safe_connect(..) wrapper above.
     */
    static std::mutex& connectMutex()
    {
        static std::mutex sConnectMutex;
        return sConnectMutex;
    }

private:
    /**
     * Global mysql_library_init initializer.
     * Must have one instance as static variable.
     * Calls mysql_library_end upon destruction.
     */
    struct Global
    {
        // pthread local key for use in Local structure. See Local description.
        pthread_key_t m_DestructionKey;

        Global()
        {
            // Set destructor function for proper call of mysql_thread_end.
            // See Local description.
            if (0 != pthread_key_create(&m_DestructionKey, Local::threadDestructor))
                throw std::runtime_error("pthread_key_create for mysql init returned an error");
            if (0 != mysql_library_init(0, nullptr, nullptr))
            {
                pthread_key_delete(m_DestructionKey);
                throw std::runtime_error("mysql_library_init returned an error");
            }
        }

        ~Global()
        {
            mysql_library_end();
            pthread_key_delete(m_DestructionKey);
        }
    };

    /**
     * Thread-local mysql_thread_init initializer.
     * Must have one instance as thread_local variable.
     * Calls mysql_thread_end if necessary upon destruction.
     * There is a problem with automatic mysql thread deinitialization:
     * https://bugs.mysql.com/bug.php?id=66740
     * First of all, mysql_thread_end must be called in every thread (that called
     * mysql_thread_init) before mysql_library_end, or 5 sec lag and warning will occur.
     * Second, mysql_thread_end relies on some pthread specific record, so
     * mysql_thread_end must be called before pthread specific vector is cleaned.
     * On some platforms, this vector is cleaned BEFORE calling of C++ thread_local
     * destructors (this order is not specified though). Thus we cannot rely on
     * destructor of a thread local variable and have to use pthread destructor
     * function for calling mysql_thread_end in proper time.
     * On the other hand there is no guarantee that pthread destructor will be
     * called before destruction of static objects, so we have to install
     * mysql thread cleaner in destructor of thread_local object too.
     */
    struct Local
    {
        // Prevents calling mysql_thread_end twice.
        bool m_NeedsCleanup = true;
        // pthread local key for pthread destructor.
        pthread_key_t& m_DestructionKey;

        // Call mysql_thread_end if mysql_thread_init was called previously.
        void destroyIfNecessary()
        {
            if (m_NeedsCleanup)
            {
                mysql_thread_end();
                m_NeedsCleanup = false;
                pthread_setspecific(m_DestructionKey, nullptr);
            }
        }

        // destroyIfNecessary wrapper for passing to pthread_key_create.
        static void threadDestructor(void* arg)
        {
            Local* sLocal = (Local*)arg; // C-style cast for a C-style void*.
            sLocal->destroyIfNecessary();
        }

        Local(pthread_key_t& aDestructionKey)
        : m_DestructionKey(aDestructionKey)
        {
            if (0 != pthread_setspecific(m_DestructionKey, this))
                throw std::runtime_error("pthread_setspecific for mysql init returned an error");
            if (0 != mysql_thread_init())
            {
                pthread_setspecific(m_DestructionKey, nullptr);
                throw std::runtime_error("mysql_thread_init returned an error");
            }
        }

        ~Local()
        {
            destroyIfNecessary();
        }
    };
};

MYSQL *mysql_safe_init(MYSQL *mysql)
{
    MysqlGuard::init();
    return mysql_init(mysql);
}

MYSQL* mysql_safe_connect(MYSQL* mysql, const char* host, const char* user,
                          const char* passwd, const char* db, unsigned int port,
                          const char* unix_socket, unsigned long client_flag)
{
    std::lock_guard<std::mutex> lock(MysqlGuard::connectMutex());
    return mysql_real_connect(mysql, host, user, passwd, db, port, unix_socket,
                              client_flag);
}

} // namespace mysql_guard

#endif
