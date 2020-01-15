[![Build status](https://travis-ci.org/vozbu/libslave.svg)](https://travis-ci.org/vozbu/libslave)

ABOUT
===================================================================

This is a library that allows any arbitrary C++ application to connect
to a MySQL replication master and read/parse the replication binary
logs.

In effect, any application can now act like a MySQL replication slave,
without having to compile or link with any MySQL server code.

One important use-case for this library is for receiving changes in
the master database in real-time, without having the store the
master's data on the client server.

Features
-------------------------------------------------------------------
* Statistics of rps and execution time of user callbacks for every
event in every table.
* Support for MySQL options:
  * binlog_checksum=(NONE,CRC32)
  * binlog_row_image=(full,minimal)
  * GTID or log name and position positioning
* Column filter - you can receive only desired subset of fields from
a table in callback.
* Distinguish between absense of field and NULL field.
* Optional use `boost::variant` instead of `boost::any` for field
value storing.
* Store field values in `vector` by indexes instead of `std::map`
by names. Must be used in conjunction with column filter.
* Handling DDL queries like `CREATE TABLE`, `ALTER TABLE` and
`RENAME TABLE` (the latter is crucial for alters via
[gh-ost](https://github.com/github/gh-ost) to work).

USAGE
===================================================================

Build requirements
-------------------------------------------------------------------

For building the library, you will need:

 * g++ with C++17 support.

 * The standard MySQL C client libraries (libmysqlclient):
   * for 5.6-5.7 versions you will need place **hash.h** from MySQL repo
     into your `mysql` include directory.

 * The headers of the boost libraries (http://www.boost.org).
   You will need at least the any.hpp.
   If boost_unit_test_framework is found, tests will be built.

 * You (likely) will need to review and edit the contents of Logging.h
   and SlaveStats.h
   These headers contain the compile-time configuration of the logging
   and monitoring subsystems.
   The provided defaults are sub-standard and an example only.

Usage requirements
-------------------------------------------------------------------
 * Requires >= MySQL 5.1.23 and <= MySQL 5.7.12. Tested with some of the 5.1, 5.5, 5.6, 5.7
   versions of mysql servers.
 * Requires rights `REPLICATION SLAVE` and `REPLICATION CLIENT`, and `SELECT` for tables being used.

Compiling
-------------------------------------------------------------------

Create directory `build` in source tree, step into it and run
`cmake .. -DCMAKE_BUILD_TYPE=Release`.
Then, if configure step is complete, run `make`.

Review and edit Logging.h and SlaveStats.h to interface the library to
your app's logging and monitoring subsystems.

You can type "make test" inside of "build" directory to run tests. You
will need a working mysql server for it. Settings of mysql connection
can be adjusted in test/data/mysql.conf. Type "ctest -V" if something
went wrong and you need see test output.

Using the library
-------------------------------------------------------------------

Please see the examples in 'test/'.

You can find the programmer's API documentation on our github wiki
pages, see https://github.com/vozbu/libslave/wiki/API.

Please read this article about real project libslave usage: http://habrahabr.ru/company/mailru/blog/219015/


CREDITS
===================================================================

(c) 2011, ZAO "Begun"

https://github.com/Begun/libslave

(c) 2016, "Mail.Ru Group" LLC

https://github.com/vozbu/libslave

This library is licensed under the GNU LGPL. Please see the file LICENSE.
