CMAKE_MINIMUM_REQUIRED (VERSION 3.1.3)
PROJECT (libslave)

OPTION (BUILD_STATIC "Force building static library" OFF)

# Build flags
SET (CMAKE_CXX_STANDARD 11)
SET (CMAKE_CXX_STANDARD_REQUIRED TRUE)
SET (CMAKE_CXX_EXTENSIONS FALSE)

IF (CMAKE_CXX_COMPILER_ID STREQUAL "GNU"
 OR CMAKE_CXX_COMPILER_ID STREQUAL "Clang")
    # Common options
    ADD_DEFINITIONS (-pipe)
    ADD_DEFINITIONS (-Wall)
ENDIF ()

FIND_PACKAGE (Boost REQUIRED)
INCLUDE_DIRECTORIES (${Boost_INCLUDE_DIRS})
FIND_PACKAGE (Boost 1.41.0 COMPONENTS unit_test_framework system)
# Fixes a lot of annoying warnings about auto_ptr deprecation
ADD_DEFINITIONS (-DBOOST_NO_AUTO_PTR)

INCLUDE (FindPackageHandleStandardArgs)
FIND_PATH (IMYSQL mysql/mysql.h)
FIND_LIBRARY (LMYSQLCLIENT NAMES mysqlclient_r
                                 mysqlclient
                           PATH_SUFFIXES mysql)
SET (Mysql_FIND_REQUIRED YES)
FIND_PACKAGE_HANDLE_STANDARD_ARGS(Mysql DEFAULT_MSG IMYSQL LMYSQLCLIENT)
INCLUDE_DIRECTORIES ("${IMYSQL}/mysql")

# Check whether we've found static or dynamic library
STRING (REGEX MATCH ".*\\.a$" MYSQL_IS_STATIC ${LMYSQLCLIENT})
IF (MYSQL_IS_STATIC STREQUAL LMYSQLCLIENT)
    SET (LINK_TYPE STATIC)
    # You should specify this according to your static mysql library configuration.
    # Try to use mysql_config --libs or pkg-config.
    SET (MYSQL_LIBS ${LMYSQLCLIENT} -lssl -lcrypto -ldl -lz)
ELSE ()
    SET (LINK_TYPE SHARED)
    SET (MYSQL_LIBS ${LMYSQLCLIENT})
ENDIF ()

MESSAGE (STATUS "Found ${LINK_TYPE} mysql library")
IF (BUILD_STATIC)
    SET (LINK_TYPE STATIC)
ENDIF ()
MESSAGE (STATUS "Build ${LINK_TYPE} slave library")

FILE (GLOB HDR "*.h")
INSTALL (FILES ${HDR} DESTINATION include)
AUX_SOURCE_DIRECTORY (${CMAKE_SOURCE_DIR} SRC)

# Most probably static mysql is built without fPIC, so, we can't build dynamic library with it
ADD_LIBRARY (slave ${LINK_TYPE} ${SRC})
TARGET_LINK_LIBRARIES (slave ${MYSQL_LIBS} -lpthread)
INSTALL (TARGETS slave DESTINATION lib64)

IF (Boost_FOUND)
    ENABLE_TESTING ()
ENDIF (Boost_FOUND)
ADD_SUBDIRECTORY (test)
