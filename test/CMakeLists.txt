INCLUDE_DIRECTORIES ("${CMAKE_SOURCE_DIR}")

ADD_EXECUTABLE (test_client test.cpp)
TARGET_LINK_LIBRARIES (test_client slave)

ADD_EXECUTABLE (db_filler db_filler.cpp)
TARGET_LINK_LIBRARIES (db_filler slave)

IF (Boost_FOUND)
    ADD_EXECUTABLE (unit_test unit_test.cpp)
    TARGET_LINK_LIBRARIES (unit_test slave ${Boost_UNIT_TEST_FRAMEWORK_LIBRARY} ${Boost_SYSTEM_LIBRARY})
    ADD_TEST (NAME unit_test COMMAND unit_test WORKING_DIRECTORY ${CMAKE_SOURCE_DIR})
ENDIF (Boost_FOUND)
