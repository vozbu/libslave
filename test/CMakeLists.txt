INCLUDE_DIRECTORIES ("${CMAKE_SOURCE_DIR}")

ADD_EXECUTABLE (test_client test.cpp)
TARGET_LINK_LIBRARIES (test_client slave_a -lz -ldl -lpthread ${Boost_SYSTEM_LIBRARY})

ADD_EXECUTABLE (db_filler db_filler.cpp)
TARGET_LINK_LIBRARIES (db_filler slave_a -lz -ldl -lpthread)

IF (Boost_FOUND)
    ADD_EXECUTABLE (unit_test unit_test.cpp)
    TARGET_LINK_LIBRARIES (unit_test slave_a ${Boost_UNIT_TEST_FRAMEWORK_LIBRARY} ${Boost_THREAD_LIBRARY} ${Boost_SYSTEM_LIBRARY} -lz -ldl)
    ADD_TEST (NAME unit_test COMMAND unit_test WORKING_DIRECTORY ${CMAKE_SOURCE_DIR})
ENDIF (Boost_FOUND)
