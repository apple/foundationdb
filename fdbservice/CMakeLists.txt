set(FDBSERVICE_SRCS FDBService.cpp ServiceBase.cpp)

add_executable(fdbmonitor ${FDBSERVICE_SRCS})

target_include_directories(fdbmonitor PRIVATE ${CMAKE_BINARY_DIR}/flow/include ${CMAKE_BINARY_DIR}/fdbclient/include)
add_dependencies(fdbmonitor fdbclient)