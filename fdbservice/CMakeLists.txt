set(FDBSERVICE_SRCS FDBService.cpp ServiceBase.cpp)
add_executable(fdbmonitor ${FDBSERVICE_SRCS})

get_target_property(fdbclient_target_includes fdbclient INCLUDE_DIRECTORIES)
target_link_libraries(fdbmonitor PUBLIC SimpleOpt)
target_include_directories(fdbmonitor PUBLIC "${fdbclient_target_includes}")
