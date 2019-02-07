set(FDBSERVICE_SRCS FDBService.cpp ServiceBase.cpp)

add_executable(fdbservice ${FDBSERVICE_SRCS})
#
# FIXME: This include directory is an ugly hack. We probably want to fix this
# as soon as we get rid of the old build system
target_include_directories(fdbservice PRIVATE ${CMAKE_BINARY_DIR}/fdbclient)

install(TARGETS fdbservice DESTINATION "${FDB_LIB_NOSUFFIX}/foundationdb" COMPONENT server)
