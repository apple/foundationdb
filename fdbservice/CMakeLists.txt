set(FDBSERVICE_SRCS FDBService.cpp ServiceBase.cpp)

add_executable(fdbmonitor ${FDBSERVICE_SRCS})
#
# FIXME: This include directory is an ugly hack. We probably want to fix this
# as soon as we get rid of the old build system
target_include_directories(fdbmonitor PRIVATE ${CMAKE_BINARY_DIR}/fdbclient)
