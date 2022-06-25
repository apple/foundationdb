set(FDBSERVICE_SRCS FDBService.cpp ServiceBase.cpp)

add_executable(fdbmonitor ${FDBSERVICE_SRCS})
target_include_directories(fdmonitor ${CMAKE_SOURCE_DIR})
