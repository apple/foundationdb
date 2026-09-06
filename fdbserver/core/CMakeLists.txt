fdb_find_sources(FDBSERVER_CORE_LOCAL_SRCS)

set(FDBSERVER_CORE_SRCS ${FDBSERVER_CORE_LOCAL_SRCS})
if(${COROUTINE_IMPL} STREQUAL libcoro)
  list(APPEND FDBSERVER_CORE_SRCS ../coroimpl/CoroFlowCoro.cpp)
else()
  list(APPEND FDBSERVER_CORE_SRCS ../coroimpl/CoroFlow.cpp)
endif()

add_flow_target(STATIC_LIBRARY NAME fdbserver_core SRCS ${FDBSERVER_CORE_SRCS})
add_fdbserver_link_test(fdbserver_corelinktest
  fdbserver_core)
add_fdbserver_unit_test(fdbserver_core_test core
  fdbserver_core)

configure_fdbserver_common_includes(fdbserver_core)
target_include_directories(fdbserver_core
  PUBLIC
    ${CMAKE_CURRENT_SOURCE_DIR}/include
    ${CMAKE_CURRENT_BINARY_DIR}/include
  PRIVATE
    ${CMAKE_SOURCE_DIR}/fdbserver/include)
target_link_libraries(fdbserver_core PUBLIC fdbclient)
