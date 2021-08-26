set(FDBCLI_SRCS
  fdbcli.actor.cpp
  fdbcli.actor.h
  AdvanceVersionCommand.actor.cpp
  CacheRangeCommand.actor.cpp
  ConsistencyCheckCommand.actor.cpp
  DataDistributionCommand.actor.cpp
  ExpensiveDataCheckCommand.actor.cpp
  FlowLineNoise.actor.cpp
  FlowLineNoise.h
  ForceRecoveryWithDataLossCommand.actor.cpp
  KillCommand.actor.cpp
  MaintenanceCommand.actor.cpp
  SetClassCommand.actor.cpp
  SnapshotCommand.actor.cpp
  SuspendCommand.actor.cpp
  ThrottleCommand.actor.cpp
  Util.actor.cpp
  linenoise/linenoise.h)

if(NOT WIN32)
  list(APPEND FDBCLI_SRCS linenoise/linenoise.c)
endif()

add_flow_target(EXECUTABLE NAME fdbcli SRCS ${FDBCLI_SRCS})
target_link_libraries(fdbcli PRIVATE fdbclient)

if(NOT OPEN_FOR_IDE)
  if(GENERATE_DEBUG_PACKAGES)
    fdb_install(TARGETS fdbcli DESTINATION bin COMPONENT clients)
  else()
    add_custom_target(prepare_fdbcli_install ALL DEPENDS strip_only_fdbcli)
    fdb_install(PROGRAMS ${CMAKE_BINARY_DIR}/packages/bin/fdbcli DESTINATION bin COMPONENT clients)
  endif()
endif()
