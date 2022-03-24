set(FDBCLI_SRCS
  fdbcli.actor.cpp
  fdbcli.actor.h
  AdvanceVersionCommand.actor.cpp
  BlobRangeCommand.actor.cpp
  CacheRangeCommand.actor.cpp
  ConfigureCommand.actor.cpp
  ConsistencyCheckCommand.actor.cpp
  CoordinatorsCommand.actor.cpp
  DataDistributionCommand.actor.cpp
  ExcludeCommand.actor.cpp
  ExpensiveDataCheckCommand.actor.cpp
  FileConfigureCommand.actor.cpp
  FlowLineNoise.actor.cpp
  FlowLineNoise.h
  ForceRecoveryWithDataLossCommand.actor.cpp
  IncludeCommand.actor.cpp
  KillCommand.actor.cpp
  LockCommand.actor.cpp
  ChangeFeedCommand.actor.cpp
  MaintenanceCommand.actor.cpp
  ProfileCommand.actor.cpp
  SetClassCommand.actor.cpp
  SnapshotCommand.actor.cpp
  StatusCommand.actor.cpp
  SuspendCommand.actor.cpp
  TenantCommands.actor.cpp
  ThrottleCommand.actor.cpp
  TriggerDDTeamInfoLogCommand.actor.cpp
  TssqCommand.actor.cpp
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
