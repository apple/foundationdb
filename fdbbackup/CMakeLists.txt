set(FDBBACKUP_SRCS
  backup.actor.cpp)

add_flow_target(EXECUTABLE NAME fdbbackup SRCS ${FDBBACKUP_SRCS})
target_link_libraries(fdbbackup PRIVATE fdbclient)

set(FDBCONVERT_SRCS
	FileConverter.actor.cpp
	FileConverter.h)
add_flow_target(EXECUTABLE NAME fdbconvert SRCS ${FDBCONVERT_SRCS})
target_link_libraries(fdbconvert PRIVATE fdbclient)

set(FDBDECODE_SRCS
	FileDecoder.actor.cpp
	FileConverter.h)
add_flow_target(EXECUTABLE NAME fdbdecode SRCS ${FDBDECODE_SRCS})
target_link_libraries(fdbdecode PRIVATE fdbclient)

if(NOT OPEN_FOR_IDE)
  if(GENERATE_DEBUG_PACKAGES)
    fdb_install(TARGETS fdbbackup DESTINATION bin COMPONENT clients)
  else()
    add_custom_target(prepare_fdbbackup_install ALL DEPENDS strip_only_fdbbackup)
    fdb_install(PROGRAMS ${CMAKE_BINARY_DIR}/packages/bin/fdbbackup DESTINATION bin COMPONENT clients)
  endif()
  install_symlink(
    COMPONENT clients
    FILE_DIR bin
    LINK_DIR fdbmonitor
    FILE_NAME fdbbackup
    LINK_NAME backup_agent/backup_agent)
  install_symlink(
    COMPONENT clients
    FILE_DIR bin
    LINK_DIR bin
    FILE_NAME fdbbackup
    LINK_NAME fdbrestore)
  install_symlink(
    COMPONENT clients
    FILE_DIR bin
    LINK_DIR bin
    FILE_NAME fdbbackup
    LINK_NAME dr_agent)
  install_symlink(
    COMPONENT clients
    FILE_DIR bin
    LINK_DIR bin
    FILE_NAME fdbbackup
    LINK_NAME fdbdr)
  symlink_files(
    LOCATION packages/bin
    SOURCE fdbbackup
    TARGETS fdbdr dr_agent backup_agent fdbrestore fastrestore_tool)
  symlink_files(
    LOCATION bin
    SOURCE fdbbackup
    TARGETS fdbdr dr_agent backup_agent fdbrestore fastrestore_tool)
endif()

if (GPERFTOOLS_FOUND)
  add_compile_definitions(USE_GPERFTOOLS)
  target_link_libraries(fdbbackup PRIVATE gperftools)
endif()
