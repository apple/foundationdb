set(FDBBACKUP_SRCS
  backup.actor.cpp)

add_flow_target(EXECUTABLE NAME fdbbackup SRCS ${FDBBACKUP_SRCS})
target_link_libraries(fdbbackup PRIVATE fdbclient)

if(NOT OPEN_FOR_IDE)
  fdb_install(TARGETS fdbbackup DESTINATION bin COMPONENT clients)
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
endif()
