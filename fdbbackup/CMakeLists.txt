set(FDBBACKUP_SRCS
  backup.actor.cpp)

actor_set(FDBBACKUP_BUILD "${FDBBACKUP_SRCS}")
add_executable(fdbbackup "${FDBBACKUP_BUILD}")
actor_compile(fdbbackup "${FDBBACKUP_SRCS}")
target_link_libraries(fdbbackup PRIVATE fdbclient)
