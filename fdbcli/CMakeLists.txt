set(FDBCLI_SRCS
  fdbcli.actor.cpp
  FlowLineNoise.actor.cpp
  FlowLineNoise.h
  linenoise/linenoise.c
  linenoise/linenoise.h)

actor_set(FDBCLI_BUILD "${FDBCLI_SRCS}")
add_executable(fdbcli "${FDBCLI_BUILD}")
actor_compile(fdbcli "${FDBCLI_SRCS}")
target_link_libraries(fdbcli PRIVATE fdbclient)

install(TARGETS fdbcli DESTINATION ${FDB_BIN_DIR} COMPONENT clients)
