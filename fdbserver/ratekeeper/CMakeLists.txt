fdb_find_sources(FDBSERVER_RATEKEEPER_SRCS)

add_flow_target(STATIC_LIBRARY NAME fdbserver_ratekeeper SRCS ${FDBSERVER_RATEKEEPER_SRCS})
