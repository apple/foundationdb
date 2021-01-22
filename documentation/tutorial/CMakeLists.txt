set(TUTORIAL_SRCS tutorial.actor.cpp)

add_flow_target(EXECUTABLE NAME tutorial SRCS "${TUTORIAL_SRCS}")
target_link_libraries(tutorial PUBLIC fdbclient)
