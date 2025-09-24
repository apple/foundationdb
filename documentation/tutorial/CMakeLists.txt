set(TUTORIAL_SRCS tutorial.actor.cpp)

add_flow_target(EXECUTABLE NAME tutorial SRCS "${TUTORIAL_SRCS}")
target_link_libraries(tutorial PUBLIC fdbclient)

# Small exercise
set(PRINT_IN_ORDER_SRCS print_in_order.actor.cpp)
add_flow_target(EXECUTABLE NAME print_in_order SRCS "${PRINT_IN_ORDER_SRCS}")
target_link_libraries(print_in_order PUBLIC fdbclient)

# Medium exercise
set(MAKE_H2O_SRCS make_h2o.actor.cpp)
add_flow_target(EXECUTABLE NAME make_h2o SRCS "${MAKE_H2O_SRCS}")
target_link_libraries(make_h2o PUBLIC fdbclient)

# Extra medium exercise
set(DP_SRCS dining_philosophers.actor.cpp)
add_flow_target(EXECUTABLE NAME dining_philosophers SRCS "${DP_SRCS}")
target_link_libraries(dining_philosophers PUBLIC fdbclient)

# Playground to learn flow
set(PLAY_SRC play.actor.cpp)
add_flow_target(EXECUTABLE NAME play SRCS "${PLAY_SRC}")
target_link_libraries(play PUBLIC fdbclient)
