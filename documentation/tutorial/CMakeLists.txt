set(TUTORIAL_SRCS tutorial.actor.cpp)

add_flow_target(EXECUTABLE NAME tutorial SRCS "${TUTORIAL_SRCS}")
target_link_libraries(tutorial PUBLIC fdbclient)

set(PRINT_IN_ORDER_SRCS print_in_order.actor.cpp)
add_flow_target(EXECUTABLE NAME print_in_order SRCS "${PRINT_IN_ORDER_SRCS}")
target_link_libraries(print_in_order PUBLIC fdbclient)

set(MAKE_H2O_SRCS make_h2o.actor.cpp)
add_flow_target(EXECUTABLE NAME make_h2o SRCS "${MAKE_H2O_SRCS}")
target_link_libraries(make_h2o PUBLIC fdbclient)
