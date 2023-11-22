add_flow_target(EXECUTABLE NAME coro_tutorial SRCS tutorial.cpp)
target_link_libraries(coro_tutorial PUBLIC fdbclient)