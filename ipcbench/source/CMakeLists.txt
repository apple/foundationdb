add_executable(ipcbench ipc.cpp)
target_link_libraries(ipcbench PUBLIC boost_target)
target_link_libraries(ipcbench PRIVATE librt.so)
target_link_libraries(ipcbench PUBLIC Threads::Threads)

add_executable(producer producer.cpp)
target_link_libraries(producer PUBLIC boost_target Threads::Threads)
target_link_libraries(producer PRIVATE librt.so)

add_executable(consumer consumer.cpp)
target_link_libraries(consumer PUBLIC boost_target Threads::Threads)
target_link_libraries(consumer PRIVATE librt.so)