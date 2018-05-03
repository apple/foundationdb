set(FDBMONITOR_SRCS ConvertUTF.h SimpleIni.h fdbmonitor.cpp)

add_executable(fdbmonitor ${FDBMONITOR_SRCS})
target_link_libraries(fdbmonitor flow)

install(TARGETS fdbmonitor DESTINATION lib/foundationdb COMPONENT server)
