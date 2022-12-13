string(RANDOM LENGTH 8 ALPHABET "0123456789abcdef" SEED_)
set(SEED "0x${SEED_}" CACHE STRING "Random seed for testing")
#set(SEED "9")
FILE(WRITE ${CMAKE_CURRENT_BINARY_DIR}/seed "${SEED}")

add_custom_command(
    OUTPUT ${CMAKE_CURRENT_BINARY_DIR}/table.h
    COMMAND python3 ${CMAKE_CURRENT_SOURCE_DIR}/flat_buffers_fuzz.py "${SEED}" cpp > ${CMAKE_CURRENT_BINARY_DIR}/table.h
    DEPENDS ${CMAKE_CURRENT_SOURCE_DIR}/flat_buffers_fuzz.py ${CMAKE_CURRENT_BINARY_DIR}/seed
    COMMENT "Generate flat buffer schema")
add_custom_target(tableh DEPENDS ${CMAKE_CURRENT_BINARY_DIR}/table.h)

add_custom_command(
    OUTPUT ${CMAKE_CURRENT_BINARY_DIR}/table.fbs
    COMMAND python3 ${CMAKE_CURRENT_SOURCE_DIR}/flat_buffers_fuzz.py "${SEED}" fbs > ${CMAKE_CURRENT_BINARY_DIR}/table.fbs
    DEPENDS ${CMAKE_CURRENT_SOURCE_DIR}/flat_buffers_fuzz.py ${CMAKE_CURRENT_BINARY_DIR}/seed
    COMMENT "Generate flat buffer schema")
add_custom_target(tablefbs DEPENDS ${CMAKE_CURRENT_BINARY_DIR}/table.fbs)

add_custom_command(
    OUTPUT ${CMAKE_CURRENT_BINARY_DIR}/table_generated.h
    COMMAND ${FLATC} --cpp ${CMAKE_CURRENT_BINARY_DIR}/table.fbs
    DEPENDS flatbuffers tablefbs ${CMAKE_CURRENT_BINARY_DIR}/seed
    COMMENT "Generate flat buffer schema")
add_custom_target(tablegenerated DEPENDS ${CMAKE_CURRENT_BINARY_DIR}/table_generated.h)

add_flow_target(EXECUTABLE NAME flat_buffers_fuzz SRCS flat_buffers_fuzz.cpp)
target_link_libraries(flat_buffers_fuzz flow boost_target Threads::Threads doctest)
target_include_directories(flat_buffers_fuzz PRIVATE ${FLATBUFFERS_INCLUDE} ${CMAKE_CURRENT_BINARY_DIR})
add_dependencies(flat_buffers_fuzz tableh)
add_dependencies(flat_buffers_fuzz tablegenerated)
add_dependencies(flat_buffers_fuzz flatbuffers)

add_test(
    NAME unittests/flat_buffers_fuzz
    COMMAND $<TARGET_FILE:flat_buffers_fuzz> ${SEED})
