#string(RANDOM LENGTH 8 ALPHABET "0123456789abcdef" SEED_)
#set(SEED "0x${SEED_}" CACHE STRING "Random seed for testing")
set(SEED "1")
FILE(WRITE ${CMAKE_CURRENT_BINARY_DIR}/seed "${SEED}")

add_custom_command(
    OUTPUT ${CMAKE_CURRENT_BINARY_DIR}/table_fdbflatbuffers.h
    COMMAND python3 ${CMAKE_CURRENT_SOURCE_DIR}/flat_buffers_fuzz.py "${SEED}" cpp > ${CMAKE_CURRENT_BINARY_DIR}/table_fdbflatbuffers.h
    DEPENDS ${CMAKE_CURRENT_SOURCE_DIR}/flat_buffers_fuzz.py ${CMAKE_CURRENT_BINARY_DIR}/seed
    COMMENT "Generate test FDB flatbuffers serialization code")
add_custom_target(table_fdbflatbuffers DEPENDS ${CMAKE_CURRENT_BINARY_DIR}/table_fdbflatbuffers.h)

add_custom_command(
    OUTPUT ${CMAKE_CURRENT_BINARY_DIR}/table.fbs
    COMMAND python3 ${CMAKE_CURRENT_SOURCE_DIR}/flat_buffers_fuzz.py "${SEED}" fbs > ${CMAKE_CURRENT_BINARY_DIR}/table.fbs
    DEPENDS ${CMAKE_CURRENT_SOURCE_DIR}/flat_buffers_fuzz.py ${CMAKE_CURRENT_BINARY_DIR}/seed
    COMMENT "Generate test flatbuffers schema")
add_custom_target(tablefbs DEPENDS ${CMAKE_CURRENT_BINARY_DIR}/table.fbs)

add_custom_command(
    OUTPUT ${CMAKE_CURRENT_BINARY_DIR}/table_generated.h
    COMMAND ${FLATC} --cpp ${CMAKE_CURRENT_BINARY_DIR}/table.fbs
    DEPENDS flatbuffers tablefbs ${CMAKE_CURRENT_BINARY_DIR}/seed
    COMMENT "Generate test Google flatbuffers serialization code")
add_custom_target(table_flatbuffers DEPENDS ${CMAKE_CURRENT_BINARY_DIR}/table_generated.h)

add_custom_command(
    OUTPUT ${CMAKE_CURRENT_BINARY_DIR}/table.h ${CMAKE_CURRENT_BINARY_DIR}/table.cpp
    COMMAND $<TARGET_FILE:flowserializer> ${CMAKE_CURRENT_BINARY_DIR}/table.fbs
    DEPENDS flowserializer tablefbs ${CMAKE_CURRENT_BINARY_DIR}/seed
    COMMENT "Generate test flowbuffers serialization code")
add_custom_target(table_flowbuffers DEPENDS ${CMAKE_CURRENT_BINARY_DIR}/table.h ${CMAKE_CURRENT_BINARY_DIR}/table.cpp)

add_executable(flat_buffers_fuzz
    ${CMAKE_CURRENT_SOURCE_DIR}/flat_buffers_fuzz.cpp
    ${CMAKE_CURRENT_BINARY_DIR}/table.cpp)
target_link_libraries(flat_buffers_fuzz flow boost_target Threads::Threads doctest)
target_include_directories(flat_buffers_fuzz PRIVATE
    ${FLATBUFFERS_INCLUDE}
    ${CMAKE_SOURCE_DIR}/flow/flowserializer
    ${CMAKE_CURRENT_BINARY_DIR})
add_dependencies(flat_buffers_fuzz flatbuffers)
add_dependencies(flat_buffers_fuzz table_fdbflatbuffers)
add_dependencies(flat_buffers_fuzz table_flatbuffers)
add_dependencies(flat_buffers_fuzz table_flowbuffers)

if (NOT OPEN_FOR_IDE)
    add_test(
        NAME unittests/flat_buffers_fuzz
        COMMAND $<TARGET_FILE:flat_buffers_fuzz>)
endif()
