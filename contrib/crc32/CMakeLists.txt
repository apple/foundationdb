add_library(crc32 STATIC crc32.S crc32_wrapper.c crc32c.cpp)
target_include_directories(crc32 PUBLIC "${CMAKE_CURRENT_SOURCE_DIR}/include")
