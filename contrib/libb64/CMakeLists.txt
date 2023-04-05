add_library(libb64 STATIC cdecode.c cencode.c)
target_include_directories(libb64 PUBLIC "${CMAKE_CURRENT_SOURCE_DIR}/include")
