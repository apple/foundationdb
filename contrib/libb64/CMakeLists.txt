add_library(libb64 STATIC cdecode.c cencode.c)
set_target_properties(libb64 PROPERTIES C_CLANG_TIDY "")
target_include_directories(libb64 PUBLIC "${CMAKE_CURRENT_SOURCE_DIR}/include")
