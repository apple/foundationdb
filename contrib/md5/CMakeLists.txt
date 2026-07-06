add_library(md5 STATIC md5.c)
set_target_properties(md5 PROPERTIES C_CLANG_TIDY "")
target_include_directories(md5 PUBLIC "${CMAKE_CURRENT_SOURCE_DIR}/include")
