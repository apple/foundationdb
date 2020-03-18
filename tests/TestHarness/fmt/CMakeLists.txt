add_library(fmt STATIC posix.cc format.cc)
target_include_directories(fmt PUBLIC include)
