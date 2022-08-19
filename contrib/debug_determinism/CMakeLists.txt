add_library(debug_determinism STATIC debug_determinism.cpp)

# So that we can link to libfdb_c.so. Not strictly necessary but convenient for use with our
# TRACE_PC_GUARD_INSTRUMENTATION_LIB cmake option
target_compile_options(debug_determinism PRIVATE -fPIC)