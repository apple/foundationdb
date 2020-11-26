set(SRCS pthread/thread.cpp
  tss_null.cpp
  future.cpp)

if(WIN32)
  list(APPEND SRCS win32/thread.cpp
    win32/thread_primitives.cpp
    win32/tss_dll.cpp
    win32/tss_pe.cpp)
else()
  list(APPEND SRCS pthread/once_atomic.cpp
    pthread/once.cpp)
endif()

add_library(boost_thread STATIC ${SRCS})
target_link_libraries(boost_thread PUBLIC boost_target)