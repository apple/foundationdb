set(SRCS
  FDBLibTLSPlugin.cpp
  FDBLibTLSPlugin.h
  FDBLibTLSPolicy.cpp
  FDBLibTLSPolicy.h
  FDBLibTLSSession.cpp
  FDBLibTLSSession.h
  FDBLibTLSVerify.cpp
  FDBLibTLSVerify.h)

add_library(FDBLibTLS STATIC ${SRCS})
target_link_libraries(FDBLibTLS PUBLIC OpenSSL::SSL boost_target PRIVATE flow)
