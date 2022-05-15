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
target_link_libraries(FDBLibTLS PUBLIC OpenSSL::SSL PRIVATE flow)
target_include_directories(FDBLibTLS INTERFACE OpenSSL::SSL PRIVATE flow)
