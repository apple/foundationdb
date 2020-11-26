# Taken from https://github.com/boost-cmake/boost

add_library(boost_context 
dummy.cpp
continuation.cpp
fiber.cpp
)

target_link_libraries(boost_context PUBLIC boost_thread)

enable_language(ASM)
set(abi
  aapcs
  eabi
  ms
  n32
  n64
  o32
  o64
  sysv
)

set(binary_format
  elf
  macho
  pe
)

set(arch
  arm
  arm64
  mips32
  ppc32
  ppc64
  i386
  x86_64
)

if(WIN32)
  set(default_abi ms)
elseif("${CMAKE_SYSTEM_PROCESSOR}" STREQUAL "arm")
  set(default_abi aapcs)
elseif("${CMAKE_SYSTEM_PROCESSOR}" STREQUAL "mips")
  set(default_abi o32)
else()
  set(default_abi sysv)
endif()
  
if(APPLE)
  set(default_binary_format macho)
elseif(WIN32)
  set(default_binary_format pe)
else()
  set(default_binary_format elf)
endif()

if("${CMAKE_SYSTEM_PROCESSOR}" STREQUAL "arm")
  if(CMAKE_SIZEOF_VOID_P EQUAL 4)
    set(default_arch arm)
  else()
    set(default_arch arm64)
  endif()
elseif("${CMAKE_SYSTEM_PROCESSOR}" STREQUAL "mips")
  set(default_arch mips32)
else()
  if(CMAKE_SIZEOF_VOID_P EQUAL 4)
    set(default_arch i386)
  else()
    set(default_arch x86_64)
  endif()
endif()

set(BOOST_CONTEXT_ABI ${default_abi}
  CACHE STRING "ABI for Boost.Context (${abi})"
  )
set_property(CACHE BOOST_CONTEXT_ABI
  PROPERTY STRINGS "${abi}"
  )

set(BOOST_CONTEXT_BINARY_FORMAT ${default_binary_format}
  CACHE STRING "Binary format for Boost.Context (${binary_format})"
  )
set_property(CACHE BOOST_CONTEXT_BINARY_FORMAT
  PROPERTY STRINGS "${binary_format}"
  )


set(BOOST_CONTEXT_ARCHITECTURE ${default_arch}
  CACHE STRING "Architecture for Boost.Context (${arch})"
  )
set_property(CACHE BOOST_CONTEXT_ARCHITECTURE
  PROPERTY STRINGS "${arch}"
  )

if(MSVC)
  set(asm_format "masm.asm")
else()
  set(asm_format "gas.S")
endif()
if(NOT APPLE OR NOT CMAKE_SYSTEM_PROCESSOR STREQUAL "arm64")
  target_sources(boost_context PRIVATE 
    asm/make_${BOOST_CONTEXT_ARCHITECTURE}_${BOOST_CONTEXT_ABI}_${BOOST_CONTEXT_BINARY_FORMAT}_${asm_format}
    asm/jump_${BOOST_CONTEXT_ARCHITECTURE}_${BOOST_CONTEXT_ABI}_${BOOST_CONTEXT_BINARY_FORMAT}_${asm_format}
    asm/ontop_${BOOST_CONTEXT_ARCHITECTURE}_${BOOST_CONTEXT_ABI}_${BOOST_CONTEXT_BINARY_FORMAT}_${asm_format}
)
endif()

if(WIN32)
  target_sources(boost_context PRIVATE windows/stack_traits.cpp)
else()
  target_sources(boost_context PRIVATE posix/stack_traits.cpp)
endif()

find_path(UCONTEXT_INCLUDE_DIR ucontext.h)

if(UCONTEXT_INCLUDE_DIR AND UCONTEXT_INCLUDE_DIR)
target_include_directories(boost_context PRIVATE ${UCONTEXT_INCLUDE_DIR})
target_compile_definitions(boost_context PRIVATE -DBOOST_USE_UCONTEXT=1)
endif()