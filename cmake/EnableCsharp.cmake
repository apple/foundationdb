find_program(MONO_EXECUTABLE mono)
if (NOT MONO_EXECUTABLE)
  message(FATAL_ERROR "Could not find 'mono' executable!")
endif()

find_program(MCS_EXECUTABLE mcs)
if (NOT MCS_EXECUTABLE)
  message(FATAL_ERROR "Could not find 'mcs' executable, which is part of Mono!")
endif()
