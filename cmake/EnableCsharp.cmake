if(WIN32)
  # C# is currently only supported on Windows.
  # On other platforms we find mono manually
  enable_language(CSharp)
else()
  # for other platforms we currently use mono

  find_program(MONO_EXECUTABLE mono)
  if (NOT MONO_EXECUTABLE)
    message(FATAL_ERROR "Could not find 'mono' executable!")
  endif()

  find_program(MCS_EXECUTABLE mcs)
  if (NOT MCS_EXECUTABLE)
    message(FATAL_ERROR "Could not find 'mcs' executable, which is part of Mono!")
  endif()
endif()
