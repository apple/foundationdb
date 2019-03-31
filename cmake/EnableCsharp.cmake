if(WIN32)
  # C# is currently only supported on Windows.
  # On other platforms we find mono manually
  enable_language(CSharp)
else()
  # for other platforms we currently use mono
  find_program(MONO_EXECUTABLE mono)
  find_program(MCS_EXECUTABLE dmcs)

  if (NOT MCS_EXECUTABLE)
    find_program(MCS_EXECUTABLE mcs)
  endif()

  set(MONO_FOUND FALSE CACHE INTERNAL "")

  if (NOT MCS_EXECUTABLE)
    find_program(MCS_EXECUTABLE mcs)
  endif()

  if (MONO_EXECUTABLE AND MCS_EXECUTABLE)
    set(MONO_FOUND True CACHE INTERNAL "")
  endif()

  if (NOT MONO_FOUND)
    message(FATAL_ERROR "Could not find mono")
  endif()
endif()
