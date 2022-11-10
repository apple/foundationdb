set(SRCS
  AST.cpp
  CodeGenerator.h
  Compiler.h
  Config.h
  Error.h
  FlatbuffersTypes.h
  Parser.h
  StaticContext.h
  AST.h
  CodeGenerator.cpp
  Compiler.cpp
  Config.cpp
  Error.cpp
  Parser.cpp
  StaticContext.cpp)

add_executable(flowserializer ${SRCS})
target_link_libraries(flowserializer PRIVATE boost_target fmt::fmt)