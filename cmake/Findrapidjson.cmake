# Findrapidjson

find_path(RAPIDJSON_INCLUDE_DIR
  NAMES rapidjson/rapidjson.h
  PATH_SUFFIXES include)

find_package_handle_standard_args(rapidjson
  DEFAULT_MSG RAPIDJSON_INCLUDE_DIR)
