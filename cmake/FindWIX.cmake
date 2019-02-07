# Find WIX

set(WIX_INSTALL_DIR $ENV{WIX})

find_program(WIX_CANDLE
  candle
  HINTS ${WIX_INSTALL_DIR}/bin)

find_program(WIX_LIGHT
  light
  HINTS ${WIX_INSTALL_DIR}/bin)

find_package_handle_standard_args(WIX
  REQUIRED_VARS
    WIX_CANDLE
    WIX_LIGHT
  FAIL_MESSAGE
    "Could not find WIX installation - try setting WIX_ROOT or the WIX environment variable")
