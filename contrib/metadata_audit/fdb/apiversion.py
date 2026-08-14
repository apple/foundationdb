# This value is only used as an upper bound check before querying the C library.
# The actual max API version is determined at runtime from the loaded libfdb_c.
# Set high enough to not block any current or near-future FDB version.
LATEST_API_VERSION = 740
FDB_VERSION = "7.3"
