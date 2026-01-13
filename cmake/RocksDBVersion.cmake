# RocksDB Version Configuration
# ==============================
# Edit this file to change the RocksDB version used by FDB.
#
# Choose ONE of the two options below by uncommenting the appropriate section.
# Do NOT set both - CMake will error if both are configured.
#
# CMake will automatically generate fdbserver/include/fdbserver/FDBRocksDBVersion.h
# based on the version specified here. Do NOT edit that file manually.
#
# To get SHA256:
#   curl -sL https://github.com/facebook/rocksdb/archive/<ref>.tar.gz | sha256sum
#   (where <ref> is either a version tag like "v9.7.3" or a commit hash)

###############################################################################
# OPTION 1: Official Release (DEFAULT - currently active)
# Use this for stable, released versions of RocksDB.
###############################################################################
set(ROCKSDB_VERSION "9.7.3")
set(ROCKSDB_VERSION_SHA256 "acfabb989cbfb5b5c4d23214819b059638193ec33dad2d88373c46448d16d38b")

###############################################################################
# OPTION 2: Git Commit
# Use this for unreleased features/fixes. Uncomment and fill in, then comment
# out Option 1 above.
#
# Note: CMake will auto-fetch the version from GitHub at configure time.
# This requires network access during cmake configure.
###############################################################################
# set(ROCKSDB_GIT_HASH "")
# set(ROCKSDB_GIT_HASH_SHA256 "")
