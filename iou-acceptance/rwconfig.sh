FDBCLI="/mnt/nvme/nvme0/uringdb/bld/bin/fdbcli"
FDBSERVER="/mnt/nvme/nvme0/uringdb/bld/bin/fdbserver"
LIB="/mnt/nvme/nvme0/uringdb/liburing/src"
#use .stub for the stub and .txt for the test
TEST="/mnt/nvme/nvme0/uringdb/tests/RW"
CLS="/home/ddi/fdb-official/fdb.cluster"
#device on which  the data and log path are mounted (used for io stat collection)
MOUNT_POINT="/mnt/nvme/nvme1"
DEV="nvme1n1"
DATALOGPATH=${MOUNT_POINT}"/ioutest"
FILEPATH=${MOUNT_POINT}"/testfiles"
PAGE_CACHE="10"  #MiB
RESULTS=`date +%Y-%m-%d_%H-%M-%S`
hn=$(hostname)
RESULTS="${RESULTS}-${hn-}KV"
LOG_SHM=1
