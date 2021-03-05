#!/bin/bash
for i in {0..8}; do mkdir -p /mnt/nvme/nvme3/ddi/450${i}; done
rm -rf /mnt/nvme/nvme3/ddi/45*/*

pkill -9 -f fdbserver
sleep 1
io="--knob_enable_io_uring true"

export LD_LIBRARY_PATH=/mnt/nvme/nvme0/uringdb/liburing/src
/dev/shm/fdb-build/bin/fdbserver -c stateless -C /home/ddi/fdb-official/fdb.cluster -p auto:4500 --listen_address public --knob_page_cache_4k 104857600 --datadir=/mnt/nvme/nvme3/ddi/4500 --logdir=/mnt/nvme/nvme3/ddi/4500 $io &


#/dev/shm/fdb-build/bin/fdbserver -c log -C /home/ddi/fdb-official/fdb.cluster -p auto:4501 --listen_address public --knob_page_cache_4k 104857600 --datadir=/mnt/nvme/nvme3/ddi/4501 --logdir=/mnt/nvme/nvme3/ddi/4501 &


/dev/shm/fdb-build/bin/fdbserver -c log  -C /home/ddi/fdb-official/fdb.cluster -p auto:4502 --listen_address public --knob_page_cache_4k 104857600 --datadir=/mnt/nvme/nvme3/ddi/4502 --logdir=/mnt/nvme/nvme3/ddi/4502 $io &

/dev/shm/fdb-build/bin/fdbserver -c storage -C /home/ddi/fdb-official/fdb.cluster -p auto:4503 --listen_address public --knob_page_cache_4k 104857600 --datadir=/mnt/nvme/nvme3/ddi/4503 --logdir=/mnt/nvme/nvme3/ddi/4503 $io &

/dev/shm/fdb-build/bin/fdbserver -c storage -C /home/ddi/fdb-official/fdb.cluster -p auto:4504 --listen_address public --knob_page_cache_4k 104857600 --datadir=/mnt/nvme/nvme3/ddi/4504 --logdir=/mnt/nvme/nvme3/ddi/4504 $io &


/dev/shm/fdb-build/bin/fdbcli -C /home/ddi/fdb-official/fdb.cluster --exec "configure new single ssd-2"
