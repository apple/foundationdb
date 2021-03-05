#!/usr/bin/env bash
version=700
deploy="single"
set -x
if [[ ${version} == 700 ]];then
lb=/mnt/nvme/nvme0/uringdb/liburing/src
srv=/mnt/nvme/nvme0/uringdb/bld/bin/fdbserver
elif [[ ${version} == 620 ]];then
lb=/home/ddi/ikvb/lib/fdb/620
srv=/home/ddi/fdb_binaries_620/fdbserver
srv=/mnt/nvme/nvme0/ddi/uringdb/bld/bin/fdbserver
else
lb=/home/ddi/ikvb/lib/fdb/630
#newly compiled
srv=/mnt/nvme/nvme0/ddi/uringdb/bld/bin/fdbserver
# with iou
#srv=/home/ddi/fdb_binaries_630/fdbserver
fi
cls=/home/ddi/fdb-official/fdb.zac13
port=4500
uring="--knob_enable_io_uring true --knob_io_uring_direct_submit true --knob_io_uring_fixed_buffers true --knob_max_outstanding 16"
#       --knob_io_uring_direct_submit true"
#       --knob_max_outstanding 1024"
#flow="--knob_max_outstanding 1024 --knob_min_submit 10"


spawn(){
    pkill -9 -f fdbserver
    sleep 2
    rm -rf /mnt/nvme/nvme0/data_ddi/*
    rm -rf /mnt/nvme/nvme0/log_ddi/*

    if [[ ${deploy} == "single" ]];then
        echo "Deploying single-process fdb"
        mkdir /mnt/nvme/nvme0/data_ddi/$port
        LD_LIBRARY_PATH=${lb} gdb -ex run --args ${srv} -C ${cls} --datadir /mnt/nvme/nvme0/data_ddi/${port}  --listen_address public \
        --logdir /mnt/nvme/nvme0/log_ddi --public_address auto:${port} --knob_page_cache_4k 104857600 --storage_memory 4GB \
        --memory 16GB --knob_cache_eviction_policy random --knob_max_evict_attempts 100 ${uring} ${flow} 
    else
        echo "Deploying multi-process fdb"

        mkdir /mnt/nvme/nvme0/data_ddi/$port
        LD_LIBRARY_PATH=${lb} ${srv}  -c coordinator -C ${cls} --datadir /mnt/nvme/nvme0/data_ddi/${port}  --listen_address public \
        --logdir /mnt/nvme/nvme0/log_ddi --public_address auto:${port} --knob_page_cache_4k 104857600 --storage_memory 4GB \
        --memory 16GB --knob_cache_eviction_policy random --knob_max_evict_attempts 100 ${uring} ${flow} > /mnt/nvme/nvme0/data_ddi/${port}.txt 2>&1  &

        port=$((${port}+1))
        mkdir /mnt/nvme/nvme0/data_ddi/$port
        LD_LIBRARY_PATH=${lb} ${srv}  -c master -C ${cls} --datadir /mnt/nvme/nvme0/data_ddi/${port}  --listen_address public \
        --logdir /mnt/nvme/nvme0/log_ddi --public_address auto:${port} --knob_page_cache_4k 104857600 --storage_memory 4GB \
        --memory 16GB --knob_cache_eviction_policy random --knob_max_evict_attempts 100 ${uring} ${flow} > /mnt/nvme/nvme0/data_ddi/${port}.txt 2>&1  &

        port=$((${port}+1))
        mkdir /mnt/nvme/nvme0/data_ddi/$port
        LD_LIBRARY_PATH=${lb} ${srv}  -c cluster_controller -C ${cls} --datadir /mnt/nvme/nvme0/data_ddi/${port}  --listen_address public \
        --logdir /mnt/nvme/nvme0/log_ddi --public_address auto:${port} --knob_page_cache_4k 104857600 --storage_memory 4GB \
        --memory 16GB --knob_cache_eviction_policy random --knob_max_evict_attempts 100 ${uring} ${flow} > /mnt/nvme/nvme0/data_ddi/${port}.txt 2>&1  &

        port=$((${port}+1))
        mkdir /mnt/nvme/nvme0/data_ddi/$port
        LD_LIBRARY_PATH=${lb} ${srv}  -c data_distributor -C ${cls} --datadir /mnt/nvme/nvme0/data_ddi/${port}  --listen_address public \
        --logdir /mnt/nvme/nvme0/log_ddi --public_address auto:${port} --knob_page_cache_4k 104857600 --storage_memory 4GB \
        --memory 16GB --knob_cache_eviction_policy random --knob_max_evict_attempts 100 ${uring} ${flow} > /mnt/nvme/nvme0/data_ddi/${port}.txt 2>&1  &

        port=$((${port}+1))
        mkdir /mnt/nvme/nvme0/data_ddi/$port
        LD_LIBRARY_PATH=${lb} ${srv}  -c ratekeeper -C ${cls} --datadir /mnt/nvme/nvme0/data_ddi/${port}  --listen_address public \
        --logdir /mnt/nvme/nvme0/log_ddi --public_address auto:${port} --knob_page_cache_4k 104857600 --storage_memory 4GB \
        --memory 16GB --knob_cache_eviction_policy random --knob_max_evict_attempts 100 ${uring} ${flow} > /mnt/nvme/nvme0/data_ddi/${port}.txt 2>&1 &

        port=$((${port}+1))
        mkdir /mnt/nvme/nvme0/data_ddi/$port
        LD_LIBRARY_PATH=${lb} ${srv}  -c resolution -C ${cls} --datadir /mnt/nvme/nvme0/data_ddi/${port}  --listen_address public \
        --logdir /mnt/nvme/nvme0/log_ddi --public_address auto:${port} --knob_page_cache_4k 104857600 --storage_memory 4GB \
        --memory 16GB --knob_cache_eviction_policy random --knob_max_evict_attempts 100 ${uring} ${flow} > /mnt/nvme/nvme0/data_ddi/${port}.txt 2>&1  &

        if [[ ${version} == 700 ]];then
            port=$((${port}+1))
            mkdir /mnt/nvme/nvme0/data_ddi/$port
            LD_LIBRARY_PATH=${lb} ${srv}  -c grv_proxy -C ${cls} --datadir /mnt/nvme/nvme0/data_ddi/${port}  --listen_address public \
            --logdir /mnt/nvme/nvme0/log_ddi --public_address auto:${port} --knob_page_cache_4k 104857600 --storage_memory 4GB \
            --memory 16GB --knob_cache_eviction_policy random --knob_max_evict_attempts 100 ${uring} ${flow} > /mnt/nvme/nvme0/data_ddi/${port}.txt 2>&1  &

            port=$((${port}+1))
            mkdir /mnt/nvme/nvme0/data_ddi/$port
            LD_LIBRARY_PATH=${lb} ${srv}  -c commit_proxy -C ${cls} --datadir /mnt/nvme/nvme0/data_ddi/${port}  --listen_address public \
            --logdir /mnt/nvme/nvme0/log_ddi --public_address auto:${port} --knob_page_cache_4k 104857600 --storage_memory 4GB \
            --memory 16GB --knob_cache_eviction_policy random --knob_max_evict_attempts 100 ${uring} ${flow} > /mnt/nvme/nvme0/data_ddi/${port}.txt 2>&1  &
         else
            port=$((${port}+1))
            mkdir /mnt/nvme/nvme0/data_ddi/$port
            LD_LIBRARY_PATH=${lb} ${srv}  -c proxy -C ${cls} --datadir /mnt/nvme/nvme0/data_ddi/${port}  --listen_address public \
            --logdir /mnt/nvme/nvme0/log_ddi --public_address auto:${port} --knob_page_cache_4k 104857600 --storage_memory 4GB \
            --memory 16GB --knob_cache_eviction_policy random --knob_max_evict_attempts 100 ${uring} ${flow} > /mnt/nvme/nvme0/data_ddi/${port}.txt 2>&1  &
        fi

        port=$(( $port + 1 ))
        mkdir /mnt/nvme/nvme0/data_ddi/$port
        LD_LIBRARY_PATH=${lb} ${srv}  -c log -C ${cls} --datadir /mnt/nvme/nvme0/data_ddi/${port}  --listen_address public \
        --logdir /mnt/nvme/nvme0/log_ddi --public_address auto:${port} --knob_page_cache_4k 104857600 --storage_memory 4GB \
        --memory 16GB --knob_cache_eviction_policy random --knob_max_evict_attempts 100 ${uring} ${flow} > /mnt/nvme/nvme0/data_ddi/${port}.txt 2>&1  &

        port=$((${port}+1))
        mkdir /mnt/nvme/nvme0/data_ddi/$port
        LD_LIBRARY_PATH=${lb} ${srv}  -c storage -C ${cls} --datadir /mnt/nvme/nvme0/data_ddi/${port}  --listen_address public \
        --logdir /mnt/nvme/nvme0/log_ddi --public_address auto:${port} --knob_page_cache_4k 104857600 --storage_memory 4GB \
        --memory 16GB --knob_cache_eviction_policy random --knob_max_evict_attempts 100 ${uring} ${flow} > /mnt/nvme/nvme0/data_ddi/${port}.txt 2>&1  &
    fi
}

create_db(){

if [[ ${version} == 700 ]];then
    LD_LIBRARY_PATH=${lb} /mnt/nvme/nvme0/uringdb/bld/bin/fdbcli -C ${cls} --exec "configure new single ssd-2"
elif [[ ${version} == 620 ]];then
    LD_LIBRARY_PATH=/home/ddi/ikvb/lib/fdb/620 ~/fdb_binaries_620/fdbcli -C ${cls} --exec "configure new single ssd-2"
else
    LD_LIBRARY_PATH=/home/ddi/ikvb/lib/fdb/630 ~/fdb_binaries_630/fdbcli -C ${cls} --exec "configure new single ssd-2"
fi

}

show_help(){
    echo "./manual.sh -s to spawn"
    echo "./manual.sh -c to created a db"
    echo "./manual.sh -t to spawn and create db after a timeout"
}


# A POSIX variable
OPTIND=1         # Reset in case getopts has been used previously in the shell.

# Initialize our own variables:
output_file=""
verbose=0

while getopts "h?sct" opt; do
    case "$opt" in
    h|\?)
        echo "help"
        show_help
        exit 0
        ;;
    s)
        spawn
        exit 0
        ;;
    c)
        create_db
        exit 0
        ;;
    t)
        spawn
        sleep 7
        create_db
        exit 0
        ;;
    esac
done

shift $((OPTIND-1))

#[ "${1:-}" = "--" ] && shift
echo "Leftovers: $@"
show_help
exit 0
#echo "Leftovers: $@"
# LD_LIBRARY_PATH=lib/fdb/700/  stdbuf -oL bin/ikvb -f /mnt/ddi/db --xput /mnt/ddi/results/1.xput --freq 2900000000 --seed 11838872141964924 -u 13 --t_population 32 -t 0 --num_keys 500000 --key_size 64 --value_size const512 --key_type random --additional_args max_batch_size=1000,grv_batch_timeout=0.005 --config_file /home/ddi/fdb.flex13 --id 1 --num_clients 1 -t 0
