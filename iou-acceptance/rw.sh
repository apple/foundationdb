#!/bin/bash
set -x
#set -e

<<END_COMM
RW
END_COMM

source rwconfig.sh

mkdir -p ${RESULTS} || exit 1
port=

CORE=
testpid=
testport=

uring=""
uring_srv=""

USERGROUP="ddi:sto"
STORAGES=1
TESTERS=1
TRIM=0

#pkill -9 -f fdbserver
#sleep 1

run_test(){
	out=${1}
	uring=${2}
	mem="64GB"
	#spawn the orchestrator
	#https://stackoverflow.com/questions/13356628/how-to-redirect-the-output-of-the-time-command-to-a-file-in-linux
	iostat -x 1 -p ${DEV} > ${RESULTS}/iostat_$out &

	{ time LD_LIBRARY_PATH=${LIB} taskset -c ${CORE} ${FDBSERVER}  -r multitest -f ${TEST}.txt -C ${CLS} --memory ${mem} ${uring} --logdir=${DATALOGPATH} ;}  > ${RESULTS}/${out} 2>&1 &
	#Take the pid of the orchestrator by taking the pid of "time" and pgrepping by parent
	timepid=$!
	orchpid=$(pgrep -P $timepid)
	echo "orch pid ${orchpid}. Waiting to finish"
	CORE=$(( $CORE + 1 ))

	#sudo perf trace -s -p $(ps aux | grep storage | grep -v grep | awk '{print $2}') -o iou_batch_nodirect_poll.perf &
	#sudo perf record  -F 10  -p $(ps aux | grep storage | grep -v grep | awk '{print $2}') --call-graph dwarf -o rec.perf
 
	set +x
	while kill -0 $orchpid ; do pmap $testpid | grep total | awk '{print $2}' >> ${RESULTS}/pmap_$out ; sleep 1 ;done
	set -x
}




spawn(){
	pkill -9 fdbserver || true    #if nothing is killed, error is returned
	pkill -9 iostat || true
	pkill -9 pstat || true

	sleep 1

	data_dir=${DATALOGPATH}


	mkdir -p ${DATALOGPATH}
	echo "removing ${DATALOGPATH}/*"
	rm -rf ${DATALOGPATH}/*
	mkdir -p ${data_dir}/${port} || true
	if [[ $STORAGES == 0 ]];then

		LD_LIBRARY_PATH=${LIB}  taskset -c ${CORE} ${FDBSERVER} -C ${CLS} -p auto:${port} --listen_address public ${uring_srv}  --datadir=${data_dir}/${port} --logdir=${data_dir}/${port} &
	
	
	else #we do have more than one storage

		#spawn stateless
		LD_LIBRARY_PATH=${LIB}  taskset -c ${CORE} ${FDBSERVER} -c stateless -C ${CLS} -p auto:${port} --listen_address public ${uring_srv}  --datadir=${data_dir}/${port} --logdir=${data_dir}/${port} &


		#spawn log
		CORE=$(( $CORE + 1 ))
		port=$((${port}+1))
		if [[ $LOG_SHM == 1 ]]; then
			#https://serverfault.com/questions/960189/why-cant-other-user-remove-dev-shm-xxx-even-with-orw-permissions 
			tlogd="/mnt/nvme/nvme2/$port"
			mkdir ${tlogd} || rm -rf ${tlogd}/*
			LD_LIBRARY_PATH=${LIB}  taskset -c ${CORE} ${FDBSERVER} -c log -C ${CLS} -p auto:${port} --listen_address public ${uring_srv} --datadir=${tlogd} --logdir=${tlogd} &
		else
			mkdir ${data_dir}/${port} || true
			LD_LIBRARY_PATH=${LIB} taskset -c ${CORE} ${FDBSERVER} -c log -C ${CLS} -p auto:${port} --listen_address public ${uring_srv} --datadir=${data_dir}/${port} --logdir=${data_dir}/${port} &
		fi
		#spawn storage servers
		#seq 0 0 is 0 so it spawns one
		for s in $(seq 1 $STORAGES);do
			CORE=$(( $CORE + 1 ))
			port=$(( ${port} + 1 ))
			mkdir -p ${data_dir}/${port} || true
			LD_LIBRARY_PATH=${LIB}  taskset -c ${CORE} ${FDBSERVER} -c storage -C ${CLS} -p auto:${port} --listen_address public ${uring_srv}  --datadir=${data_dir}/${port} --logdir=${data_dir}/${port} &
			storageport=${port}
		done	
	fi

	#spawn the test role
	for i in $(seq 1 $CLIENTS);do
		CORE=$(( $CORE + 1 ))
		port=$((${port}+1))
		mkdir ${data_dir}/${port} || true
		LD_LIBRARY_PATH=${LIB} taskset -c ${CORE} ${FDBSERVER} -C ${CLS} -c test -p auto:${port} --listen_address public ${uring_srv} --datadir=${data_dir}/${port} --logdir=${data_dir}/${port} &
		testpid=$!
		echo "Test pid is $testpid"
	done

	sleep 5 #give time to join the cluster

	#create the db
	if [[ $kv == "redwood" ]];then kvs="ssd-redwood-experimental"; else kvs="ssd-2";fi
	LD_LIBRARY_PATH=${LIB} ${FDBCLI} -C ${CLS} --exec "configure new single ${kvs}"
	sleep 5
}

setup_test(){

	if [[ $TRIM == 1 ]];then
		echo "A"
		sudo mount | grep -qs $MOUNT_POINT
		ret=$?
		echo $ret
		if [ $ret -eq 0 ];then
			echo "umounting $MOUNT_POINT"

			while true;do
				sudo umount $MOUNT_POINT
				ret=$?
				if [[ $ret -ne 0 ]]; then
					echo "umount ${MOUNT_POINT} failed with ret $ret"
					sleep 2
				else
					break
				fi
			done
		fi

		echo "Trimming /dev/$DEV"
		sudo /sbin/blkdiscard /dev/$DEV
		yes | sudo mkfs.ext4 /dev/$DEV -E lazy_itable_init=0,lazy_journal_init=0,nodiscard
		if [[ $? -ne 0 ]]; then
			echo "ext4 failed"
			exit 1
		fi
		sudo mount /dev/$DEV $MOUNT_POINT
		if [[ $? -ne 0 ]]; then
			echo "mount ${MOUNT_POINT} failed"
			exit 1
		fi
		sudo chown -R $USERGROUP ${MOUNT_POINT}
	fi
	pc=$(( ${PAGE_CACHE} * 1024 * 1024 ))
	if [[ $1 == "io_uring"* ]];then
		uring="--knob_enable_io_uring true --knob_page_cache_4k ${pc}" 
		if [[ $1 == *"direct"* ]];then
			uring="$uring --knob_io_uring_direct_submit true"
		fi

		if [[ $1 == *"batch"* ]];then
			uring="$uring --knob_io_uring_batch true"
		fi

		if [[ $1 == *"poll"* ]];then
			uring="$uring --knob_io_uring_poll true"
		fi

	elif [[ $1 == "kaio" ]];then
		uring=" --knob_page_cache_4k ${pc}"
		echo "KAIO"
	elif [[ $1 == "kaio_nobatch" ]];then
		uring="--knob_min_submit 1 --knob_page_cache_4k ${pc}"
		echo "KAIO_NOBATCH"
	else

		echo "Mode not supported. Use either io_uring or kaio"
		exit 1
	fi

	uring_srv=${uring}

	mkdir -p $FILEPATH
	cp ${TEST}.stub ${TEST}.txt
	sed -i  "s/TEST_DURATION/$3/g" ${TEST}.txt
	sed -i  "s/READS_PER_TX/$4/g" ${TEST}.txt
	sed -i  "s/WRITES_PER_TX/$5/g" ${TEST}.txt
	sed -i  "s/NUM_ACTORS/$6/g" ${TEST}.txt
	#replace slash in path with escaped slash
	#https://unix.stackexchange.com/questions/211834/slash-and-backslash-in-sed
	file=$(echo "${FILEPATH}/file.dat" |  sed -e 's/\//\\\//g')
	sed -i  "s/FILE_NAME/${file}/g" ${TEST}.txt
}

run_one(){
	duration=$1
	kv=$2
	reads=$3
	writes=$4
	run=$5
	io=$6
	actors=$7
	CORE=1
	port=4500

	pc=$(( ${PAGE_CACHE} * 1024 * 1024 ))
	out_file="io=${io}_kv=${kv}_s=${duration}_rd=${reads}_wr=${writes}_c=${PAGE_CACHE}_a=${actors}_st=${STORAGES}_shm=${LOG_SHM}_clnt=${CLIENTS}r=${run}.txt"
	echo ${out_file}

	setup_test $io $kv $duration $reads $writes $actors
	cp ${TEST}.txt $RESULTS/TEST_$out_file

	spawn

	time run_test ${out_file} "${uring}"
	#cat ${timing} >> ${out_file}
	#kill server and iostat
	pkill -9 fdbserver
	pkill -9 iostat

	#copy the xml file of the test server 
	xml=$(ls ${DATALOGPATH}/${storageport}/*xml | tail -n1)
	cp $xml $RESULTS/${out_file}.storage.xml

}

sec=60

if [[ $TRIM == 1 ]]; then
	#Keepalive for sudo. https://gist.github.com/cowboy/3118588
	# Might as well ask for password up-front, right?
	sudo -v
	# Keep-alive: update existing sudo time stamp if set, otherwise do nothing.
	while true; do
		sudo -n true
		sleep 60
		kill -0 "$$" || exit
	done 2>/dev/null &
fi

ops=10
CLIENTS=2
for cac in 100;do
	PAGE_CACHE=${cac}
	for st in 1 2; do
	for CLIENTS in 1 2; do
	for kv in "sqlite" "redwood";do
	STORAGES=$st
	for wr in 0  5;  do
	for run in 1 2 3 4 5;do
	for na in 1 64;do
	for io in "io_uring_batch" "io_uring_batch_direct" "kaio";do
	rd=$(( $ops - $wr ))
	run_one  ${sec} ${kv} ${rd} ${wr} ${run} ${io} ${na} ${st}
	done
	done
	done
	done
	done
	done
	done
done


#comparing to
#sudo fio --filename=/mnt/nvme/nvme10/aftest.bin  --direct=1 --rw=randread --bs=4k --ioengine=libaio --iodepth=128 --runtime=30 --numjobs=20 --time_based --group_reporting --name=throughput-test-job --eta-newline=1 --readonly --size=10G


# LD_LIBRARY_PATH=/mnt/nvme/nvme0/uringdb/liburing/src ../bld/bin/fdbcli -C ~/fdb-official/fdb.cluster --exec "status json" | fdbtop
# LD_LIBRARY_PATH=/mnt/nvme/nvme0/uringdb/liburing/src PATH=/mnt/nvme/nvme0/uringdb/bld/bin/:$PATH fdbtop -- -C ~/fdb-official/fdb.cluster
