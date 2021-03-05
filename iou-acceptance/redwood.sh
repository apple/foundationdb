#!/usr/bin/env bash
#!/usr/bin/env bash
#set -x
#set -e

<<END_COMM
AsyncFileTest
END_COMM


source /mnt/ddi/uringdb/iou-acceptance/config.sh
mkdir -p ${RESULTS} || exit 1
port=

CORE=
testpid=
testport=

uring=""

TEST="/mnt/ddi/uringdb/tests/RedwoodCorrectnessBTree"

run_test(){
	out=${1}
	uring=${2}
	mem="4GB"
	mkdir -p ${data_dir}/${port} || true
	#spawn the orchestrator
	#https://stackoverflow.com/questions/13356628/how-to-redirect-the-output-of-the-time-command-to-a-file-in-linux
	iostat -x 1 -p ${DEV} > ${RESULTS}/iostat_$out &

	LD_LIBRARY_PATH=${LIB} taskset -c ${CORE} ${FDBSERVER}  -r multitest -s 300 -f ${TEST}.txt -C ${CLS} --memory ${mem} ${uring} --logdir=${DATALOGPATH} > ${RESULTS}/${out} 2>&1 &
		orchpid=$!
		echo "orch pid ${orchpid}"
		CORE=$(( $CORE + 1 ))
		if [[ -z $orchpid ]];then
			exit 1  #we exit so that we can replicate the last orchestrator command and see what is what
		fi
		while kill -0 $orchpid ; do pmap $testpid | grep total | awk '{print $2}' >> ${RESULTS}/pmap_$out ; sleep 1 ;done
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
	#spawn one-process cluster
	mkdir -p ${data_dir}/${port} || true
	LD_LIBRARY_PATH=${LIB}  taskset -c ${CORE} ${FDBSERVER} -C ${CLS} -p auto:${port} --listen_address public ${uring}  --datadir=${data_dir}/${port} --logdir=${data_dir}/${port} &
	CORE=$(( $CORE + 1 ))

	#spawn the test role
	port=$((${port}+1))
	mkdir ${data_dir}/${port} || true
	LD_LIBRARY_PATH=${LIB} taskset -c ${CORE} ${FDBSERVER} -C ${CLS} -c test -s 100 -p auto:${port} --listen_address public ${uring} --datadir=${data_dir}/${port} --logdir=${data_dir}/${port} &
	testpid=$!
	testport=${port}
	echo "Test pid is $testpid"
	CORE=$(( $CORE + 1 ))

	sleep 5 #give time to join the cluster

    #create the db
    #LD_LIBRARY_PATH=${lb} ${cli} -C ${cls} --exec "configure new single ssd-2"
}

setup_test(){
	pc=$(( ${PAGE_CACHE} * 1024 * 1024 ))
	if [[ $1 == "io_uring" ]]; then
		uring="--knob_enable_io_uring true --knob_io_uring_direct_submit true --knob_page_cache_4k ${pc}"
		echo "URING"
	elif [[ $1 == "kaio" ]];then
		uring=" --knob_page_cache_4k ${pc}"
		echo "KAIO"
	else
		echo "Mode not supported. Use either io_uring or kaio"
		exit 1
	fi
}

run_one(){
	io=$1
	r=$2
	CORE=1
	port=4500

	out_file="io=${io}_s=${r}"
	echo ${out_file}

	setup_test $io

	spawn

	time run_test ${out_file} "${uring}"
	#cat ${timing} >> ${out_file}
	#kill server and iostat
	pkill -9 fdbserver
	pkill -9 iostat

}

# Starts here
pkill -9 -f fdbserver

for r in $(seq 1 10000); do
	run_one "io_uring" $r
done
