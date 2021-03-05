#!/usr/bin/env bash
#!/usr/bin/env bash
#set -x
#set -e

<<END_COMM
AsyncFileTest
END_COMM

source config.sh || exit 1

#read -p "You have selected $DEV as device and $MOUNT_POINT as mount point. Continuing might wipe them. Are you sureyou want to go on? (y/n)" yn
#select yn in "y" "n"; do
#case $yn in
#	[Nn]*) exit ;;
#	[Yy]*) echo "Copy that" ;;
#	# Matching with invalid data
#	*) echo "Invalid reply (y/n)." && exit 1 ;;
#esac

mkdir -p ${RESULTS} || exit 1
port=

CORE=
testpid=
testport=

uring=""

TRIM=1
#If TRIM is enabled, the file has to be retrieved from somewhere to avoid creating it every time
#Be sure that the name of the file matches the field in the test file
PRE_TEST_FILE="/mnt/ddi/file.dat"
USERGROUP="ddi:sto"


run_test(){
	out=${1}
	uring=${2}
	mem="4GB"
	mkdir -p ${data_dir}/${port} || true
	#spawn the orchestrator
	#https://stackoverflow.com/questions/13356628/how-to-redirect-the-output-of-the-time-command-to-a-file-in-linux
	iostat -x 1 -p ${DEV} > ${RESULTS}/iostat_$out &

	{ time LD_LIBRARY_PATH=${LIB} taskset -c ${CORE} ${FDBSERVER}  -r multitest -f ${TEST}.txt -C ${CLS} --memory ${mem} ${uring} --logdir=${DATALOGPATH} ;}  > ${RESULTS}/${out} 2>&1 &
		#LD_LIBRARY_PATH=${LIB} gdb -ex run --args  ${FDBSERVER}  -r test -f ${TEST}.txt -C ${CLS} --memory ${mem} ${uring} --logdir=${DATALOGPATH}
		#Take the pid of the orchestrator by taking the pid of "time" and pgrepping by parent
		timepid=$!
		orchpid=$(pgrep -P $timepid)
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

	fn=$(cat ${TEST}.txt | grep "fileName" | cut -d= -f2)

	if [[ $TRIM == 1 ]];then
		echo "Copying  $PRE_TEST_FILE to $fn"
		cp $PRE_TEST_FILE $fn
		echo "Finished copying"
		sync $fn
		echo "Sync'd"
		sudo sh -c "echo '3' > /proc/sys/vm/drop_caches"
		echo "Cache dropped"
		while [ $(echo "$(iostat 1 1 -y| grep $DEV | awk {'print $4'}) >= 4096" | bc -l) == "1" ];do echo "not quiescent" ;sleep 5; done
	fi


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
	LD_LIBRARY_PATH=${LIB} taskset -c ${CORE} ${FDBSERVER} -C ${CLS} -c test -p auto:${port} --listen_address public ${uring} --datadir=${data_dir}/${port} --logdir=${data_dir}/${port} &
	testpid=$!
	testport=${port}
	echo "Test pid is $testpid"
	CORE=$(( $CORE + 1 ))

	sleep 5 #give time to join the cluster

    #create the db
    #LD_LIBRARY_PATH=${lb} ${cli} -C ${cls} --exec "configure new single ssd-2"
}

setup_test(){
	if [[ $TRIM == 1 ]];then
		mount | grep -qs $MOUNT_POINT
		if [ $? -eq 0 ];then
			echo "umounting $MOUNT_POINT"

			while true;do
				sudo umount $MOUNT_POINT
				ret=$?
				if [[ $ret -ne 0 ]]; then
					echo "umount ${MOUNT_POINT} failed with ret $ret"
					sleep 10
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

	mkdir -p $FILEPATH
	cp ${TEST}.stub ${TEST}.txt
	sed -i  "s/TEST_DURATION/$2/g" ${TEST}.txt
	sed -i  "s/TEST_READS/$3/g" ${TEST}.txt
	sed -i  "s/TEST_UNBUFFERED/$4/g" ${TEST}.txt
	sed -i  "s/TEST_UNCACHED/$5/g" ${TEST}.txt
	sed -i  "s/TEST_WRITE_FRACTION/$6/g" ${TEST}.txt
	#replace slash in path with escaped slash
	#https://unix.stackexchange.com/questions/211834/slash-and-backslash-in-sed
	file=$(echo "${FILEPATH}/file.dat" |  sed -e 's/\//\\\//g')
	sed -i  "s/FILE_NAME/${file}/g" ${TEST}.txt
}

run_one(){
	io=$1
	duration=$2
	parallel_reads=$3
	unbuffered=$4  #buffered/unbuffered
	uncached=$5  #true/false
	write_fraction=$6
	run=${7}
	CORE=1
	port=4500

	out_file="io=${io}_s=${duration}_pr=${parallel_reads}_b=${unbuffered}_c=${uncached}_w=${write_fraction}_pc=${PAGE_CACHE}r=${run}.txt"
	echo ${out_file}
	if [[ $5 == "cached" ]];then
		uncached="false"
	else
		uncached="true"
	fi

	if [[ $4 == "buffered" ]];then
		unbuffered="false"
	else
		unbuffered="true"
	fi

	setup_test $io $duration $parallel_reads $unbuffered $uncached $write_fraction
	cp ${TEST}.txt $RESULTS/TEST_$out_file

	spawn

	time run_test ${out_file} "${uring}"
	#cat ${timing} >> ${out_file}
	#kill server and iostat
	pkill -9 fdbserver
	pkill -9 iostat

	#copy the xml file of the test server 
	xml=$(ls ${DATALOGPATH}/${testport}/*xml | tail -n1)
	cp $xml $RESULTS/$out_file.xml
}

# Starts here
pkill -9 -f fdbserver
sleep 1
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

sec=90
buff="unbuffered" #buffered unbuffered
cached="uncached"   #cached uncached

for PAGE_CACHE in 10 100; do
for b in "unbuffered"; do
	for c in "uncached" "cached";do
	   for run in 1 2 3 4 5; do
			for parallel_reads in 1 32 64; do
				for write_perc in 0 0.5 1;do
					for io in  "io_uring_batch_direct" "io_uring_batch" "kaio"; do
						run_one ${io} ${sec} ${parallel_reads} ${b} ${c} ${write_perc} ${run}
					done #uring
				done #write perc
			done #reads
		done #run
	done
done
done


#comparing to fio with direct i/o, many parallel  jobs, each  with qd 1 
#fio --filename=/mnt/nvme/nvme0/testfiles/file.dat  --direct=1 --rw=randread --bs=4k --ioengine=libaio --iodepth=1 --runtime=30 --numjobs=64 --time_based --group_reporting --name=throughput-test-job --eta-newline=1  --size=10G --randrepeat=0 --norandommap
