FDBCLI="/dev/shm/fdb-build/bin/fdbcli"
FDBSERVER="/dev/shm/fdb-build/bin/fdbserver"
LIB="/mnt/nvme/nvme0/uringdb/liburing/src"
#use .stub for the stub and .txt for the test
TEST="/mnt/nvme/nvme0/uringdb/tests/RW"
CLS="/home/ddi/fdb-official/fdb.cluster"
FILEPATH="/mnt/nvme/nvme1/testfiles"
PAGE_CACHE="100"  #MiB
RESULTS=`date +%Y-%m-%d_%H-%M-%S`
hn=$(hostname)
RESULTS="${RESULTS}-${hn}-KV"

DEVS=("/dev/nvme3n1" "/dev/nvme4n1" "/dev/nvme5n1" "/dev/nvme6n1" "/dev/nvme7n1" "/dev/nvme8n1" "/dev/nvme9n1" "/dev/nvme10n1" "/dev/nvme11n1")
MNTS=("/mnt/nvme/nvme3" "/mnt/nvme/nvme4" "/mnt/nvme/nvme5" "/mnt/nvme/nvme6" "/mnt/nvme/nvme7" "/mnt/nvme/nvme8" "/mnt/nvme/nvme9" "/mnt/nvme/nvme10" "/mnt/nvme/nvme11")
USERGROUP="ddi:sto"

STORAGE_PER_DISK=1
LOG_PER_DISK=1
STORAGE_DISKS=7
LOG_DISKS=1

STORAGES=$(( STORAGE_PER_DISK * STORAGE_DISKS ))
LOGS=$(( LOG_PER_DISK * LOG_DISKS))
echo "$(( $STORAGE_DISKS + $LOG_DISKS + 1 ))"
echo "${#DEVS[@]}"
if [[ $(( $STORAGE_DISKS + $LOG_DISKS + 1 )) -gt ${#DEVS[@]} ]] || [[ ${#DEVS[@]} != ${#MNTS[@]} ]];then
	echo "error"
	exit 1
fi
TRIM=1

trim(){
	if [[ $TRIM == 1 ]];then
		#Keepalive for sudo. https://gist.github.com/cowboy/3118588
		# Might as well ask for password up-front, right?
		sudo -v
		# Keep-alive: update existing sudo time stamp if set, otherwise do nothing.
		while true; do
			sudo -n true
			sleep 60
			kill -0 "$$" || exit
		done 2>/dev/null &
		echo $(mount)
		for i in "${!DEVS[@]}"; do 
			sudo mount | grep -qs ${DEVS[$i]}
			ret=$?
			if [ $ret -eq 0 ];then
				while true;do
					echo "umounting ${DEVS[$i]}"
					sudo umount ${MNTS[$i]}
					ret=$?
					if [[ $ret -ne 0 ]]; then
						echo "umount ${MNTTS[$1]} failed with ret $ret"
						sleep 5
					else
						break
					fi
				done
			fi

			echo "Trimming ${DEV[$i]}"
			sudo /sbin/blkdiscard ${DEVS[$i]}
			yes | sudo mkfs.ext4 ${DEVS[$i]} -E lazy_itable_init=0,lazy_journal_init=0,nodiscard
			if [[ $? -ne 0 ]]; then
				echo "ext4 failed"
				exit 1
			fi
			sudo mount ${DEVS[$i]} ${MNTS[$i]}
			if [[ $? -ne 0 ]]; then
				echo "mount ${MNTS[$i]} failed"
				exit 1
			fi
			sudo chown -R $USERGROUP ${MNTS[$i]}
		done
	fi
}






