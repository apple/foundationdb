FN="/mnt/nvme/nvme0/testfiles/file.dat"

#io_uring seq write
fio --filename=$FN --direct=1 --rw=randwrite --bs=4k --ioengine=io_uring --iodepth=1 --runtime=30 --numjobs=64 --time_based --group_reporting --name=throughput-test-job --eta-newline=1  --size=20G --randrepeat=0 --norandommap


fio --filename=$FN --direct=1 --rw=randread --bs=4k --ioengine=io_uring --iodepth=1 --runtime=30 --numjobs=64 --time_based --group_reporting --name=throughput-test-job --eta-newline=1  --size=20G --randrepeat=0 --norandommap

fio --filename=$FN --direct=1 --rw=randrw --bs=4k --ioengine=io_uring --iodepth=1 --runtime=30 --numjobs=64 --time_based --group_reporting --name=throughput-test-job --eta-newline=1  --size=20G --randrepeat=0 --norandommap


#kaio
fio --filename=$FN --direct=1 --rw=randwrite --bs=4k --ioengine=libaio --iodepth=1 --runtime=30 --numjobs=64 --time_based --group_reporting --name=throughput-test-job --eta-newline=1  --size=20G --randrepeat=0 --norandommap


fio --filename=$FN --direct=1 --rw=randread --bs=4k --ioengine=libaio --iodepth=1 --runtime=30 --numjobs=64 --time_based --group_reporting --name=throughput-test-job --eta-newline=1  --size=20G --randrepeat=0 --norandommap

fio --filename=$FN --direct=1 --rw=randrw --bs=4k --ioengine=libaio --iodepth=1 --runtime=30 --numjobs=64 --time_based --group_reporting --name=throughput-test-job --eta-newline=1  --size=20G --randrepeat=0 --norandommap

