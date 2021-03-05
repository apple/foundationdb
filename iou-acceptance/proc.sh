#!/bin/bash
A="kaio"
B="kaio_nobatch"
FLD=2021-01-18_19-15-48-zac13-KV-shm
echo "io storage actors read ${B}_avg ${A}_avg ${B}_stdv ${A}_stdv"
for kv in sqlite; do
	for rd in 10 5; do
		for st in 1  ;do
			for act in 1 64; do
				wr=$(( 10 - $rd ))
				echo -n "$kv $st $act $rd "
				iou=$(cat  $FLD/io=${B}_kv=${kv}_s=60_rd=${rd}_wr=${wr}_c=100_a=${act}_st=${st}_shm=1_r=*.txt | grep Transactions/sec | awk {'print $5'} | cut -d, -f1 | awk '{sum+=$1} END {print sum/NR}')
				kaio=$(cat  $FLD/io=${A}_kv=${kv}_s=60_rd=${rd}_wr=${wr}_c=100_a=${act}_st=${st}_shm=1_r=*.txt | grep Transactions/sec | awk {'print $5'} | cut -d, -f1 | awk '{sum+=$1} END {print sum/NR}')
				ious=$(cat $FLD/io=${B}_kv=${kv}_s=60_rd=${rd}_wr=${wr}_c=100_a=${act}_st=${st}_shm=1_r=*.txt | grep Transactions/sec | awk {'print $5'} | cut -d, -f1 | awk  '{sum+=$1; sumsq+=$1*$1} END {print sqrt(sumsq/NR - (sum/NR)**2)}')
				kaios=$(cat $FLD/io=${A}_kv=${kv}_s=60_rd=${rd}_wr=${wr}_c=100_a=${act}_st=${st}_shm=1_r=*.txt | grep Transactions/sec | awk {'print $5'} | cut -d, -f1 | awk  '{sum+=$1; sumsq+=$1*$1} END {print sqrt(sumsq/NR - (sum/NR)**2)}')
				echo "$iou $kaio  $ious $kaios"
			done
		done
	done
done
