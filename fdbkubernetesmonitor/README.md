
This package provides a launcher program for running FoundationDB in Kubernetes.

To test this, run the following commands from the root of the FoundationDB
repository:

		docker build -t foundationdb/foundationdb-kubernetes:latest --build-arg FDB_VERSION=6.3.15 --build-arg FDB_LIBRARY_VERSIONS="6.3.15 6.2.30 6.1.13" -f packaging/docker/kubernetes/Dockerfile .
		kubectl apply -f packaging/docker/kubernetes/test_config.yaml
		# Wait for the pods to become ready
		ips=$(kubectl get pod -l app=fdb-kubernetes-example -o json | jq -j '[[.items|.[]|select(.status.podIP!="")]|limit(3;.[])|.status.podIP+":4501"]|join(",")')
		cat packaging/docker/kubernetes/test_config.yaml | sed -e "s/fdb.cluster: \"\"/fdb.cluster: \"test:test@$ips\"/" -e "s/\"serverCount\": 0/\"serverCount\": 1/" | kubectl apply -f -
		kubectl get pod -l app=fdb-kubernetes-example -o name | xargs -I {} kubectl annotate {} foundationdb.org/outdated-config-map-seen=$(date +%s) --overwrite
		# Watch the logs for the fdb-kubernetes-example pods to confirm that they have launched the fdbserver processes.
		kubectl exec -it sts/fdb-kubernetes-example -- fdbcli --exec "configure new double ssd"

You can then make changes to the data in the config map and update the fdbserver processes:

		cat packaging/docker/kubernetes/test_config.yaml | sed -e "s/fdb.cluster: \"\"/fdb.cluster: \"test:test@$ips\"/" -e "s/\"serverCount\": 0/\"serverCount\": 1/" | kubectl apply -f -

		# You can apply an annotation to speed up the propagation of config
		kubectl get pod -l app=fdb-kubernetes-example -o name | xargs -I {} kubectl annotate {} foundationdb.org/outdated-config-map-seen=$(date +%s) --overwrite

		# Watch the logs for the fdb-kubernetes-example pods to confirm that they have reloaded their configuration, and then do a bounce.
		kubectl exec -it sts/fdb-kubernetes-example -- fdbcli --exec "kill; kill all; status"

Once you are done, you can tear down the example with the following command:

		kubectl delete -f packaging/docker/kubernetes/test_config.yaml; kubectl delete pvc -l app=fdb-kubernetes-example
