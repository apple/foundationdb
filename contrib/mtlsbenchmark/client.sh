#!/bin/bash
# Run the mtlsbenchmark client
# Usage: ./client.sh
# This script assumes that the server is already running on localhost:4500
# Adjust the parameters as needed
# test_remoteAddresses is set to connect to the server
# test_targetDuration is set to 10 seconds for the client duration
# test_connectionsOut is set to 10 for the number of outgoing connections
# knob_disable_mainthread_tls_handshake is enabled to use background threads for TLS handshakes only
# knob_tls_handshake_flowlock_priority is set to 8900 for enabling TLS flowlock priority as high as the handshake priority. Default is 7000.

taskset -c 0-0 /root/build_output/bin/fdbserver \
	-r unittests \
	-f :/network/p2ptest \
	--test_remoteAddresses=127.0.0.1:4500:tls \
	--test_targetDuration=10 \
	--test_connectionsOut=10 \
	--knob_disable_mainthread_tls_handshake=true \
	--knob_tls_handshake_flowlock_priority=8900 \
	--tls_ca_file keys/ca_file.crt \
	--tls_certificate_file keys/certificate_file.crt  \
	--tls_key_file keys/key_file.key  \
	--tls_verify_peers "Root.CN=dummy-ca"
