#!/bin/bash
# Run the mtlsbenchmark server
# Usage: ./server.sh
# This script starts the server to listen on port 4500 for incoming mTLS connections
# Adjust the parameters as needed
# test_listenerAddresses is set to listen on all interfaces at port 4500 with TLS
# test_targetDuration is set to 0 for indefinite server run
# knob_tls_handshake_limit is set to 1000 to limit the number of concurrent TLS
# knob_tls_server_handshake_threads is set to 1 to use one thread for TLS handshakes
# knob_disable_mainthread_tls_handshake is enabled to use background threads for TLS handshakes only
# knob_tls_handshake_flowlock_priority is set to 8900 for enabling TLS flowlock priority as high as the handshake priority. Default is 7000.
# knob_tls_handshake_timeout_seconds is set to 3.0 seconds to timeout a handshake if not completed in 3 seconds. The default is 2.0 seconds.

taskset -c 1-1  /root/build_output/bin/fdbserver \
    -r unittests \
    -f :/network/p2ptest \
    --test_listenerAddresses=0.0.0.0:4500:tls \
    --test_targetDuration=0 \
    --knob_tls_handshake_limit=1000 \
    --knob_tls_server_handshake_threads=1 \
    --knob_disable_mainthread_tls_handshake=true \
    --knob_tls_handshake_flowlock_priority=8900 \
    --knob_tls_handshake_timeout_seconds=3.0 \
    --tls_ca_file keys/ca_file.crt \
	--tls_certificate_file keys/certificate_file.crt  \
	--tls_key_file keys/key_file.key  \
    --tls_verify_peers "Root.CN=dummy-ca"
