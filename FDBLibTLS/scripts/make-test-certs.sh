#!/bin/sh
#
# make-test-certs.sh
#
# This source file is part of the FoundationDB open source project
#
# Copyright 2013-2022 Apple Inc. and the FoundationDB project authors
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

set -e
set -u

readonly SUBJECT="/C=US/ST=California/L=Cupertino/O=Apple Inc./OU=FDB Team/CN=FDB LibTLS Plugin Test"
readonly SUBJECT_ALT="/C=AU/ST=New South Wales/L=Sydney/O=Apple Pty Limited/OU=FDB Team/CN=FDB LibTLS Plugin Test"

readonly TMPDIR=$(mktemp -d)

cleanup() {
        rm -rf "${TMPDIR}"
}

trap cleanup EXIT INT

make_ca_bundle() {
  local bundle_file=$1;
  shift 1;

  printf '' > "${bundle_file}"
  for f in $@; do
    openssl x509 -nameopt oneline -subject -issuer -noout -in "${TMPDIR}/${f}" >> "${bundle_file}"
    cat "${TMPDIR}/${f}" >> "${bundle_file}"
  done
}

make_bundle() {
  local bundle_file=$1;
  local key_file=$2;
  shift 2;

  printf '' > "${bundle_file}"
  for f in $@; do
    openssl x509 -nameopt oneline -subject -issuer -noout -in "${TMPDIR}/${f}" >> "${bundle_file}"
  done
  for f in $@; do
    cat "${TMPDIR}/${f}" >> "${bundle_file}"
  done
  cat "${TMPDIR}/${key_file}" >> "${bundle_file}"
}

echo '100001' > "${TMPDIR}/certserial"

cat > "${TMPDIR}/openssl.cnf" <<EOF
[ca]
default_ca = fdb_test_ca

[req]
distinguished_name = req_distinguished_name

[req_distinguished_name]

[fdb_test_ca]
unique_subject = no
database = ${TMPDIR}/certindex
default_md = sha256
new_certs_dir = ${TMPDIR}/
policy = fdb_test_ca_policy
serial = ${TMPDIR}/certserial

[fdb_test_ca_policy]

[fdb_v3_ca]
subjectKeyIdentifier = hash
authorityKeyIdentifier = keyid:always,issuer
basicConstraints = critical, CA:true
keyUsage = critical, cRLSign, keyCertSign

[fdb_v3_other]
subjectKeyIdentifier = hash
authorityKeyIdentifier = keyid:always,issuer
basicConstraints = critical, CA:false
keyUsage = critical, digitalSignature

[fdb_v3_server_san]
subjectKeyIdentifier = hash
authorityKeyIdentifier = keyid:always,issuer
basicConstraints = critical, CA:false
keyUsage = critical, digitalSignature
subjectAltName = @fdb_v3_server_alt_names

[fdb_v3_server_alt_names]
DNS.1 = test.foundationdb.org
EOF

# Root CA 1.
openssl req -new -days 3650 -nodes -newkey rsa:2048 -sha256 -x509 \
  -subj "${SUBJECT} Root CA 1" -keyout "${TMPDIR}/ca-root-1.key" \
  -config "${TMPDIR}/openssl.cnf" -extensions fdb_v3_ca \
  -out "${TMPDIR}/ca-root-1.crt"

# Root CA 2.
openssl req -new -days 3650 -nodes -newkey rsa:2048 -sha256 -x509 \
  -subj "${SUBJECT_ALT} Root CA 2" -keyout "${TMPDIR}/ca-root-2.key" \
  -config "${TMPDIR}/openssl.cnf" -extensions fdb_v3_ca \
  -out "${TMPDIR}/ca-root-2.crt"

# Intermediate CA 1 (from CA 1).
openssl req -new -days 3650 -nodes -newkey rsa:2048 -sha256 \
  -subj "${SUBJECT} Intermediate CA 1" -keyout "${TMPDIR}/ca-int-1.key" \
  -out "${TMPDIR}/ca-int-1.csr"
openssl x509 -req -days 3650 -CA "${TMPDIR}/ca-root-1.crt" -CAkey "${TMPDIR}/ca-root-1.key" \
  -extfile "${TMPDIR}/openssl.cnf" -extensions fdb_v3_ca -days 3650 \
  -CAcreateserial -in "${TMPDIR}/ca-int-1.csr" -out "${TMPDIR}/ca-int-1.crt"

# Intermediate CA 2 (from CA 1).
openssl req -new -days 3650 -nodes -newkey rsa:2048 -sha256 \
  -subj "${SUBJECT} Intermediate CA 2" -keyout "${TMPDIR}/ca-int-2.key" \
  -out "${TMPDIR}/ca-int-2.csr"
openssl x509 -req -days 3650 -CA "${TMPDIR}/ca-root-1.crt" -CAkey "${TMPDIR}/ca-root-1.key" \
  -extfile "${TMPDIR}/openssl.cnf" -extensions fdb_v3_ca -days 3650 \
  -CAcreateserial -in "${TMPDIR}/ca-int-2.csr" -out "${TMPDIR}/ca-int-2.crt"

# Intermediate CA 3 (from CA 2).
openssl req -new -days 3650 -nodes -newkey rsa:2048 -sha256 \
  -subj "${SUBJECT} Intermediate CA 3" -keyout "${TMPDIR}/ca-int-3.key" \
  -out "${TMPDIR}/ca-int-3.csr"
openssl x509 -req -days 3650 -CA "${TMPDIR}/ca-root-2.crt" -CAkey "${TMPDIR}/ca-root-2.key" \
  -extfile "${TMPDIR}/openssl.cnf" -extensions fdb_v3_ca -days 3650 \
  -CAcreateserial -in "${TMPDIR}/ca-int-3.csr" -out "${TMPDIR}/ca-int-3.crt"

# Server 1.
openssl req -new -days 3650 -nodes -newkey rsa:2048 -sha256 \
  -subj "${SUBJECT} Server 1" -keyout "${TMPDIR}/server-1.key" \
  -out "${TMPDIR}/server-1.csr"
openssl x509 -req -days 3650 -CA "${TMPDIR}/ca-int-1.crt" -CAkey "${TMPDIR}/ca-int-1.key" \
  -extfile "${TMPDIR}/openssl.cnf" -extensions fdb_v3_server_san -days 3650 \
  -CAcreateserial -in "${TMPDIR}/server-1.csr" -out "${TMPDIR}/server-1.crt"

# Server 2.
openssl req -new -days 3650 -nodes -newkey rsa:2048 -sha256 \
  -subj "$(printf "${SUBJECT_ALT} Server 2, \200 <\001+\002=\003>")" -keyout "${TMPDIR}/server-2.key" \
  -out "${TMPDIR}/server-2.csr"
openssl x509 -req -days 3650 -CA "${TMPDIR}/ca-int-2.crt" -CAkey "${TMPDIR}/ca-int-2.key" \
  -extfile "${TMPDIR}/openssl.cnf" -extensions fdb_v3_other \
  -CAcreateserial -in "${TMPDIR}/server-2.csr" -out "${TMPDIR}/server-2.crt"

# Server 3 (expired).
openssl req -new -days 1 -nodes -newkey rsa:2048 -sha256 \
  -subj "${SUBJECT} Server 3" -keyout "${TMPDIR}/server-3.key" \
  -out "${TMPDIR}/server-3.csr"
cp /dev/null "${TMPDIR}/certindex"
printf "y\ny\n" | openssl ca -cert "${TMPDIR}/ca-int-1.crt" -keyfile "${TMPDIR}/ca-int-1.key" \
  -startdate 20170101000000Z -enddate 20171231000000Z \
  -config "${TMPDIR}/openssl.cnf" -notext \
  -in "${TMPDIR}/server-3.csr" -out "${TMPDIR}/server-3.crt"

# Server 4.
openssl genpkey -algorithm rsa -pkeyopt rsa_keygen_bits:2048 -aes128 -pass pass:fdb123 \
  -out "${TMPDIR}/server-4.key"
openssl req -new -days 3650 -sha256 -key "${TMPDIR}/server-4.key" -passin pass:fdb123 \
  -subj "${SUBJECT} Server 4" -out "${TMPDIR}/server-4.csr"
openssl x509 -req -days 3650 -CA "${TMPDIR}/ca-int-3.crt" -CAkey "${TMPDIR}/ca-int-3.key" \
  -extfile "${TMPDIR}/openssl.cnf" -extensions fdb_v3_other -days 3650 \
  -CAcreateserial -in "${TMPDIR}/server-4.csr" -out "${TMPDIR}/server-4.crt"

# Client 1.
openssl req -new -days 3650 -nodes -newkey rsa:2048 -sha256 \
  -subj "${SUBJECT} Client 1" -keyout "${TMPDIR}/client-1.key" \
  -out "${TMPDIR}/client-1.csr"
openssl x509 -req -days 3650 -CA "${TMPDIR}/ca-int-1.crt" -CAkey "${TMPDIR}/ca-int-1.key" \
  -extfile "${TMPDIR}/openssl.cnf" -extensions fdb_v3_other \
  -CAcreateserial -in "${TMPDIR}/client-1.csr" -out "${TMPDIR}/client-1.crt"

# Client 2.
openssl req -new -days 3650 -nodes -newkey rsa:2048 -sha256 \
  -subj "$(printf "${SUBJECT_ALT} Client 2, \200 <\001+\002=\003>")" -keyout "${TMPDIR}/client-2.key" \
  -out "${TMPDIR}/client-2.csr"
openssl x509 -req -days 3650 -CA "${TMPDIR}/ca-int-2.crt" -CAkey "${TMPDIR}/ca-int-2.key" \
  -extfile "${TMPDIR}/openssl.cnf" -extensions fdb_v3_other \
  -CAcreateserial -in "${TMPDIR}/client-2.csr" -out "${TMPDIR}/client-2.crt"

# Client 3 (expired).
openssl req -new -days 1 -nodes -newkey rsa:2048 -sha256 \
  -subj "${SUBJECT} Client 3" -keyout "${TMPDIR}/client-3.key" \
  -out "${TMPDIR}/client-3.csr"
cp /dev/null "${TMPDIR}/certindex"
printf "y\ny\n" | openssl ca -cert "${TMPDIR}/ca-int-1.crt" -keyfile "${TMPDIR}/ca-int-1.key" \
  -startdate 20170101000000Z -enddate 20171231000000Z \
  -config "${TMPDIR}/openssl.cnf" \
  -in "${TMPDIR}/client-3.csr" -out "${TMPDIR}/client-3.crt"

# Client 4.
openssl genpkey -algorithm rsa -pkeyopt rsa_keygen_bits:2048 -aes128 -pass pass:fdb321 \
  -out "${TMPDIR}/client-4.key"
openssl req -new -days 3650 -sha256 -key "${TMPDIR}/client-4.key" -passin pass:fdb321 \
  -subj "${SUBJECT} Client 4" -out "${TMPDIR}/client-4.csr"
openssl x509 -req -days 3650 -CA "${TMPDIR}/ca-int-3.crt" -CAkey "${TMPDIR}/ca-int-3.key" \
  -extfile "${TMPDIR}/openssl.cnf" -extensions fdb_v3_other \
  -CAcreateserial -in "${TMPDIR}/client-4.csr" -out "${TMPDIR}/client-4.crt"

#
# Test Bundles
#

make_ca_bundle 'test-ca-1.pem' 'ca-root-1.crt'
make_ca_bundle 'test-ca-2.pem' 'ca-root-2.crt'
make_ca_bundle 'test-ca-all.pem' 'ca-root-1.crt' 'ca-root-2.crt'

# Valid client/server from intermediate CA 1.
make_bundle 'test-client-1.pem' 'client-1.key' 'client-1.crt' 'ca-int-1.crt'
make_bundle 'test-server-1.pem' 'server-1.key' 'server-1.crt' 'ca-int-1.crt'

# Valid client/server from intermediate CA 2.
make_bundle 'test-client-2.pem' 'client-2.key' 'client-2.crt' 'ca-int-2.crt'
make_bundle 'test-server-2.pem' 'server-2.key' 'server-2.crt' 'ca-int-2.crt'

# Expired client/server from intermediate CA 1.
make_bundle 'test-client-3.pem' 'client-3.key' 'client-3.crt' 'ca-int-1.crt'
make_bundle 'test-server-3.pem' 'server-3.key' 'server-3.crt' 'ca-int-1.crt'

# Valid client/server from intermediate CA 3.
make_bundle 'test-client-4.pem' 'client-4.key' 'client-4.crt' 'ca-int-3.crt'
make_bundle 'test-server-4.pem' 'server-4.key' 'server-4.crt' 'ca-int-3.crt'
