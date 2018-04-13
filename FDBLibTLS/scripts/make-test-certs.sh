#!/bin/sh

set -e
set -u

readonly SUBJECT="/C=US/ST=California/L=Cupertino/O=Apple Inc./OU=FDB Team/CN=FDB LibTLS Plugin Test"
readonly SUBJECT_ALT="/C=AU/ST=New South Wales/L=Sydney/O=Apple Pty Limited/OU=FDB Team/CN=FDB LibTLS Plugin Test"

readonly TMPDIR=$(mktemp -d)

cleanup() {
        rm -rf "${TMPDIR}"
}

trap cleanup EXIT INT

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
EOF

# Root CA.
openssl req -new -days 3650 -nodes -newkey rsa:2048 -sha256 -x509 \
  -subj "${SUBJECT} Root CA" -keyout "${TMPDIR}/ca-root.key" \
  -config "${TMPDIR}/openssl.cnf" -extensions fdb_v3_ca \
  -out "${TMPDIR}/ca-root.crt"

# Intermediate CA 1.
openssl req -new -days 3650 -nodes -newkey rsa:2048 -sha256 \
  -subj "${SUBJECT} Intermediate CA 1" -keyout "${TMPDIR}/ca-int-1.key" \
  -out "${TMPDIR}/ca-int-1.csr"
openssl x509 -req -days 3650 -CA "${TMPDIR}/ca-root.crt" -CAkey "${TMPDIR}/ca-root.key" \
  -extfile "${TMPDIR}/openssl.cnf" -extensions fdb_v3_ca -days 3650 \
  -CAcreateserial -in "${TMPDIR}/ca-int-1.csr" -out "${TMPDIR}/ca-int-1.crt"

# Intermediate CA 2.
openssl req -new -days 3650 -nodes -newkey rsa:2048 -sha256 \
  -subj "${SUBJECT} Intermediate CA 2" -keyout "${TMPDIR}/ca-int-2.key" \
  -out "${TMPDIR}/ca-int-2.csr"
openssl x509 -req -days 3650 -CA "${TMPDIR}/ca-root.crt" -CAkey "${TMPDIR}/ca-root.key" \
  -extfile "${TMPDIR}/openssl.cnf" -extensions fdb_v3_ca -days 3650 \
  -CAcreateserial -in "${TMPDIR}/ca-int-2.csr" -out "${TMPDIR}/ca-int-2.crt"

# Server 1.
openssl req -new -days 3650 -nodes -newkey rsa:2048 -sha256 \
  -subj "${SUBJECT} Server 1" -keyout "${TMPDIR}/server-1.key" \
  -out "${TMPDIR}/server-1.csr"
openssl x509 -req -days 3650 -CA "${TMPDIR}/ca-int-1.crt" -CAkey "${TMPDIR}/ca-int-1.key" \
  -extfile "${TMPDIR}/openssl.cnf" -extensions fdb_v3_other -days 3650 \
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

#
# Test Bundles
#

make_bundle 'test-1-server.pem' 'server-1.key' 'server-1.crt' 'ca-int-1.crt' 'ca-root.crt'
make_bundle 'test-1-client.pem' 'client-1.key' 'client-1.crt' 'ca-int-1.crt' 'ca-root.crt'
make_bundle 'test-2-server.pem' 'server-2.key' 'server-2.crt' 'ca-int-2.crt' 'ca-root.crt'
make_bundle 'test-2-client.pem' 'client-2.key' 'client-2.crt' 'ca-int-2.crt' 'ca-root.crt'

# Expired client/server.
make_bundle 'test-3-client.pem' 'client-3.key' 'client-3.crt' 'ca-int-1.crt' 'ca-root.crt'
make_bundle 'test-3-server.pem' 'server-3.key' 'server-3.crt' 'ca-int-1.crt' 'ca-root.crt'

# Bundles that terminate at intermediate 1.
make_bundle 'test-4-server.pem' 'server-1.key' 'server-1.crt' 'ca-int-1.crt'
make_bundle 'test-4-client.pem' 'client-1.key' 'client-1.crt' 'ca-int-1.crt'

# Bundles that terminate at intermediate 2.
make_bundle 'test-5-server.pem' 'server-2.key' 'server-2.crt' 'ca-int-2.crt'
make_bundle 'test-5-client.pem' 'client-2.key' 'client-2.crt' 'ca-int-2.crt'
