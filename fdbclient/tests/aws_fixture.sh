#!/bin/bash
#
# Functions for dealing w/ aws/s3
#

# Cleanup any mess we've made. For calling from signal trap on exit.
# $1 The aws scratch directory to clean up on exit.
function shutdown_aws {
  local scratch_dir="${1}"
  if [[ -d "${scratch_dir}" ]]; then
    rm -rf "${scratch_dir}"
  fi
}

# Create directory for aws test to use writing temporary data.
# Call shutdown_aws to clean up the directory created here for test.
# $1 Directory where we want weed to write data and logs.
# check $? error code on return.
function create_aws_dir {
  local dir="${1}"
  local aws_dir
  aws_dir=$(mktemp -d -p "${dir}" -t s3.XXXX)
  # Exit if the temp directory wasn't created successfully.
  if [[ ! -d "${aws_dir}" ]]; then
    echo "ERROR: Failed create of aws  directory ${aws_dir}" >&2
    return 1
  fi
  echo "${aws_dir}"
}

# Write out blob_credentials
# $1 The token to use querying credentials
# $2 Hostname going to aws
# $3 The scratch dir to write the blob credentials file into
# Echos the blob_credentials file path.
function write_blob_credentials {
  local token="${1}"
  local local_host="${2}"
  local dir="${3}"
  if ! credentials=$( curl -H "X-aws-ec2-metadata-token: ${token}" \
      http://169.254.169.254/latest/meta-data/iam/security-credentials/foundationdb-dev_node_instance_role ); then
    echo "ERROR: Failed reading credentials"
    return 1
  fi
  if ! blob_credentials_str=$( echo "${credentials}" | jq --arg host_arg "${local_host}" \
      '{"accounts": { ($host_arg): {"api_key": .AccessKeyId, "secret": .SecretAccessKey, "token": .Token}}}'); then
    echo "ERROR: Failed jq'ing ${blob_credentials_str}"
    return 1
  fi
  readonly blob_credentials_file="${dir}/blob_credentials.json"
  echo "${blob_credentials_str}" > "${blob_credentials_file}"
  echo "${blob_credentials_file}"
}

# Check if curl is installed
if ! command -v curl &> /dev/null; then
  echo "ERROR: curl is not installed. Please install it to use this script." >&2
  exit 1
fi
# Check if curl is installed
if ! command -v openssl &> /dev/null; then
  echo "ERROR: openssl is not installed. Please install it to use this script." >&2
  exit 1
fi
# Check if jq is installed.
if ! command -v jq &> /dev/null; then
  echo "ERROR: jq is not installed. Please install it to use this script generating json credentials file." >&2
  exit 1
fi
