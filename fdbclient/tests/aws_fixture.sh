#!/bin/bash
#
# Functions for dealing w/ aws/s3
#
# Here is how to use this fixture:
#  # First source this fixture.
#  if ! source "${cwd}/aws_fixture.sh"; then
#    err "Failed to source aws_fixture.sh"
#    exit 1
#  fi
#  # Save off the return from create_aws_dir. You'll need it for shutdown.
#  if ! TEST_SCRATCH_DIR=$( create_aws_dir "${scratch_dir}" ); then
#    err "Failed creating local aws_dir"
#    exit 1
#  fi
#  readonly TEST_SCRATCH_DIR
#  if ! readarray -t configs < <(aws_setup "${TEST_SCRATCH_DIR}"); then
#    err "Failed aws_setup"
#    return 1
#  fi
#  readonly host="${configs[0]}"
#  ...etc.
#  # When done, call shutdown_aws
#  shutdown_aws "${TEST_SCRATCH_DIR}"
#

# Cleanup any mess we've made. For calling from signal trap on exit.
# $1 The aws scratch directory to clean up on exit.
function shutdown_aws {
  local local_scratch_dir="${1}"
  if [[ -d "${local_scratch_dir}" ]]; then
    rm -rf "${local_scratch_dir}"
  fi
}

# Create directory for aws test to use writing temporary data.
# Call shutdown_aws to clean up the directory created here for test.
# $1 Directory where we want weed to write data and logs.
# check $? error code on return.
function create_aws_dir {
  local dir="${1}"
  local aws_dir
  aws_dir=$(mktemp -d -p "${dir}" -t s3.$$.XXXX)
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


# Set up s3 access.
# $1 aws_dir (This is what is returned when you call create_aws_dir
# so call it first).
# Returns array of configurations to use contacting s3.
function aws_setup {
  local local_aws_dir="${1}"
  # Fetch token, region, etc. from our aws environment.
  # On 169.254.169.254, see
  # https://www.baeldung.com/linux/cloud-ip-meaning#169254169254-and-other-link-local-addresses-on-the-cloud
  if ! imdsv2_token=$(curl -X PUT "http://169.254.169.254/latest/api/token" \
      -H "X-aws-ec2-metadata-token-ttl-seconds: 21600"); then
    err "Failed reading token"
    exit 1
  fi
  readonly imdsv2_token
  if ! region=$( curl -H "X-aws-ec2-metadata-token: ${imdsv2_token}" \
      "http://169.254.169.254/latest/meta-data/placement/region"); then
    err "Failed reading region"
    exit 1
  fi
  readonly region
  if ! account_id=$( aws --output text sts get-caller-identity --query 'Account' ); then
    err "Failed reading account id"
    exit 1
  fi
  readonly account_id
  readonly bucket="backup-${account_id}-${region}"
  # Add the '@' in front so we force reading of credentials file when s3
  # When we do lookup for credentials, we don't expect a port but it is expected later going via proxy.
  readonly host="@s3.${region}.amazonaws.com"
  if ! blob_credentials_file=$(write_blob_credentials "${imdsv2_token}" "${host}" "${local_aws_dir}"); then
    err "Failed to write credentials file"
    exit 1
  fi
  results_array=("${host}" "${bucket}" "${blob_credentials_file}" "${region}")
  printf "%s\n" "${results_array[@]}"
}

# Check bash version. We require 4.0 or greater. Mac os x is old -- 3.x.
if ((BASH_VERSINFO[0] < 4)); then
    echo "Error: This script requires Bash 4.0 or newer." >&2
    echo "Current version: $BASH_VERSION" >&2
    if [[ "$OSTYPE" == "darwin"* ]]; then
        echo "On macOS, you can install a newer version of Bash using Homebrew:" >&2
        echo "    brew install bash" >&2
        echo "Then change your shell:" >&2
        echo "    sudo bash -c 'echo /usr/local/bin/bash >> /etc/shells'" >&2
        echo "    chsh -s /usr/local/bin/bash" >&2
    fi
    exit 1
fi
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
