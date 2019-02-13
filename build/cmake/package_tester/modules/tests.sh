#!/usr/bin/env bash

declare -A test_start_state
declare -A test_exit_state
declare -a tests

if [ -z "${tests_sh_included}" ]
then
   tests_sh_included=1

   source ${source_dir}/modules/util.sh
   source ${source_dir}/modules/testing.sh

   tests=( "fresh_install" "keep_config" )
   test_start_state=([fresh_install]=INSTALLED [keep_config]=CLEAN )

   fresh_install() {
       tests_healthy
       success Fresh installation is not clean
       # test that we can read from and write to fdb
       cd /
       fdbcli --exec 'writemode on ; set foo bar'
       success Cannot write to database
       getresult="$(fdbcli --exec 'get foo')"
       success Get on database failed
       if [ "${getresult}" != "\`foo' is \`bar'" ]
       then
          fail "value was not set correctly"
       fi
       fdbcli --exec 'writemode on ; clear foo'
       success Deleting value failed
       getresult="$(fdbcli --exec 'get foo')"
       success Get on database failed
       if [ "${getresult}" != "\`foo': not found" ]
       then
          fail "value was not cleared correctly"
       fi
   }

   keep_config() {
       mkdir /etc/foundationdb
       description=$(LC_CTYPE=C tr -dc A-Za-z0-9 < /dev/urandom | head -c 8)
       random_str=$(LC_CTYPE=C tr -dc A-Za-z0-9 < /dev/urandom | head -c 8)
       success Could not create secret
       echo $description:$random_str@127.0.0.1:4500 > /tmp/fdb.cluster
       success Could not create fdb.cluster file
       sed '/\[fdbserver.4500\]/a \[fdbserver.4501\]' /foundationdb/packaging/foundationdb.conf > /tmp/foundationdb.conf
       success Could not change foundationdb.conf file
       # we need to keep these files around for testing that the install didn't change them
       cp /tmp/fdb.cluster /etc/foundationdb/fdb.cluster
       cp /tmp/foundationdb.conf /etc/foundationdb/foundationdb.conf

       install
       success FoundationDB install failed
       # make sure we are not in build directory as there is a fdbc.cluster file there
       echo "Configure new database - Install isn't supposed to do this for us"
       echo "as there was an existing configuration"
       cd /
       fdbcli --exec 'configure new single ssd'
       success "Couldn't" configure new database
       tests_healthy
       num_processes="$(fdbcli --exec 'status' | grep "FoundationDB processes" | sed -e 's/.*- //')"
       if [ "${num_processes}" -ne 2 ]
       then
          ?=2
          success Number of processes incorrect after config change
       fi

       differences="$(diff /tmp/fdb.cluster /etc/foundationdb/fdb.cluster)"
       if [ -n "${differences}" ]
       then
          ?=1
          success Install changed configuration files
       fi
       differences="$(diff /tmp/foundationdb.conf /etc/foundationdb/foundationdb.conf)"
       if [ -n "${differences}" ]
       then
          ?=1
          success Install changed configuration files
       fi

       uninstall
       # make sure config didn't get deleted
       if [ ! -f /etc/foundationdb/fdb.cluster ] || [ ! -f /etc/foundationdb/foundationdb.conf ]
       then
          fail "Uninstall removed configuration"
       fi

       rm /tmp/fdb.cluster
       rm /tmp/foundationdb.conf
       return 0
   }
fi
