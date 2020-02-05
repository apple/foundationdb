#!/usr/bin/env bash

# In this file the tests are formulated which
# should run in the docker container to test
# whether the RPM and DEB packages work properly.
#
# In order to add a test, a user first has to
# add the name of the test to the `tests` array
# which is defined in this file.
#
# Then, she must define the state this test
# expects the container to be in. To do that,
# a value for the test has to be added to the
# associative array `test_start_state`. Valid
# values are:
#
#  - INSTALLED: In this case, the test will be
#    started with a freshly installed FDB, but
#    no other changes were made to the container.
#  - CLEAN: This simply means that the container
#    will run a minimal version of the OS (as defined
#    in the corresponsing Dockerfile)
#
# A test is then simply a bash function with the
# same name as the test. It can use the predefined
# bash functions `install` and `uninstall` to either
# install or uninstall FDB on the container. The FDB
# build directory can be found in `/build`, the
# source code will be located in `/foundationdb`

if [ -z "${tests_sh_included+x}" ]
then
   tests_sh_included=1

   source ${source_dir}/modules/util.sh
   source ${source_dir}/modules/testing.sh

   tests=( "fresh_install" "keep_config" )
   test_start_state=([fresh_install]=INSTALLED [keep_config]=CLEAN )

   fresh_install() {
       tests_healthy
       successOr "Fresh installation is not clean"
       # test that we can read from and write to fdb
       cd /
       timeout 2 fdbcli --exec 'writemode on ; set foo bar'
       successOr "Cannot write to database"
       getresult="$(timeout 2 fdbcli --exec 'get foo')"
       successOr "Get on database failed"
       if [ "${getresult}" != "\`foo' is \`bar'" ]
       then
          fail "value was not set correctly"
       fi
       timeout 2 fdbcli --exec 'writemode on ; clear foo'
       successOr "Deleting value failed"
       getresult="$(timeout 2 fdbcli --exec 'get foo')"
       successOr "Get on database failed"
       if [ "${getresult}" != "\`foo': not found" ]
       then
          fail "value was not cleared correctly"
       fi
       PYTHON_TARGZ_NAME="$(ls /build/packages | grep 'foundationdb-[0-9.]*\.tar\.gz' | sed 's/\.tar\.gz$//')"
       tar -C /tmp -xvf /build/packages/${PYTHON_TARGZ_NAME}.tar.gz
       pushd /tmp/${PYTHON_TARGZ_NAME}
       python setup.py install
       successOr "Installing python bindings failed"
       popd
       python -c 'import fdb; fdb.api_version(700)'
       successOr "Loading python bindings failed"

       # Test cmake and pkg-config integration: https://github.com/apple/foundationdb/issues/1483
       install_build_tools
       rm -rf build-fdb_c_app
       mkdir build-fdb_c_app
       pushd build-fdb_c_app
       cmake /foundationdb/build/cmake/package_tester/fdb_c_app && make
       successOr "FoundationDB-Client cmake integration failed"
       cc /foundationdb/build/cmake/package_tester/fdb_c_app/app.c `pkg-config --libs --cflags foundationdb-client`
       successOr "FoundationDB-Client pkg-config integration failed"
       popd
   }

   keep_config() {
       mkdir /etc/foundationdb
       description=$(LC_CTYPE=C tr -dc A-Za-z0-9 < /dev/urandom | head -c 8)
       random_str=$(LC_CTYPE=C tr -dc A-Za-z0-9 < /dev/urandom | head -c 8)
       successOr "Could not create secret"
       echo $description:$random_str@127.0.0.1:4500 > /tmp/fdb.cluster
       successOr "Could not create fdb.cluster file"
       sed '/\[fdbserver.4500\]/a \[fdbserver.4501\]' /foundationdb/packaging/foundationdb.conf > /tmp/foundationdb.conf
       successOr "Could not change foundationdb.conf file"
       # we need to keep these files around for testing that the install didn't change them
       cp /tmp/fdb.cluster /etc/foundationdb/fdb.cluster
       cp /tmp/foundationdb.conf /etc/foundationdb/foundationdb.conf

       install
       successOr "FoundationDB install failed"
       # make sure we are not in build directory as there is a fdbc.cluster file there
       echo "Configure new database - Install isn't supposed to do this for us"
       echo "as there was an existing configuration"
       cd /
       timeout 2 fdbcli --exec 'configure new single ssd'
       successOr "Couldn't configure new database"
       tests_healthy
       num_processes="$(timeout 2 fdbcli --exec 'status' | grep "FoundationDB processes" | sed -e 's/.*- //')"
       if [ "${num_processes}" -ne 2 ]
       then
          fail Number of processes incorrect after config change
       fi

       differences="$(diff /tmp/fdb.cluster /etc/foundationdb/fdb.cluster)"
       if [ -n "${differences}" ]
       then
          fail Install changed configuration files
       fi
       differences="$(diff /tmp/foundationdb.conf /etc/foundationdb/foundationdb.conf)"
       if [ -n "${differences}" ]
       then
          fail Install changed configuration files
       fi

       uninstall
       # make sure config didn't get deleted
       # RPM, however, renames the file on remove, so we need to check for this
       conffile="/etc/foundationdb/foundationdb.conf${conf_save_extension}"
       if [ ! -f /etc/foundationdb/fdb.cluster ] || [ ! -f "${conffile}" ]
       then
          fail "Uninstall removed configuration"
       fi
       differences="$(diff /tmp/foundationdb.conf ${conffile})"
       if [ -n "${differences}" ]
       then
          fail "${conffile} changed during remove"
       fi
       differences="$(diff /tmp/fdb.cluster /etc/foundationdb/fdb.cluster)"
       if [ -n "${differences}" ]
       then
          fail "/etc/foundationdb/fdb.cluster changed during remove"
       fi

       return 0
   }
fi
