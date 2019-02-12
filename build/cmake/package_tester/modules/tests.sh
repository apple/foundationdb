#!/usr/bin/env bash

if [ -z "${tests_sh_included}" ]
then
   tests_sh_included=1

   sourcce ${source_dir}/modules/util.sh

   tests_healthy() {
       cd /
       fdbcli --exec status
        if [ $? -ne 0 ]
        then
            return 1
        fi
        healthy="$(fdbcli --exec status | grep HEALTHY | wc -l)"
        if [ -z "${healthy}" ]
        then
            __res=1
            break
        fi
   }

   tests_main() {
       install
       success Installation failed
       tests_healthy
       success FoundationDB is not healthy or not running
       uninstall
       success FoundationDB could not be uninstalled
   }
fi
