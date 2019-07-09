#!/usr/bin/env bash

if [ -z "${rpm_sh_included}" ]
then
   rpm_sh_included=1

   source ${source_dir}/modules/util.sh

   conf_save_extension=".rpmsave"

   install_build_tools() {
       yum -y install cmake gcc
   }

   install() {
       local __res=0
       enterfun
       cd /build/packages
       package_names=()
       for f in "${package_files[@]}"
       do
           package_names+=( "$(rpm -qp ${f})" )
       done
       yum install -y ${package_files[@]}
       __res=$?
       # give the server some time to come up
       sleep 5
       exitfun
       return ${__res}
   }

   uninstall() {
       local __res=0
       enterfun
       if [ "$1" == "purge" ]
       then
           yum remove --purge -y ${package_names[@]}
       else
           yum remove -y ${package_names[@]}
       fi
       __res=$?
       exitfun
       return ${__res}
   }
fi
