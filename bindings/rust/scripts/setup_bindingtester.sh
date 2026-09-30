#!/usr/bin/env bash

set -ex;

fdb_rs_dir=$(pwd)
bindingtester="${fdb_rs_dir:?}/$1"

pip install foundationdb==7.4.3
fdboption_file="$(pip show foundationdb | grep Loca | awk '{print $2}')/fdb/fdboptions.py"
echo "fdb option file: $fdboption_file"

python3 --version

## build the python bindings
(
  fdb_builddir=${fdb_rs_dir:?}/target/foundationdb_build
  mkdir -p ${fdb_builddir:?}
  cd ${fdb_builddir:?}

  ## Get foundationdb source
  git clone --depth 1 https://github.com/apple/foundationdb.git -b release-7.4
  cd foundationdb
  git checkout release-7.4

  # Instead of building fdb-python with ninja/cmake, patching it with pip install
  cp ${fdboption_file} ./bindings/python/fdb/fdboptions.py
  echo "LATEST_API_VERSION = 740" >> ./bindings/python/fdb/apiversion.py
  echo "FDB_VERSION = '7.4.3'" >> ./bindings/python/fdb/apiversion.py

  # Tenants are not supported by this binding.
  echo "testers['rust'] = Tester('rust', '${bindingtester}', 2040, 23, 740, types=ALL_TYPES)
" >> ./bindings/bindingtester/known_testers.py
)

cd "${fdb_rs_dir}";
