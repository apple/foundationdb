#!/usr/bin/env bash

set -e

DIR_UUID=$(cat /dev/urandom | tr -dc 'a-zA-Z0-9' | fold -w 32 | head -n 1)
user=$(id -un)
group=$(id -gn)
uid=$(id -u)
gid=$(id -g)
tmpdir="/tmp/fdb-docker-${DIR_UUID}"
image=fdb-dev

pushd .
mkdir ${tmpdir}
cd ${tmpdir}

cat <<EOF >> Dockerfile
FROM foundationdb/foundationdb-build:latest
RUN groupadd -g ${gid} ${group} && useradd -u ${uid} -g ${gid} -m ${user}

USER ${user}
CMD scl enable devtoolset-8 rh-python36 rh-ruby24 -- bash

EOF

echo "Created ${tmpdir}"
echo "Buidling Docker container ${image}"
sudo docker build -t ${image} .

popd

echo "Writing startup script"
mkdir -p $HOME/bin
cat <<EOF > $HOME/bin/fdb-dev
#!/usr/bin/bash

sudo docker run --rm `# delete (temporary) image after return` \\
                -it `# Run in interactive mode and simulate a TTY` \\
                --privileged=true `# Run in privileged mode ` \\
                --cap-add=SYS_PTRACE \\
                --security-opt seccomp=unconfined \\
                -v '${HOME}:${HOME}' `# Mount home directory` \\
                -e "CCACHE_DIR=$CCACHE_DIR" \\
                -e "CCACHE_UMASK=$CCACHE_UMASK" \\
                ${image}
EOF

chmod +x $HOME/bin/fdb-dev
echo "To start the dev docker image run $HOME/bin/fdb-dev"
echo "You can edit this file but be aware that this script will overwrite your changes if you rerun it"
