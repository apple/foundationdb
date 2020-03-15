#!/usr/bin/env bash

set -e

# we first check whether the user is in the group docker
user=$(id -un)
DIR_UUID=$(cat /dev/urandom | tr -dc 'a-zA-Z0-9' | fold -w 32 | head -n 1)
group=$(id -gn)
uid=$(id -u)
gid=$(id -g)
gids=( $(id -G) )
groups=( $(id -Gn) )
tmpdir="/tmp/fdb-docker-${DIR_UUID}"
image=fdb-dev

pushd .
mkdir ${tmpdir}
cd ${tmpdir}

echo

cat <<EOF >> Dockerfile
FROM foundationdb/foundationdb-build:latest
EOF

num_groups=${#gids[@]}
additional_groups=""
for ((i=0;i<num_groups;i++))
do
        echo "RUN groupadd -g ${gids[$i]} ${groups[$i]}" >> Dockerfile
        if [ ${gids[i]} -ne ${gid} ]
        then
                if [ -z "${additional_groups}" ]
                then
                        additional_groups="-G ${gids[$i]}"
                else
                        additional_groups="${additional_groups},${gids[$i]}"
                fi
        fi
done

cat <<EOF >> Dockerfile
RUN useradd -u ${uid} -g ${gid} ${additional_groups} -m ${user}

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

if [ -d "\${CCACHE_DIR}" ]
then
        args="-v \${CCACHE_DIR}:\${CCACHE_DIR}"
        args="\${args} -e CCACHE_DIR=\${CCACHE_DIR}"
        args="\${args} -e CCACHE_UMASK=\${CCACHE_UMASK}"
        ccache_args=\$args
fi


sudo docker run --rm `# delete (temporary) image after return` \\
                -it `# Run in interactive mode and simulate a TTY` \\
                --privileged=true `# Run in privileged mode ` \\
                --cap-add=SYS_PTRACE \\
                --security-opt seccomp=unconfined \\
                -v "${HOME}:${HOME}" `# Mount home directory` \\
                \${ccache_args} \\
                ${image}
EOF

chmod +x $HOME/bin/fdb-dev
echo "To start the dev docker image run $HOME/bin/fdb-dev"
echo "You can edit this file but be aware that this script will overwrite your changes if you rerun it"
