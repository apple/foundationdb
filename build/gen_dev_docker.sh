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
FROM foundationdb/foundationdb-dev:0.11.1
RUN yum install -y sudo
RUN echo '%sudo ALL=(ALL) NOPASSWD:ALL' >> /etc/sudoers
RUN groupadd -g 1100 sudo
EOF

num_groups=${#gids[@]}
additional_groups="-G sudo"
for ((i=0;i<num_groups;i++))
do
        echo "RUN groupadd -g ${gids[$i]} ${groups[$i]} || true" >> Dockerfile
        if [ ${gids[i]} -ne ${gid} ]
        then
                additional_groups="${additional_groups},${gids[$i]}"
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

if [ -t 1 ] ; then
    TERMINAL_ARGS=-it `# Run in interactive mode and simulate a TTY`
else
    TERMINAL_ARGS=-i `# Run in interactive mode`
fi

sudo docker run --rm `# delete (temporary) image after return` \\
                \${TERMINAL_ARGS}  \\
                --privileged=true `# Run in privileged mode ` \\
                --cap-add=SYS_PTRACE \\
                --security-opt seccomp=unconfined \\
                -v "${HOME}:${HOME}" `# Mount home directory` \\
                -w="\$(pwd)" \\
                \${ccache_args} \\
                ${image} "\$@"
EOF

cat <<EOF > $HOME/bin/clangd
#!/usr/bin/bash

fdb-dev scl enable devtoolset-8 rh-python36 rh-ruby24 -- clangd
EOF

if [[ ":$PATH:" != *":$HOME/bin:"* ]]
then
        echo "WARNING: $HOME/bin is not in your PATH!"
        echo -e "\tThis can cause problems with some scripts (like fdb-clangd)"
fi
chmod +x $HOME/bin/fdb-dev
chmod +x $HOME/bin/clangd
echo "To start the dev docker image run $HOME/bin/fdb-dev"
echo "$HOME/bin/clangd can be used for IDE integration"
echo "You can edit these files but be aware that this script will overwrite your changes if you rerun it"
