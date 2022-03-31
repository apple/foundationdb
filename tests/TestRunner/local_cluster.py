from pathlib import Path
from argparse import ArgumentParser
import random
import string
import subprocess
import sys
import socket


def _get_free_port_internal():
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.bind(('0.0.0.0', 0))
        return s.getsockname()[1]


_used_ports = set()


def get_free_port():
    global _used_ports
    port = _get_free_port_internal()
    while port in _used_ports:
        port = _get_free_port_internal()
    _used_ports.add(port)
    return port


class LocalCluster:
    configuration_template = """
## foundationdb.conf
##
## Configuration file for FoundationDB server processes
## Full documentation is available at
## https://apple.github.io/foundationdb/configuration.html#the-configuration-file

[fdbmonitor]

[general]
restart-delay = 10
## by default, restart-backoff = restart-delay-reset-interval = restart-delay
# initial-restart-delay = 0
# restart-backoff = 60
# restart-delay-reset-interval = 60
cluster-file = {etcdir}/fdb.cluster
# delete-envvars =
# kill-on-configuration-change = true

## Default parameters for individual fdbserver processes
[fdbserver]
command = {fdbserver_bin}
public-address = auto:$ID
listen-address = public
datadir = {datadir}/$ID
logdir = {logdir}
# logsize = 10MiB
# maxlogssize = 100MiB
# machine-id =
# datacenter-id =
# class =
# memory = 8GiB
# storage-memory = 1GiB
# cache-memory = 2GiB
# metrics-cluster =
# metrics-prefix =

## An individual fdbserver process with id 4000
## Parameters set here override defaults from the [fdbserver] section

"""

    valid_letters_for_secret = string.ascii_letters + string.digits

    def __init__(self, basedir: str, fdbserver_binary: str, fdbmonitor_binary: str,
                 fdbcli_binary: str, process_number: int, create_config=True, port=None, ip_address=None):
        self.basedir = Path(basedir)
        self.fdbserver_binary = Path(fdbserver_binary)
        self.fdbmonitor_binary = Path(fdbmonitor_binary)
        self.fdbcli_binary = Path(fdbcli_binary)
        for b in (self.fdbserver_binary, self.fdbmonitor_binary, self.fdbcli_binary):
            assert b.exists(), "{} does not exist".format(b)
        if not self.basedir.exists():
            self.basedir.mkdir()
        self.etc = self.basedir.joinpath('etc')
        self.log = self.basedir.joinpath('log')
        self.data = self.basedir.joinpath('data')
        self.etc.mkdir(exist_ok=True)
        self.log.mkdir(exist_ok=True)
        self.data.mkdir(exist_ok=True)
        self.port = get_free_port() if port is None else port
        self.ip_address = '127.0.0.1' if ip_address is None else ip_address
        self.running = False
        self.process = None
        self.fdbmonitor_logfile = None
        if create_config:
            with open(self.etc.joinpath('fdb.cluster'), 'x') as f:
                random_string = lambda len : ''.join(random.choice(LocalCluster.valid_letters_for_secret) for i in range(len))
                f.write('{desc}:{secret}@{ip_addr}:{server_port}'.format(
                    desc=random_string(8),
                    secret=random_string(8),
                    ip_addr=self.ip_address,
                    server_port=self.port
                ))
                with open(self.etc.joinpath('foundationdb.conf'), 'x') as f:
                    f.write(LocalCluster.configuration_template.format(
                        etcdir=self.etc,
                        fdbserver_bin=self.fdbserver_binary,
                        datadir=self.data,
                        logdir=self.log
                    ))
                    # By default, the cluster only has one process
                    # If a port number is given and process_number > 1, we will use subsequent numbers
                    # E.g., port = 4000, process_number = 5
                    # Then 4000,4001,4002,4003,4004 will be used as ports
                    # If port number is not given, we will randomly pick free ports
                    for index, _ in enumerate(range(process_number)):
                        f.write('[fdbserver.{server_port}]\n'.format(server_port=self.port))
                        self.port = get_free_port() if port is None else str(int(self.port) + 1)

    def __enter__(self):
        assert not self.running, "Can't start a server that is already running"
        args = [str(self.fdbmonitor_binary),
                '--conffile',
                str(self.etc.joinpath('foundationdb.conf')),
                '--lockfile',
                str(self.etc.joinpath('fdbmonitor.lock'))]
        self.fdbmonitor_logfile = open(self.log.joinpath('fdbmonitor.log'), 'w')
        self.process = subprocess.Popen(args, stdout=self.fdbmonitor_logfile, stderr=self.fdbmonitor_logfile)
        self.running = True
        return self

    def __exit__(self, xc_type, exc_value, traceback):
        assert self.running, "Server is not running"
        if self.process.poll() is None:
            self.process.terminate()
        self.running = False

    def create_database(self, storage='ssd'):
        args = [self.fdbcli_binary, '-C', self.etc.joinpath('fdb.cluster'), '--exec',
                'configure new single {} tenant_mode=optional_experimental'.format(storage)]
        subprocess.run(args)
