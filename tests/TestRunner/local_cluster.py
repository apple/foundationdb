from pathlib import Path
from argparse import ArgumentParser
import random
import string
import subprocess
import sys
import socket


def get_free_port():
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.bind(('0.0.0.0', 0))
        return s.getsockname()[1]


class LocalCluster:
    configuration_template = """
## foundationdb.conf
##
## Configuration file for FoundationDB server processes
## Full documentation is available at
## https://apple.github.io/foundationdb/configuration.html#the-configuration-file

[fdbmonitor]

[general]
restart_delay = 10
## by default, restart_backoff = restart_delay_reset_interval = restart_delay
# initial_restart_delay = 0
# restart_backoff = 60
# restart_delay_reset_interval = 60
cluster_file = {etcdir}/fdb.cluster
# delete_envvars =
# kill_on_configuration_change = true

## Default parameters for individual fdbserver processes
[fdbserver]
command = {fdbserver_bin}
public_address = auto:$ID
listen_address = public
datadir = {datadir}/$ID
logdir = {logdir}
# logsize = 10MiB
# maxlogssize = 100MiB
# machine_id =
# datacenter_id =
# class =
# memory = 8GiB
# storage_memory = 1GiB
# cache_memory = 2GiB
# metrics_cluster =
# metrics_prefix =

## An individual fdbserver process with id 4000
## Parameters set here override defaults from the [fdbserver] section
[fdbserver.{server_port}]
    """

    valid_letters_for_secret = string.ascii_letters + string.digits

    def __init__(self, basedir: str, fdbserver_binary: str, fdbmonitor_binary: str,
                 fdbcli_binary: str, create_config=True, port=None, ip_address=None):
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
                        logdir=self.log,
                        server_port=self.port
                    ))

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
                'configure new single {}'.format(storage)]
        subprocess.run(args)
