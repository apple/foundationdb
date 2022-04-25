import json
from pathlib import Path
import random
import string
import subprocess
import os
import socket
import time


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


def is_port_in_use(port):
    import socket
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        return s.connect_ex(('localhost', port)) == 0


valid_letters_for_secret = string.ascii_letters + string.digits


def random_secret_string(len):
    return ''.join(random.choice(valid_letters_for_secret) for i in range(len))


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
public-address = {ip_address}:$ID
listen-address = public
datadir = {datadir}/$ID
logdir = {logdir}
{bg_knob_line}
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

    def __init__(self, basedir: str, fdbserver_binary: str, fdbmonitor_binary: str, fdbcli_binary: str,
                 process_number: int, create_config=True, port=None, ip_address=None, blob_granules_enabled: bool=False):
        self.basedir = Path(basedir)
        self.etc = self.basedir.joinpath('etc')
        self.log = self.basedir.joinpath('log')
        self.data = self.basedir.joinpath('data')
        self.conf_file = self.etc.joinpath('foundationdb.conf')
        self.cluster_file = self.etc.joinpath('fdb.cluster')
        self.fdbserver_binary = Path(fdbserver_binary)
        self.fdbmonitor_binary = Path(fdbmonitor_binary)
        self.fdbcli_binary = Path(fdbcli_binary)
        for b in (self.fdbserver_binary, self.fdbmonitor_binary, self.fdbcli_binary):
            assert b.exists(), "{} does not exist".format(b)
        self.etc.mkdir(exist_ok=True)
        self.log.mkdir(exist_ok=True)
        self.data.mkdir(exist_ok=True)
        self.process_number = process_number
        self.ip_address = '127.0.0.1' if ip_address is None else ip_address
        self.first_port = port
        self.blob_granules_enabled = blob_granules_enabled
        if (blob_granules_enabled):
            # add extra process for blob_worker
            self.process_number += 1

        if (self.first_port is not None):
            self.last_used_port = int(self.first_port)-1
        self.server_ports = [self.__next_port()
                             for _ in range(self.process_number)]
        self.cluster_desc = random_secret_string(8)
        self.cluster_secret = random_secret_string(8)
        self.env_vars = {}
        self.running = False
        self.process = None
        self.fdbmonitor_logfile = None
        self.use_legacy_conf_syntax = False
        
        if create_config:
            self.create_cluster_file()
            self.save_config()

    def __next_port(self):
        if (self.first_port is None):
            return get_free_port()
        else:
            self.last_used_port += 1
            return self.last_used_port

    def save_config(self):
        new_conf_file = self.conf_file.parent / (self.conf_file.name + '.new')
        with open(new_conf_file, 'x') as f:
            conf_template = LocalCluster.configuration_template
            bg_knob_line = ""
            if (self.use_legacy_conf_syntax):
                conf_template = conf_template.replace("-", "_")
            if (self.blob_granules_enabled):
                bg_knob_line = "knob_bg_url=file://" + str(self.data) + "/fdbblob/"
            f.write(conf_template.format(
                etcdir=self.etc,
                fdbserver_bin=self.fdbserver_binary,
                datadir=self.data,
                logdir=self.log,
                ip_address=self.ip_address,
                bg_knob_line=bg_knob_line
            ))
            # By default, the cluster only has one process
            # If a port number is given and process_number > 1, we will use subsequent numbers
            # E.g., port = 4000, process_number = 5
            # Then 4000,4001,4002,4003,4004 will be used as ports
            # If port number is not given, we will randomly pick free ports
            for port in self.server_ports:
                f.write('[fdbserver.{server_port}]\n'.format(
                    server_port=port))
            if (self.blob_granules_enabled):
                # make last process a blob_worker class
                f.write('class = blob_worker')
            f.flush()
            os.fsync(f.fileno())

        os.replace(new_conf_file, self.conf_file)

    def create_cluster_file(self):
        with open(self.cluster_file, 'x') as f:
            f.write('{desc}:{secret}@{ip_addr}:{server_port}'.format(
                desc=self.cluster_desc,
                secret=self.cluster_secret,
                ip_addr=self.ip_address,
                server_port=self.server_ports[0]
            ))

    def start_cluster(self):
        assert not self.running, "Can't start a server that is already running"
        args = [str(self.fdbmonitor_binary),
                '--conffile',
                str(self.etc.joinpath('foundationdb.conf')),
                '--lockfile',
                str(self.etc.joinpath('fdbmonitor.lock'))]
        self.fdbmonitor_logfile = open(
            self.log.joinpath('fdbmonitor.log'), 'w')
        self.process = subprocess.Popen(
            args, stdout=self.fdbmonitor_logfile, stderr=self.fdbmonitor_logfile, env=self.process_env())
        self.running = True

    def stop_cluster(self):
        assert self.running, "Server is not running"
        if self.process.poll() is None:
            self.process.terminate()
        self.running = False

    def ensure_ports_released(self, timeout_sec=5):
        sec = 0
        while (sec < timeout_sec):
            in_use = False
            for port in self.server_ports:
                if is_port_in_use(port):
                    print("Port {} in use. Waiting for it to be released".format(port))
                    in_use = True
                    break
            if not in_use:
                return
            time.sleep(0.5)
            sec += 0.5
        assert False, "Failed to release ports in {}s".format(timeout_sec)

    def __enter__(self):
        self.start_cluster()
        return self

    def __exit__(self, xc_type, exc_value, traceback):
        self.stop_cluster()

    def create_database(self, storage='ssd', enable_tenants=True):
        db_config = 'configure new single {}'.format(storage)
        if (enable_tenants):
            db_config += " tenant_mode=optional_experimental"
        if (self.blob_granules_enabled):
            db_config += " blob_granules_enabled:=1"
        args = [self.fdbcli_binary, '-C',
                self.cluster_file, '--exec', db_config]

        res = subprocess.run(args, env=self.process_env())
        assert res.returncode == 0, "Create database failed with {}".format(
            res.returncode)

        if (self.blob_granules_enabled):
            bg_args = [self.fdbcli_binary, '-C',
                self.cluster_file, '--exec', 'blobrange start \\x00 \\xff']
            bg_res = subprocess.run(bg_args, env=self.process_env())
            assert bg_res.returncode == 0, "Start blob granules failed with {}".format(bg_res.returncode)

    def get_status(self):
        args = [self.fdbcli_binary, '-C', self.cluster_file, '--exec',
                'status json']
        res = subprocess.run(args, env=self.process_env(),
                             stdout=subprocess.PIPE)
        assert res.returncode == 0, "Get status failed with {}".format(
            res.returncode)
        return json.loads(res.stdout)

    def process_env(self):
        env = dict(os.environ)
        env.update(self.env_vars)
        return env

    def set_env_var(self, var_name, var_val):
        self.env_vars[var_name] = var_val
