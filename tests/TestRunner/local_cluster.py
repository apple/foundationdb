import json
from pathlib import Path
import random
import string
import subprocess
import os
import socket
import time

CLUSTER_UPDATE_TIMEOUT_SEC = 10
EXCLUDE_SERVERS_TIMEOUT_SEC = 120
RETRY_INTERVAL_SEC = 0.5


def _get_free_port_internal():
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.bind(("0.0.0.0", 0))
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
        return s.connect_ex(("localhost", port)) == 0


valid_letters_for_secret = string.ascii_letters + string.digits


class TLSConfig:
    # Passing a negative chain length generates expired leaf certificate
    def __init__(
        self,
        server_chain_len: int = 3,
        client_chain_len: int = 2,
        verify_peers="Check.Valid=1",
    ):
        self.server_chain_len = server_chain_len
        self.client_chain_len = client_chain_len
        self.verify_peers = verify_peers


def random_secret_string(length):
    return "".join(random.choice(valid_letters_for_secret) for _ in range(length))


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
public-address = {ip_address}:$ID{optional_tls}
listen-address = public
datadir = {datadir}/$ID
logdir = {logdir}
{bg_knob_line}
{tls_config}
{authz_public_key_config}
{custom_config}
{use_future_protocol_version}
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

    def __init__(
        self,
        basedir: str,
        fdbserver_binary: str,
        fdbmonitor_binary: str,
        fdbcli_binary: str,
        process_number: int,
        create_config=True,
        port=None,
        ip_address=None,
        blob_granules_enabled: bool = False,
        use_future_protocol_version: bool = False,
        redundancy: str = "single",
        tls_config: TLSConfig = None,
        mkcert_binary: str = "",
        custom_config: dict = {},
        public_key_json_str: str = "",
    ):
        self.basedir = Path(basedir)
        self.etc = self.basedir.joinpath("etc")
        self.log = self.basedir.joinpath("log")
        self.data = self.basedir.joinpath("data")
        self.cert = self.basedir.joinpath("cert")
        self.conf_file = self.etc.joinpath("foundationdb.conf")
        self.cluster_file = self.etc.joinpath("fdb.cluster")
        self.fdbserver_binary = Path(fdbserver_binary)
        self.fdbmonitor_binary = Path(fdbmonitor_binary)
        self.fdbcli_binary = Path(fdbcli_binary)
        for b in (self.fdbserver_binary, self.fdbmonitor_binary, self.fdbcli_binary):
            assert b.exists(), "{} does not exist".format(b)
        self.etc.mkdir(exist_ok=True)
        self.log.mkdir(exist_ok=True)
        self.data.mkdir(exist_ok=True)
        self.process_number = process_number
        self.redundancy = redundancy
        self.ip_address = "127.0.0.1" if ip_address is None else ip_address
        self.first_port = port
        self.custom_config = custom_config
        self.blob_granules_enabled = blob_granules_enabled
        if blob_granules_enabled:
            # add extra process for blob_worker
            self.process_number += 1
        self.use_future_protocol_version = use_future_protocol_version

        if self.first_port is not None:
            self.last_used_port = int(self.first_port) - 1
        self.server_ports = {server_id: self.__next_port() for server_id in range(self.process_number)}
        self.server_by_port = {port: server_id for server_id, port in self.server_ports.items()}
        self.next_server_id = self.process_number
        self.cluster_desc = random_secret_string(8)
        self.cluster_secret = random_secret_string(8)
        self.env_vars = {}
        self.running = False
        self.process = None
        self.fdbmonitor_logfile = None
        self.use_legacy_conf_syntax = False
        self.coordinators = set()
        self.active_servers = set(self.server_ports.keys())
        self.tls_config = tls_config
        self.public_key_json_file = None
        self.mkcert_binary = Path(mkcert_binary)
        self.server_cert_file = self.cert.joinpath("server_cert.pem")
        self.client_cert_file = self.cert.joinpath("client_cert.pem")
        self.server_key_file = self.cert.joinpath("server_key.pem")
        self.client_key_file = self.cert.joinpath("client_key.pem")
        self.server_ca_file = self.cert.joinpath("server_ca.pem")
        self.client_ca_file = self.cert.joinpath("client_ca.pem")

        if public_key_json_str:
            self.public_key_json_file = self.etc.joinpath("public_keys.json")
            with open(self.public_key_json_file, "w") as pubkeyfile:
                pubkeyfile.write(public_key_json_str)

        if create_config:
            self.create_cluster_file()
            self.save_config()

        if self.tls_config is not None:
            self.create_tls_cert()

        self.cluster_file = self.etc.joinpath("fdb.cluster")

    def __next_port(self):
        if self.first_port is None:
            return get_free_port()
        else:
            self.last_used_port += 1
            return self.last_used_port

    def save_config(self):
        new_conf_file = self.conf_file.parent / (self.conf_file.name + ".new")
        with open(new_conf_file, "x") as f:
            conf_template = LocalCluster.configuration_template
            bg_knob_line = ""
            if self.use_legacy_conf_syntax:
                conf_template = conf_template.replace("-", "_")
            if self.blob_granules_enabled:
                bg_knob_line = "knob_bg_url=file://" + str(self.data) + "/fdbblob/"
            f.write(
                conf_template.format(
                    etcdir=self.etc,
                    fdbserver_bin=self.fdbserver_binary,
                    datadir=self.data,
                    logdir=self.log,
                    ip_address=self.ip_address,
                    bg_knob_line=bg_knob_line,
                    tls_config=self.tls_conf_string(),
                    authz_public_key_config=self.authz_public_key_conf_string(),
                    optional_tls=":tls" if self.tls_config is not None else "",
                    custom_config='\n'.join(["{} = {}".format(key, value) for key, value in self.custom_config.items()]),
                    use_future_protocol_version="use-future-protocol-version = true" if self.use_future_protocol_version else "",
                )
            )
            # By default, the cluster only has one process
            # If a port number is given and process_number > 1, we will use subsequent numbers
            # E.g., port = 4000, process_number = 5
            # Then 4000,4001,4002,4003,4004 will be used as ports
            # If port number is not given, we will randomly pick free ports
            for server_id in self.active_servers:
                f.write("[fdbserver.{server_port}]\n".format(server_port=self.server_ports[server_id]))
                if self.use_legacy_conf_syntax:
                    f.write("machine_id = {}\n".format(server_id))
                else:
                    f.write("machine-id = {}\n".format(server_id))
            if self.blob_granules_enabled:
                # make last process a blob_worker class
                f.write("class = blob_worker\n")
            f.flush()
            os.fsync(f.fileno())

        os.replace(new_conf_file, self.conf_file)

    def create_cluster_file(self):
        with open(self.cluster_file, "x") as f:
            f.write(
                "{desc}:{secret}@{ip_addr}:{server_port}{optional_tls}".format(
                    desc=self.cluster_desc,
                    secret=self.cluster_secret,
                    ip_addr=self.ip_address,
                    server_port=self.server_ports[0],
                    optional_tls=":tls" if self.tls_config is not None else "",
                )
            )
        self.coordinators = {0}

    def start_cluster(self):
        assert not self.running, "Can't start a server that is already running"
        args = [
            str(self.fdbmonitor_binary),
            "--conffile",
            str(self.etc.joinpath("foundationdb.conf")),
            "--lockfile",
            str(self.etc.joinpath("fdbmonitor.lock")),
        ]
        self.fdbmonitor_logfile = open(self.log.joinpath("fdbmonitor.log"), "w")
        self.process = subprocess.Popen(
            args,
            stdout=self.fdbmonitor_logfile,
            stderr=self.fdbmonitor_logfile,
            env=self.process_env(),
        )
        self.running = True

    def stop_cluster(self):
        assert self.running, "Server is not running"
        if self.process.poll() is None:
            self.process.terminate()
        self.running = False

    def ensure_ports_released(self, timeout_sec=5):
        sec = 0
        while sec < timeout_sec:
            in_use = False
            for server_id in self.active_servers:
                port = self.server_ports[server_id]
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

    def __fdbcli_exec(self, cmd, stdout, stderr, timeout):
        args = [self.fdbcli_binary, "-C", self.cluster_file, "--exec", cmd]
        if self.tls_config:
            args += [
                "--tls-certificate-file",
                self.client_cert_file,
                "--tls-key-file",
                self.client_key_file,
                "--tls-ca-file",
                self.server_ca_file,
            ]
        if self.use_future_protocol_version:
            args += ["--use-future-protocol-version"]
        res = subprocess.run(args, env=self.process_env(), stderr=stderr, stdout=stdout, timeout=timeout)
        assert res.returncode == 0, "fdbcli command {} failed with {}".format(cmd, res.returncode)
        return res.stdout

    # Execute a fdbcli command
    def fdbcli_exec(self, cmd, timeout=None):
        self.__fdbcli_exec(cmd, None, None, timeout)

    # Execute a fdbcli command and return its output
    def fdbcli_exec_and_get(self, cmd, timeout=None):
        return self.__fdbcli_exec(cmd, subprocess.PIPE, None, timeout)

    def create_database(self, storage="ssd", enable_tenants=True):
        db_config = "configure new {} {}".format(self.redundancy, storage)
        if enable_tenants:
            db_config += " tenant_mode=optional_experimental"
        if self.blob_granules_enabled:
            db_config += " blob_granules_enabled:=1"
        self.fdbcli_exec(db_config)

        if self.blob_granules_enabled:
            self.fdbcli_exec("blobrange start \\x00 \\xff")

    # Generate and install test certificate chains and keys
    def create_tls_cert(self):
        assert self.tls_config is not None, "TLS not enabled"
        assert self.mkcert_binary.exists() and self.mkcert_binary.is_file(), "{} does not exist".format(
            self.mkcert_binary
        )
        self.cert.mkdir(exist_ok=True)
        server_chain_len = abs(self.tls_config.server_chain_len)
        client_chain_len = abs(self.tls_config.client_chain_len)
        expire_server_cert = self.tls_config.server_chain_len < 0
        expire_client_cert = self.tls_config.client_chain_len < 0
        args = [
            str(self.mkcert_binary),
            "--server-chain-length",
            str(server_chain_len),
            "--client-chain-length",
            str(client_chain_len),
            "--server-cert-file",
            str(self.server_cert_file),
            "--client-cert-file",
            str(self.client_cert_file),
            "--server-key-file",
            str(self.server_key_file),
            "--client-key-file",
            str(self.client_key_file),
            "--server-ca-file",
            str(self.server_ca_file),
            "--client-ca-file",
            str(self.client_ca_file),
            "--print-args",
        ]
        if expire_server_cert:
            args.append("--expire-server-cert")
        if expire_client_cert:
            args.append("--expire-client-cert")
        subprocess.run(args, check=True)

    # Materialize server's TLS configuration section
    def tls_conf_string(self):
        if self.tls_config is None:
            return ""
        else:
            conf_map = {
                "tls-certificate-file": self.server_cert_file,
                "tls-key-file": self.server_key_file,
                "tls-ca-file": self.client_ca_file,
                "tls-verify-peers": self.tls_config.verify_peers,
            }
            return "\n".join("{} = {}".format(k, v) for k, v in conf_map.items())

    def authz_public_key_conf_string(self):
        if self.public_key_json_file is not None:
            return "authorization-public-key-file = {}".format(self.public_key_json_file)
        else:
            return ""

    # Get cluster status using fdbcli
    def get_status(self):
        status_output = self.fdbcli_exec_and_get("status json")
        return json.loads(status_output)

    # Get the set of servers from the cluster status matching the given filter
    def get_servers_from_status(self, filter):
        status = self.get_status()
        if "processes" not in status["cluster"]:
            return {}

        servers_found = set()
        addresses = [proc_info["address"] for proc_info in status["cluster"]["processes"].values() if filter(proc_info)]
        for addr in addresses:
            port = int(addr.split(":", 1)[1])
            assert port in self.server_by_port, "Unknown server port {}".format(port)
            servers_found.add(self.server_by_port[port])

        return servers_found

    # Get the set of all servers from the cluster status
    def get_all_servers_from_status(self):
        return self.get_servers_from_status(lambda _: True)

    # Get the set of all servers with coordinator role from the cluster status
    def get_coordinators_from_status(self):
        def is_coordinator(proc_status):
            return any(entry["role"] == "coordinator" for entry in proc_status["roles"])

        return self.get_servers_from_status(is_coordinator)

    def process_env(self):
        env = dict(os.environ)
        env.update(self.env_vars)
        return env

    def set_env_var(self, var_name, var_val):
        self.env_vars[var_name] = var_val

    # Add a new server process to the cluster and return its ID
    # Need to call save_config to apply the changes
    def add_server(self):
        server_id = self.next_server_id
        assert server_id not in self.server_ports, "Server ID {} is already in use".format(server_id)
        self.next_server_id += 1
        port = self.__next_port()
        self.server_ports[server_id] = port
        self.server_by_port[port] = server_id
        self.active_servers.add(server_id)
        return server_id

    # Remove the server with the given ID from the cluster
    # Need to call save_config to apply the changes
    def remove_server(self, server_id):
        assert server_id in self.active_servers, "Server {} does not exist".format(server_id)
        self.active_servers.remove(server_id)

    # Wait until changes to the set of servers (additions & removals) are applied
    def wait_for_server_update(self, timeout=CLUSTER_UPDATE_TIMEOUT_SEC):
        time_limit = time.time() + timeout
        servers_found = set()
        while time.time() <= time_limit:
            servers_found = self.get_all_servers_from_status()
            if servers_found != self.active_servers:
                break
            time.sleep(RETRY_INTERVAL_SEC)
        assert "Failed to apply server changes after {}sec. Expected: {}, Actual: {}".format(
            timeout, self.active_servers, servers_found
        )

    # Apply changes to the set of the coordinators, based on the current value of self.coordinators
    def update_coordinators(self):
        urls = ["{}:{}".format(self.ip_address, self.server_ports[id]) for id in self.coordinators]
        self.fdbcli_exec("coordinators {}".format(" ".join(urls)))

    # Wait until the changes to the set of the coordinators are applied
    def wait_for_coordinator_update(self, timeout=CLUSTER_UPDATE_TIMEOUT_SEC):
        time_limit = time.time() + timeout
        coord_found = set()
        while time.time() <= time_limit:
            coord_found = self.get_coordinators_from_status()
            if coord_found != self.coordinators:
                break
            time.sleep(RETRY_INTERVAL_SEC)
        assert "Failed to apply coordinator changes after {}sec. Expected: {}, Actual: {}".format(
            timeout, self.coordinators, coord_found
        )
        # Check if the cluster file was successfully updated too
        connection_string = open(self.cluster_file, "r").read()
        for server_id in self.coordinators:
            assert (
                connection_string.find(str(self.server_ports[server_id])) != -1
            ), "Missing coordinator {} port {} in the cluster file".format(server_id, self.server_ports[server_id])

    # Exclude the servers with the given ID from the cluster, i.e. move out their data
    # The method waits until the changes are applied
    def exclude_servers(self, server_ids):
        urls = ["{}:{}".format(self.ip_address, self.server_ports[id]) for id in server_ids]
        self.fdbcli_exec("exclude FORCE {}".format(" ".join(urls)), timeout=EXCLUDE_SERVERS_TIMEOUT_SEC)

    # Perform a cluster wiggle: replace all servers with new ones
    def cluster_wiggle(self):
        old_servers = self.active_servers.copy()
        new_servers = set()
        print("Starting cluster wiggle")
        print(
            "Old servers: {} on ports {}".format(
                old_servers, [self.server_ports[server_id] for server_id in old_servers]
            )
        )
        print("Old coordinators: {}".format(self.coordinators))

        # Step 1: add new servers
        start_time = time.time()
        for _ in range(len(old_servers)):
            new_servers.add(self.add_server())
        print(
            "New servers: {} on ports {}".format(
                new_servers, [self.server_ports[server_id] for server_id in new_servers]
            )
        )
        self.save_config()
        self.wait_for_server_update()
        print("New servers successfully added to the cluster. Time: {}s".format(time.time() - start_time))

        # Step 2: change coordinators
        start_time = time.time()
        new_coordinators = set(random.sample(new_servers, len(self.coordinators)))
        print("New coordinators: {}".format(new_coordinators))
        self.coordinators = new_coordinators.copy()
        self.update_coordinators()
        self.wait_for_coordinator_update()
        print("Coordinators successfully changed. Time: {}s".format(time.time() - start_time))

        # Step 3: exclude old servers from the cluster, i.e. move out their data
        start_time = time.time()
        self.exclude_servers(old_servers)
        print("Old servers successfully excluded from the cluster. Time: {}s".format(time.time() - start_time))

        # Step 4: remove the old servers
        start_time = time.time()
        for server_id in old_servers:
            self.remove_server(server_id)
        self.save_config()
        self.wait_for_server_update()
        print("Old servers successfully removed from the cluster. Time: {}s".format(time.time() - start_time))
