#!/usr/bin/env python3

import glob
import os
import shutil
import subprocess
import sys
from local_cluster import LocalCluster, TLSConfig, random_secret_string
from argparse import ArgumentParser, RawDescriptionHelpFormatter
from pathlib import Path


class TempCluster(LocalCluster):
    def __init__(
        self,
        build_dir: str,
        process_number: int = 1,
        port: str = None,
        blob_granules_enabled: bool = False,
        tls_config: TLSConfig = None,
        public_key_json_str: str = None,
        remove_at_exit: bool = True,
        custom_config: dict = {},
        enable_tenants: bool = True,
    ):
        self.build_dir = Path(build_dir).resolve()
        assert self.build_dir.exists(), "{} does not exist".format(build_dir)
        assert self.build_dir.is_dir(), "{} is not a directory".format(build_dir)
        tmp_dir = self.build_dir.joinpath("tmp", random_secret_string(16))
        tmp_dir.mkdir(parents=True)
        self.tmp_dir = tmp_dir
        self.remove_at_exit = remove_at_exit
        self.enable_tenants = enable_tenants
        super().__init__(
            tmp_dir,
            self.build_dir.joinpath("bin", "fdbserver"),
            self.build_dir.joinpath("bin", "fdbmonitor"),
            self.build_dir.joinpath("bin", "fdbcli"),
            process_number,
            port=port,
            blob_granules_enabled=blob_granules_enabled,
            tls_config=tls_config,
            mkcert_binary=self.build_dir.joinpath("bin", "mkcert"),
            public_key_json_str=public_key_json_str,
            custom_config=custom_config,
        )

    def __enter__(self):
        super().__enter__()
        if self.enable_tenants:
            super().create_database()
        else:
            super().create_database(enable_tenants=False)
        return self

    def __exit__(self, xc_type, exc_value, traceback):
        super().__exit__(xc_type, exc_value, traceback)
        if self.remove_at_exit:
            shutil.rmtree(self.tmp_dir)

    def close(self):
        super().__exit__(None, None, None)
        if self.remove_at_exit:
            shutil.rmtree(self.tmp_dir)


if __name__ == "__main__":
    parser = ArgumentParser(
        formatter_class=RawDescriptionHelpFormatter,
        description="""
    This script automatically configures a temporary local cluster on the machine
    and then calls a command while this cluster is running. As soon as the command
    returns, the configured cluster is killed and all generated data is deleted.
    This is useful for testing: if a test needs access to a fresh fdb cluster, one
    can simply pass the test command to this script.

    The command to run after the cluster started. Before the command is executed,
    the following arguments will be preprocessed:
    - All occurrences of @CLUSTER_FILE@ will be replaced with the path to the generated cluster file.
    - All occurrences of @DATA_DIR@ will be replaced with the path to the data directory.
    - All occurrences of @LOG_DIR@ will be replaced with the path to the log directory.
    - All occurrences of @ETC_DIR@ will be replaced with the path to the configuration directory.

    The environment variable FDB_CLUSTER_FILE is set to the generated cluster for the command if it is not set already.
    """,
    )
    parser.add_argument(
        "--build-dir",
        "-b",
        metavar="BUILD_DIRECTORY",
        help="FDB build directory",
        required=True,
    )
    parser.add_argument("cmd", metavar="COMMAND", nargs="+", help="The command to run")
    parser.add_argument(
        "--process-number",
        "-p",
        help="Number of fdb processes running",
        type=int,
        default=1,
    )
    parser.add_argument(
        "--disable-log-dump",
        help="Do not dump cluster log on error",
        action="store_true",
    )
    parser.add_argument(
        "--disable-tenants",
        help="Do not enable tenant mode",
        action="store_true",
        default=False
    )
    parser.add_argument(
        "--blob-granules-enabled", help="Enable blob granules", action="store_true"
    )
    parser.add_argument(
        "--tls-enabled", help="Enable TLS (with test-only certificates)", action="store_true")
    parser.add_argument(
        "--server-cert-chain-len",
        help="Length of server TLS certificate chain including root CA. Negative value deliberately generates expired leaf certificate for TLS testing. Only takes effect with --tls-enabled.",
        type=int,
        default=3,
    )
    parser.add_argument(
        "--client-cert-chain-len",
        help="Length of client TLS certificate chain including root CA. Negative value deliberately generates expired leaf certificate for TLS testing. Only takes effect with --tls-enabled.",
        type=int,
        default=2,
    )
    parser.add_argument(
        "--tls-verify-peer",
        help="Rules to verify client certificate chain. See https://apple.github.io/foundationdb/tls.html#peer-verification",
        type=str,
        default="Check.Valid=1",
    )
    args = parser.parse_args()

    if args.disable_tenants:
        enable_tenants = False
    else:
        enable_tenants = True

    tls_config = None
    if args.tls_enabled:
        tls_config = TLSConfig(server_chain_len=args.server_cert_chain_len,
                               client_chain_len=args.client_cert_chain_len)
    errcode = 1
    with TempCluster(
        args.build_dir,
        args.process_number,
        blob_granules_enabled=args.blob_granules_enabled,
        tls_config=tls_config,
        enable_tenants=enable_tenants,
    ) as cluster:
        print("log-dir: {}".format(cluster.log))
        print("etc-dir: {}".format(cluster.etc))
        print("data-dir: {}".format(cluster.data))
        print("cluster-file: {}".format(cluster.cluster_file))
        cmd_args = []
        for cmd in args.cmd:
            if cmd == "@CLUSTER_FILE@":
                cmd_args.append(str(cluster.cluster_file))
            elif cmd == "@DATA_DIR@":
                cmd_args.append(str(cluster.data))
            elif cmd == "@LOG_DIR@":
                cmd_args.append(str(cluster.log))
            elif cmd == "@ETC_DIR@":
                cmd_args.append(str(cluster.etc))
            elif cmd == "@TMP_DIR@":
                cmd_args.append(str(cluster.tmp_dir))
            elif cmd == "@SERVER_CERT_FILE@":
                cmd_args.append(str(cluster.server_cert_file))
            elif cmd == "@SERVER_KEY_FILE@":
                cmd_args.append(str(cluster.server_key_file))
            elif cmd == "@SERVER_CA_FILE@":
                cmd_args.append(str(cluster.server_ca_file))
            elif cmd == "@CLIENT_CERT_FILE@":
                cmd_args.append(str(cluster.client_cert_file))
            elif cmd == "@CLIENT_KEY_FILE@":
                cmd_args.append(str(cluster.client_key_file))
            elif cmd == "@CLIENT_CA_FILE@":
                cmd_args.append(str(cluster.client_ca_file))
            elif cmd.startswith("@DATA_DIR@"):
                cmd_args.append(str(cluster.data) + cmd[len("@DATA_DIR@") :])
            else:
                cmd_args.append(cmd)
        env = dict(**os.environ)
        env["FDB_CLUSTER_FILE"] = env.get(
            "FDB_CLUSTER_FILE", cluster.cluster_file
        )
        env["FDB_CLUSTERS"] = env.get(
            "FDB_CLUSTERS", cluster.cluster_file
        )
        errcode = subprocess.run(
            cmd_args, stdout=sys.stdout, stderr=sys.stderr, env=env
        ).returncode

        sev40s = (
            subprocess.getoutput(
                "grep -r 'Severity=\"40\"' {}".format(cluster.log.as_posix())
            )
            .rstrip()
            .splitlines()
        )

        for line in sev40s:
            # When running ASAN we expect to see this message. Boost coroutine should be using the correct asan
            # annotations so that it shouldn't produce any false positives.
            if (
                "WARNING: ASan doesn't fully support makecontext/swapcontext functions and may produce false positives in some cases!"
                in line
            ):
                continue
            print(">>>>>>>>>>>>>>>>>>>> Found severity 40 events - the test fails")
            errcode = 1
            break

        if errcode and not args.disable_log_dump:
            for etc_file in glob.glob(os.path.join(cluster.etc, "*")):
                print(">>>>>>>>>>>>>>>>>>>> Contents of {}:".format(etc_file))
                with open(etc_file, "r") as f:
                    print(f.read())
            for log_file in glob.glob(os.path.join(cluster.log, "*")):
                print(">>>>>>>>>>>>>>>>>>>> Contents of {}:".format(log_file))
                with open(log_file, "r") as f:
                    print(f.read())

    sys.exit(errcode)
