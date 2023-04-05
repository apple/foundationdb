#!/usr/bin/env python3

import glob
import os
import shutil
import subprocess
import sys
from pathlib import Path

from cluster_args import CreateTmpFdbClusterArgParser
from local_cluster import LocalCluster, TLSConfig
from test_util import random_alphanum_string


class TempCluster(LocalCluster):
    def __init__(
        self,
        build_dir: str,
        process_number: int = 1,
        port: str = None,
        blob_granules_enabled: bool = False,
        tls_config: TLSConfig = None,
        authorization_kty: str = "",
        authorization_keypair_id: str = "",
        remove_at_exit: bool = True,
        custom_config: dict = {},
        enable_tenants: bool = True,
        enable_encryption_at_rest: bool = False,
    ):
        self.build_dir = Path(build_dir).resolve()
        assert self.build_dir.exists(), "{} does not exist".format(build_dir)
        assert self.build_dir.is_dir(), "{} is not a directory".format(build_dir)
        tmp_dir = self.build_dir.joinpath("tmp", random_alphanum_string(16))
        tmp_dir.mkdir(parents=True)
        self.tmp_dir = tmp_dir
        self.remove_at_exit = remove_at_exit
        self.enable_tenants = enable_tenants
        self.enable_encryption_at_rest = enable_encryption_at_rest
        super().__init__(
            tmp_dir,
            self.build_dir.joinpath("bin", "fdbserver"),
            self.build_dir.joinpath("bin", "fdbmonitor"),
            self.build_dir.joinpath("bin", "fdbcli"),
            process_number,
            port=port,
            blob_granules_enabled=blob_granules_enabled,
            enable_encryption_at_rest=enable_encryption_at_rest,
            tls_config=tls_config,
            mkcert_binary=self.build_dir.joinpath("bin", "mkcert"),
            authorization_kty=authorization_kty,
            authorization_keypair_id=authorization_keypair_id,
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
    script_desc = """
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
    """
    parser = CreateTmpFdbClusterArgParser(description=script_desc)

    parser.add_argument("cmd", metavar="COMMAND", nargs="+", help="The command to run")

    parser.add_argument(
        "--disable-log-dump",
        help="Do not dump cluster log on error",
        action="store_true",
    )

    args = parser.parse_args()

    if args.disable_tenants:
        enable_tenants = False
    else:
        enable_tenants = True

    tls_config = None
    if args.tls_enabled:
        tls_config = TLSConfig(
            server_chain_len=args.server_cert_chain_len,
            client_chain_len=args.client_cert_chain_len,
        )
    errcode = 1
    with TempCluster(
        args.build_dir,
        args.process_number,
        blob_granules_enabled=args.blob_granules_enabled,
        tls_config=tls_config,
        enable_tenants=enable_tenants,
        authorization_kty=args.authorization_kty,
        authorization_keypair_id=args.authorization_keypair_id,
        remove_at_exit=not args.no_remove_at_exit,
    ) as cluster:
        print("log-dir: {}".format(cluster.log))
        print("etc-dir: {}".format(cluster.etc))
        print("data-dir: {}".format(cluster.data))
        print("cluster-file: {}".format(cluster.cluster_file))
        cmd_args = []
        substitution_table = [
            ("@CLUSTER_FILE@", str(cluster.cluster_file)),
            ("@DATA_DIR@", str(cluster.data)),
            ("@LOG_DIR@", str(cluster.log)),
            ("@ETC_DIR@", str(cluster.etc)),
            ("@TMP_DIR@", str(cluster.tmp_dir)),
            ("@SERVER_CERT_FILE@", str(cluster.server_cert_file)),
            ("@SERVER_KEY_FILE@", str(cluster.server_key_file)),
            ("@SERVER_CA_FILE@", str(cluster.server_ca_file)),
            ("@CLIENT_CERT_FILE@", str(cluster.client_cert_file)),
            ("@CLIENT_KEY_FILE@", str(cluster.client_key_file)),
            ("@CLIENT_CA_FILE@", str(cluster.client_ca_file)),
        ]

        for cmd in args.cmd:
            for (placeholder, value) in substitution_table:
                cmd = cmd.replace(placeholder, value)
            cmd_args.append(cmd)
        env = dict(**os.environ)
        env["FDB_CLUSTER_FILE"] = env.get("FDB_CLUSTER_FILE", cluster.cluster_file)
        print("command: {}".format(cmd_args))
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
