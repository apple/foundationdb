#!/usr/bin/env python3
""" Runs a local FoundationDB cluster
"""

import argparse
import asyncio
import logging
import os
import os.path
import sys

import lib.local_cluster


logger = logging.getLogger(__name__)


def _setup_logs(log_level: int = logging.INFO):
    log_format = logging.Formatter(
        "%(asctime)s | %(name)20s :: %(levelname)-8s :: %(message)s"
    )

    logger.handlers.clear()

    stdout_handler = logging.StreamHandler(stream=sys.stderr)
    stdout_handler.setLevel(log_level)
    stdout_handler.setFormatter(log_format)

    logger.addHandler(stdout_handler)
    logger.setLevel(log_level)

    # Here we might lose some of the logging from lib
    lib_logger = logging.getLogger("lib")
    lib_logger.setLevel(log_level)


def _setup_args() -> argparse.Namespace:
    """Parse the command line arguments"""
    parser = argparse.ArgumentParser(os.path.basename(__file__))

    parser.add_argument(
        "-n", "--num-processes", type=int, default=1, help="Number of FDB processes"
    )
    parser.add_argument(
        "-W", "--work-dir", type=str, default=None, help="Work directory"
    )
    parser.add_argument(
        "--debug", action="store_true", default=False, help="Debug logging"
    )
    parser.add_argument("--cluster-file", type=str, default=None, help="Cluster file")
    parser.add_argument(
        "--fdbserver-path", type=str, default=None, help="Path to fdbserver"
    )
    parser.add_argument("--fdbcli-path", type=str, default=None, help="Path to fdbcli")
    parser.add_argument(
        "--port", type=int, default=4000, help="Port for the first process"
    )

    return parser.parse_args()


async def run_fdbservers(num_processes, work_dir, cluster_file, port):
    async with lib.local_cluster.FDBServerLocalCluster(
        num_processes, work_dir, cluster_file, port
    ):
        await asyncio.sleep(20)


def main():
    args = _setup_args()
    _setup_logs(logging.DEBUG if args.debug else logging.INFO)

    if args.num_processes < 1:
        raise RuntimeError(f"Must spawn more than 1 process, got {args.num_processes}")

    lib.fdb_process.set_fdbserver_path(args.fdbserver_path)

    asyncio.get_event_loop().run_until_complete(
        run_fdbservers(args.num_processes, args.work_dir, args.cluster_file, args.port)
    )

    return 0


if __name__ == "__main__":
    sys.exit(main())
