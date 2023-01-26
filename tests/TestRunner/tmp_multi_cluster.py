#!/usr/bin/env python3

#
# tmp_multi_cluster.py
#
# This source file is part of the FoundationDB open source project
#
# Copyright 2013-2018 Apple Inc. and the FoundationDB project authors
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

import os
import subprocess
import sys

from cluster_args import CreateTmpFdbClusterArgParser
from tmp_cluster import TempCluster

if __name__ == "__main__":
    script_desc = """
	This script automatically configures N temporary local clusters on the machine and then
	calls a command while these clusters are running. As soon as the command returns, all
	configured clusters are killed and all generated data is deleted.

	The purpose of this is to support testing a set of integration tests using multiple clusters
	(i.e. using the Multi-threaded client). 
	"""

    parser = CreateTmpFdbClusterArgParser(description=script_desc)

    parser.add_argument(
        "--clusters",
        "-c",
        metavar="NUM_CLUSTERS",
        type=int,
        help="The number of clusters to run",
        required=True,
    )
    parser.add_argument("cmd", metavar="COMMAND", nargs="+", help="The command to run")
    args = parser.parse_args()
    errcode = 1

    # spawn all the clusters
    num_clusters = args.clusters

    clusters = []
    try:
        for c in range(1, num_clusters + 1):
            # now start the cluster up
            local_c = TempCluster(args.build_dir)

            local_c.__enter__()
            clusters.append(local_c)

        # all clusters should be running now, so run the subcommand
        # TODO (bfines): pass through the proper ENV commands so that the client can find everything
        cluster_paths = ";".join(
            [str(cluster.etc.joinpath("fdb.cluster")) for cluster in clusters]
        )
        print(cluster_paths)
        env = dict(**os.environ)
        env["FDB_CLUSTERS"] = env.get("FDB_CLUSTERS", cluster_paths)
        env["FDB_CLUSTER_FILE"] = env.get("FDB_CLUSTER_FILE", cluster_paths)
        cmd_args = []
        for cmd in args.cmd:
            cmd_args.append(cmd)
        print("command: {}".format(cmd_args))
        errcode = subprocess.run(
            args.cmd, stdout=sys.stdout, stderr=sys.stderr, env=env
        ).returncode
    finally:
        # shutdown all the running clusters
        for tc in clusters:
            tc.close()

    sys.exit(errcode)
