#!/usr/bin/env python3
#
# make_public.py
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


import argparse
import platform
import socket
import sys
import re
import errno
import subprocess
import os


def invalidClusterFile(clusterFile):
    print("ERROR: '%s' is not a valid cluster file" % clusterFile)
    sys.exit(1)


def getOrValidateAddress(address):
    if address is None:
        try:
            s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
            s.connect(("www.foundationdb.org", 80))
            return s.getsockname()[0]
        except Exception as e:
            print("ERROR: Could not determine an address: %s" % e)
            exit(1)
    else:
        try:
            s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
            s.bind((address, 8000))
            s.close()
            return address
        except socket.error as e:
            if e.errno == errno.EADDRINUSE:
                return address
            else:
                print("ERROR: Address %s could not be bound" % address)
                exit(1)


def makePublic(clusterFile, newAddress, makeTLS):
    newAddress = getOrValidateAddress(newAddress)

    f = open(clusterFile, "r")
    clusterStr = None
    for line in f:
        line = line.strip()
        if len(line) > 0:
            if clusterStr is not None:
                invalidClusterFile(clusterFile)
                return
            clusterStr = line

    if clusterStr is None:
        invalidClusterFile(clusterFile)

    if not re.match(
        "^[a-zA-Z0-9_]*:[a-zA-Z0-9]*@([0-9\\.]*:[0-9]*(:tls)?,)*[0-9\\.]*:[0-9]*(:tls)?$",
        clusterStr,
    ):
        invalidClusterFile(clusterFile)
    if not re.match(
        "^.*@(127\\.0\\.0\\.1:[0-9]*(:tls)?,)*127\\.0\\.0\\.1:[0-9]*(:tls)?$",
        clusterStr,
    ):
        print(
            "ERROR: Cannot modify cluster file whose coordinators are not at address 127.0.0.1"
        )
        sys.exit(1)

    f.close()

    f = open(clusterFile, "w")
    clusterStr = clusterStr.replace("127.0.0.1", newAddress)
    if makeTLS:
        clusterStr = re.sub("([0-9]),", "\\1:tls,", clusterStr)
        if not clusterStr.endswith(":tls"):
            clusterStr += ":tls"

    f.write(clusterStr + "\n")
    f.close()

    return newAddress, clusterStr.count(":tls") != 0


def restartServer():
    subprocess.call(["service", "foundationdb", "restart"])


if __name__ == "__main__":

    if platform.system() != "Linux":
        print("ERROR: this script can only be run on Linux")
        sys.exit(1)

    parser = argparse.ArgumentParser(
        description="Converts a cluster with a local address to one with a public address"
    )
    parser.add_argument(
        "-C",
        dest="clusterFile",
        type=str,
        help="The cluster file to be converted. If unspecified, the cluster file at /etc/foundationdb/fdb.cluster is used.",
        default="/etc/foundationdb/fdb.cluster",
    )
    parser.add_argument(
        "-a",
        dest="address",
        type=str,
        help="The new IP address to use. By default, an interface with access to the internet is chosen.",
    )
    parser.add_argument(
        "-t",
        dest="tls",
        action="store_true",
        default=False,
        help="Convert addresses without TLS enabled to accepting TLS connections.",
    )

    args = parser.parse_args()

    if os.geteuid() != 0:
        print("ERROR: this script must be run as root")
        sys.exit(1)

    address, hasTLS = makePublic(args.clusterFile, args.address, args.tls)
    restartServer()

    print(
        "%s is now using address %s%s"
        % (args.clusterFile, address, " (TLS enabled)" if hasTLS else "")
    )
