#!/usr/bin/env python3
"""Exercise the standalone network diagnostic over bounded loopback connections."""

import argparse
import contextlib
import re
import socket
import subprocess
import tempfile
import time
from pathlib import Path


def unused_port():
    with socket.socket() as sock:
        sock.bind(("127.0.0.1", 0))
        return sock.getsockname()[1]


class SmokeTest:
    def __init__(self, executable, root):
        self.executable = executable
        self.root = root
        self.deadline = time.monotonic() + 75
        self.tls_options = []
        self.tls_suffix = ""

    def timeout(self, seconds):
        remaining = self.deadline - time.monotonic()
        if remaining <= 0:
            raise TimeoutError("network test smoke exceeded its deadline")
        return min(seconds, remaining)

    @contextlib.contextmanager
    def process(self, name, arguments):
        directory = self.root / name
        directory.mkdir()
        output = directory / "output.log"
        with output.open("w") as log:
            process = subprocess.Popen(
                [self.executable, *arguments, *self.tls_options],
                cwd=directory,
                stdout=log,
                stderr=subprocess.STDOUT,
            )
            try:
                yield process, output
            except BaseException:
                print("{} output:\n{}".format(name, output.read_text()))
                raise
            finally:
                if process.poll() is None:
                    process.terminate()
                    try:
                        process.wait(timeout=2)
                    except subprocess.TimeoutExpired:
                        process.kill()
                        process.wait(timeout=2)

    def finish(self, process, output, seconds=15, success=True):
        result = process.wait(timeout=self.timeout(seconds))
        text = output.read_text()
        if (result == 0) != success:
            raise AssertionError("unexpected exit status {}:\n{}".format(result, text))
        return text

    def ready(self, process, output):
        deadline = time.monotonic() + self.timeout(10)
        while time.monotonic() < deadline:
            if process.poll() is not None:
                raise AssertionError("listener exited before becoming ready")
            if "Listener: " in output.read_text():
                return
            time.sleep(0.05)
        raise TimeoutError("listener did not report readiness")

    def address(self, port):
        return "127.0.0.1:{}{}".format(port, self.tls_suffix)

    def enable_tls(self):
        cert = self.root / "cert.pem"
        key = self.root / "key.pem"
        config = self.root / "openssl.cnf"
        config.write_text(
            "[req]\n"
            "distinguished_name=subject\n"
            "x509_extensions=extensions\n"
            "prompt=no\n"
            "[subject]\n"
            "CN=networktest-smoke\n"
            "[extensions]\n"
            "basicConstraints=critical,CA:TRUE\n"
            "keyUsage=critical,digitalSignature,keyEncipherment,keyCertSign\n"
            "extendedKeyUsage=serverAuth,clientAuth\n"
            "subjectAltName=IP:127.0.0.1\n"
        )
        subprocess.run(
            [
                "openssl",
                "req",
                "-x509",
                "-newkey",
                "rsa:2048",
                "-nodes",
                "-days",
                "1",
                "-config",
                str(config),
                "-keyout",
                str(key),
                "-out",
                str(cert),
            ],
            check=True,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            timeout=self.timeout(15),
        )
        self.tls_suffix = ":tls"
        self.tls_options = [
            "--tls_certificate_file",
            str(cert),
            "--tls_key_file",
            str(key),
            "--tls_ca_file",
            str(cert),
            "--tls_verify_peers",
            "Root.CN=networktest-smoke",
        ]

    def rpc(self):
        port = unused_port()
        address = self.address(port)
        with self.process("rpc-server", ["--mode", "server", "-p", address]) as (
            server,
            server_output,
        ):
            self.ready(server, server_output)
            with self.process(
                "rpc-client",
                [
                    "--mode",
                    "client",
                    "--testservers",
                    address,
                    "--knob_network_test_script_mode=true",
                ],
            ) as (client, output):
                text = self.finish(client, output)
                rates = re.findall(r"(?m)^([0-9]+(?:\.[0-9]+)?)\t", text)
                assert any(float(rate) > 0 for rate in rates), text
            assert server.poll() is None, "RPC server exited during traffic"

    def p2p(self):
        address = self.address(unused_port())
        with self.process(
            "p2p",
            [
                "--mode",
                "p2p",
                "--test_listenerAddresses=" + address,
                "--test_remoteAddresses=" + address,
                "--test_connectionsOut=2",
                "--test_requestBytes=32:48",
                "--test_replyBytes=96:64",
                "--test_requests=2:3",
                "--test_idleMilliseconds=1:0",
                "--test_waitReadMilliseconds=1:2",
                "--test_waitWriteMilliseconds=0:1",
                "--test_targetDuration=2",
            ],
        ) as (process, output):
            text = self.finish(process, output)
            for expected in (
                "2 outgoing connections",
                "Request size: 32:48",
                "Response size: 64:96",
                "Requests per outgoing session: 2:3",
                "Delay before socket read: 1:2",
                "Delay before socket write: 0:1",
                "Delay before session close: 0:1",
            ):
                assert expected in text, text
            for direction in ("in", "out"):
                rates = re.findall(r"([0-9.]+)/s completed sessions " + direction, text)
                assert any(float(rate) > 0 for rate in rates), text
            errors = re.findall(r"Total Errors (\d+)", text)
            assert errors and all(int(count) == 0 for count in errors), text

    def handshake(self):
        port = unused_port()
        address = self.address(port)
        with self.process(
            "handshake-server",
            ["--mode", "p2p-oneshot", "--test_listenerAddresses=" + address],
        ) as (server, server_output):
            self.ready(server, server_output)
            with self.process(
                "handshake-client",
                ["--mode", "p2p-oneshot", "--test_remoteAddresses=" + address],
            ) as (client, output):
                text = self.finish(client, output)
                assert re.search(r"Client: connected to .*handshake done", text), text
            text = self.finish(server, server_output, seconds=20)
            assert re.search(r"Server: connected from .*handshake done", text), text
            assert "handshake error" not in text, text

    def invalid_arguments(self):
        server = ["--mode", "server", "-p", self.address(unused_port())]
        p2p = ["--mode", "p2p", "--test_listenerAddresses=" + self.address(unused_port())]
        cases = [
            [],
            ["--mode", "p2p"],
            p2p + ["--test_unknown=1"],
            p2p + ["--test_connectionsOut=-1"],
            p2p + ["--test_connectionsOut=invalid"],
            p2p + ["--test_requestBytes=1::2"],
            p2p + ["--test_replyBytes=2147483647"],
            p2p + ["--test_targetDuration=nan"],
            server + ["--knob_network_test_script_mode=invalid"],
            server + ["--knob_not_a_network_test_knob=1"],
            server + ["--not-a-network-test-option"],
        ]
        for index, arguments in enumerate(cases):
            with self.process("invalid-{}".format(index), arguments) as (
                process,
                output,
            ):
                text = self.finish(process, output, seconds=5, success=False)
                assert "ERROR:" in text, text


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("executable", type=lambda value: str(Path(value).resolve()))
    parser.add_argument("--tls", action="store_true", help="use temporary TLS fixtures")
    args = parser.parse_args()
    with tempfile.TemporaryDirectory(prefix="fdbrpc-network-smoke-") as directory:
        smoke = SmokeTest(args.executable, Path(directory))
        smoke.invalid_arguments()
        if args.tls:
            smoke.enable_tls()
        smoke.rpc()
        smoke.p2p()
        smoke.handshake()
    print("network test smoke passed ({})".format("TLS" if args.tls else "plain TCP"))


if __name__ == "__main__":
    main()
