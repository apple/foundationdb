#! /usr/bin/python

# entrypoint.py
#
# This source file is part of the FoundationDB open source project
#
# Copyright 2018-2019 Apple Inc. and the FoundationDB project authors
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
import hashlib
import http.server
import logging
import json
import os
import shutil
import socket
import ssl
import stat
import time
import traceback
import sys
from pathlib import Path

from watchdog.observers import Observer
from watchdog.events import FileSystemEventHandler

log = logging.getLogger(__name__)
log.setLevel(logging.INFO)


class Config(object):
    def __init__(self):
        parser = argparse.ArgumentParser(description="FoundationDB Kubernetes Sidecar")
        parser.add_argument(
            "--init-mode",
            help=(
                "Whether to run the sidecar in init mode "
                "which causes it to copy the files once and "
                "exit without starting a server."
            ),
            action="store_true",
        )
        parser.add_argument(
            "--bind-address", help="IP and port to bind on", default="0.0.0.0:8080"
        )
        parser.add_argument(
            "--tls",
            help=("This flag enables TLS for incoming " "connections"),
            action="store_true",
        )
        parser.add_argument(
            "--tls-certificate-file",
            help=(
                "The path to the certificate file for TLS "
                "connections. If this is not provided we "
                "will take the path from the "
                "FDB_TLS_CERTIFICATE_FILE environment "
                "variable."
            ),
        )
        parser.add_argument(
            "--tls-ca-file",
            help=(
                "The path to the certificate authority file "
                "for TLS connections  If this is not "
                "provided we will take the path from the "
                "FDB_TLS_CA_FILE environment variable."
            ),
        )
        parser.add_argument(
            "--tls-key-file",
            help=(
                "The path to the key file for TLS "
                "connections. If this is not provided we "
                "will take the path from the "
                "FDB_TLS_KEY_FILE environment "
                "variable."
            ),
        )
        parser.add_argument(
            "--tls-verify-peers",
            help=(
                "The peer verification rules for incoming "
                "TLS  connections. If this is not provided "
                "we will take the rules from the "
                "FDB_TLS_VERIFY_PEERS environment variable. "
                "The format of this is the same as the TLS "
                "peer verification rules in FoundationDB."
            ),
        )
        parser.add_argument(
            "--input-dir",
            help=("The directory containing the input files " "the config map."),
            default="/var/input-files",
        )
        parser.add_argument(
            "--output-dir",
            help=(
                "The directory into which the sidecar should "
                "place the file it generates."
            ),
            default="/var/output-files",
        )
        parser.add_argument(
            "--substitute-variable",
            help=(
                "A custom environment variable that should "
                "available for substitution in the monitor "
                "conf."
            ),
            action="append",
        )
        parser.add_argument(
            "--copy-file",
            help=("A file to copy from the config map to the " "output directory."),
            action="append",
        )
        parser.add_argument(
            "--copy-binary",
            help=("A binary to copy from the to the output" "directory."),
            action="append",
        )
        parser.add_argument(
            "--copy-library",
            help=(
                "A version of the client library to copy " "to the output directory."
            ),
            action="append",
        )
        parser.add_argument(
            "--input-monitor-conf",
            help=("The name of a monitor conf template in the " "input files"),
        )
        parser.add_argument(
            "--main-container-version",
            help=("The version of the main foundationdb " "container in the pod"),
        )
        parser.add_argument(
            "--main-container-conf-dir",
            help=(
                "The directory where the dynamic conf "
                "written by the sidecar will be mounted in "
                "the main container."
            ),
            default="/var/dynamic-conf",
        )
        parser.add_argument(
            "--require-not-empty",
            help=(
                "A file that must be present and non-empty " "in the input directory"
            ),
            action="append",
        )
        args = parser.parse_args()

        self.bind_address = args.bind_address
        self.input_dir = args.input_dir
        self.output_dir = args.output_dir

        self.enable_tls = args.tls
        self.copy_files = args.copy_file or []
        self.copy_binaries = args.copy_binary or []
        self.copy_libraries = args.copy_library or []
        self.input_monitor_conf = args.input_monitor_conf
        self.init_mode = args.init_mode
        self.main_container_version = args.main_container_version
        self.require_not_empty = args.require_not_empty

        with open("/var/fdb/version") as version_file:
            self.primary_version = version_file.read().strip()

        version_split = self.primary_version.split(".")
        self.minor_version = [int(version_split[0]), int(version_split[1])]

        forbid_deprecated_environment_variables = self.is_at_least([6, 3])

        if self.enable_tls:
            self.certificate_file = args.tls_certificate_file or os.getenv(
                "FDB_TLS_CERTIFICATE_FILE"
            )
            assert self.certificate_file, (
                "You must provide a certificate file, either through the "
                "tls_certificate_file argument or the FDB_TLS_CERTIFICATE_FILE "
                "environment variable"
            )
            self.ca_file = args.tls_ca_file or os.getenv("FDB_TLS_CA_FILE")
            assert self.ca_file, (
                "You must provide a CA file, either through the tls_ca_file "
                "argument or the FDB_TLS_CA_FILE environment variable"
            )
            self.key_file = args.tls_key_file or os.getenv("FDB_TLS_KEY_FILE")
            assert self.key_file, (
                "You must provide a key file, either through the tls_key_file "
                "argument or the FDB_TLS_KEY_FILE environment variable"
            )
            self.peer_verification_rules = args.tls_verify_peers or os.getenv(
                "FDB_TLS_VERIFY_PEERS"
            )

        self.substitutions = {}
        for key in [
            "FDB_PUBLIC_IP",
            "FDB_MACHINE_ID",
            "FDB_ZONE_ID",
            "FDB_INSTANCE_ID",
        ]:
            self.substitutions[key] = os.getenv(key, "")

        if self.substitutions["FDB_MACHINE_ID"] == "":
            self.substitutions["FDB_MACHINE_ID"] = os.getenv("HOSTNAME", "")

        if self.substitutions["FDB_ZONE_ID"] == "":
            self.substitutions["FDB_ZONE_ID"] = self.substitutions["FDB_MACHINE_ID"]
        if self.substitutions["FDB_PUBLIC_IP"] == "":
            address_info = socket.getaddrinfo(
                self.substitutions["FDB_MACHINE_ID"],
                4500,
                family=socket.AddressFamily.AF_INET,
            )
            if len(address_info) > 0:
                self.substitutions["FDB_PUBLIC_IP"] = address_info[0][4][0]

        if self.main_container_version == self.primary_version:
            self.substitutions["BINARY_DIR"] = "/usr/bin"
        else:
            self.substitutions["BINARY_DIR"] = target_path = str(
                Path("%s/bin/%s" % (args.main_container_conf_dir, self.primary_version))
            )

        for variable in args.substitute_variable or []:
            self.substitutions[variable] = os.getenv(variable)

        if forbid_deprecated_environment_variables:
            for variable in [
                "SIDECAR_CONF_DIR",
                "INPUT_DIR",
                "OUTPUT_DIR",
                "COPY_ONCE",
            ]:
                if os.getenv(variable):
                    print(
                        f"""Environment variable {variable} is not supported in this version of FoundationDB.
                        Please use the command-line arguments instead."""
                    )
                    sys.exit(1)

        if os.getenv("SIDECAR_CONF_DIR"):
            with open(
                os.path.join(os.getenv("SIDECAR_CONF_DIR"), "config.json")
            ) as conf_file:
                config = json.load(conf_file)
        else:
            config = {}

        if os.getenv("INPUT_DIR"):
            self.input_dir = os.getenv("INPUT_DIR")

        if os.getenv("OUTPUT_DIR"):
            self.output_dir = os.getenv("OUTPUT_DIR")

        if "ADDITIONAL_SUBSTITUTIONS" in config and config["ADDITIONAL_SUBSTITUTIONS"]:
            for key in config["ADDITIONAL_SUBSTITUTIONS"]:
                self.substitutions[key] = os.getenv(key, key)

        if "COPY_FILES" in config and config["COPY_FILES"]:
            self.copy_files.extend(config["COPY_FILES"])

        if "COPY_BINARIES" in config and config["COPY_BINARIES"]:
            self.copy_binaries.extend(config["COPY_BINARIES"])

        if "COPY_LIBRARIES" in config and config["COPY_LIBRARIES"]:
            self.copy_libraries.extend(config["COPY_LIBRARIES"])

        if "INPUT_MONITOR_CONF" in config and config["INPUT_MONITOR_CONF"]:
            self.input_monitor_conf = config["INPUT_MONITOR_CONF"]

        if os.getenv("COPY_ONCE", "0") == "1":
            self.init_mode = True

    @classmethod
    def shared(cls):
        if cls.shared_config:
            return cls.shared_config
        cls.shared_config = Config()
        return cls.shared_config

    shared_config = None

    def is_at_least(self, target_version):
        return self.minor_version[0] > target_version[0] or (
            self.minor_version[0] == target_version[0]
            and self.minor_version[1] >= target_version[1]
        )


class Server(http.server.BaseHTTPRequestHandler):
    ssl_context = None

    @classmethod
    def start(cls):
        """
        This method starts the server.
        """
        config = Config.shared()
        (address, port) = config.bind_address.split(":")
        log.info("Listening on %s:%s" % (address, port))
        httpd = http.server.HTTPServer((address, int(port)), cls)

        if config.enable_tls:
            context = Server.load_ssl_context()
            httpd.socket = context.wrap_socket(httpd.socket, server_side=True)
            observer = Observer()
            event_handler = CertificateEventHandler()
            for path in set(
                [
                    Path(config.certificate_file).parent.as_posix(),
                    Path(config.key_file).parent.as_posix(),
                ]
            ):
                observer.schedule(event_handler, path)
            observer.start()

        httpd.serve_forever()

    @classmethod
    def load_ssl_context(cls):
        config = Config.shared()
        if not cls.ssl_context:
            cls.ssl_context = ssl.create_default_context(cafile=config.ca_file)
            cls.ssl_context.check_hostname = False
            cls.ssl_context.verify_mode = ssl.CERT_REQUIRED
        cls.ssl_context.load_cert_chain(config.certificate_file, config.key_file)
        return cls.ssl_context

    def send_text(self, text, code=200, content_type="text/plain", add_newline=True):
        """
        This method sends a text response.
        """
        if add_newline:
            text += "\n"

        self.send_response(code)
        response = bytes(text, encoding="utf-8")
        self.send_header("Content-Length", str(len(response)))
        self.send_header("Content-Type", content_type)
        self.end_headers()
        self.wfile.write(response)

    def check_request_cert(self):
        config = Config.shared()
        approved = not config.enable_tls or self.check_cert(
            self.connection.getpeercert(), config.peer_verification_rules
        )
        if not approved:
            self.send_error(401, "Client certificate was not approved")
        return approved

    def check_cert(self, cert, rules):
        """
        This method checks that the client's certificate is valid.

        If there is any problem with the certificate, this will return a string
        describing the error.
        """
        if not rules:
            return True

        for option in rules.split(";"):
            option_valid = True
            for rule in option.split(","):
                if not self.check_cert_rule(cert, rule):
                    option_valid = False
                    break

            if option_valid:
                return True

        return False

    def check_cert_rule(self, cert, rule):
        (key, expected_value) = rule.split("=", 1)
        if "." in key:
            (scope_key, field_key) = key.split(".", 1)
        else:
            scope_key = "S"
            field_key = key

        if scope_key == "S" or scope_key == "Subject":
            scope_name = "subject"
        elif scope_key == "I" or scope_key == "Issuer":
            scope_name = "issuer"
        elif scope_key == "R" or scope_key == "Root":
            scope_name = "root"
        else:
            assert False, "Unknown certificate scope %s" % scope_key

        if scope_name not in cert:
            return False

        rdns = None
        operator = ""
        if field_key == "CN":
            field_name = "commonName"
        elif field_key == "C":
            field_name = "country"
        elif field_key == "L":
            field_name = "localityName"
        elif field_key == "ST":
            field_name = "stateOrProvinceName"
        elif field_key == "O":
            field_name = "organizationName"
        elif field_key == "OU":
            field_name = "organizationalUnitName"
        elif field_key == "UID":
            field_name = "userId"
        elif field_key == "DC":
            field_name = "domainComponent"
        elif field_key.startswith("subjectAltName") and scope_name == "subject":
            operator = field_key[14:]
            field_key = field_key[0:14]
            (field_name, expected_value) = expected_value.split(":", 1)
            if field_key not in cert:
                return False
            rdns = [cert["subjectAltName"]]
        else:
            assert False, "Unknown certificate field %s" % field_key

        if not rdns:
            rdns = list(cert[scope_name])

        for rdn in rdns:
            for entry in list(rdn):
                if entry[0] == field_name:
                    if operator == "" and entry[1] == expected_value:
                        return True
                    elif operator == "<" and entry[1].endswith(expected_value):
                        return True
                    elif operator == ">" and entry[1].startswith(expected_value):
                        return True

    def do_GET(self):
        """
        This method executes a GET request.
        """
        try:
            if not self.check_request_cert():
                return
            if self.path.startswith("/check_hash/"):
                try:
                    self.send_text(check_hash(self.path[12:]), add_newline=False)
                except FileNotFoundError:
                    self.send_error(404, "Path not found")
                    self.end_headers()
            elif self.path == "/ready":
                self.send_text(ready())
            elif self.path == "/substitutions":
                self.send_text(get_substitutions())
            else:
                self.send_error(404, "Path not found")
                self.end_headers()
        except RequestException as e:
            self.send_error(400, e.message)
        except Exception as ex:
            log.error(f"Error processing request {ex}", exc_info=True)
            self.send_error(500)
            self.end_headers()

    def do_POST(self):
        """
        This method executes a POST request.
        """
        try:
            if not self.check_request_cert():
                return
            if self.path == "/copy_files":
                self.send_text(copy_files())
            elif self.path == "/copy_binaries":
                self.send_text(copy_binaries())
            elif self.path == "/copy_libraries":
                self.send_text(copy_libraries())
            elif self.path == "/copy_monitor_conf":
                self.send_text(copy_monitor_conf())
            elif self.path == "/refresh_certs":
                self.send_text(refresh_certs())
            elif self.path == "/restart":
                self.send_text("OK")
                exit(1)
            else:
                self.send_error(404, "Path not found")
                self.end_headers()
        except SystemExit as e:
            raise e
        except RequestException as e:
            self.send_error(400, e.message)
        except e:
            log.error("Error processing request", exc_info=True)
            self.send_error(500)
            self.end_headers()

    def log_message(self, format, *args):
        log.info(format % args)


class CertificateEventHandler(FileSystemEventHandler):
    def on_any_event(self, event):
        log.info("Detected change to certificates")
        time.sleep(10)
        log.info("Reloading certificates")
        Server.load_ssl_context()


def check_hash(filename):
    with open(os.path.join(Config.shared().output_dir, filename), "rb") as contents:
        m = hashlib.sha256()
        m.update(contents.read())
        return m.hexdigest()


def copy_files():
    config = Config.shared()
    if config.require_not_empty:
        for filename in config.require_not_empty:
            path = os.path.join(config.input_dir, filename)
            if not os.path.isfile(path) or os.path.getsize(path) == 0:
                raise Exception("No contents for file %s" % path)
    for filename in config.copy_files:
        tmp_file = os.path.join(config.output_dir, f"{filename}.tmp")
        shutil.copy(os.path.join(config.input_dir, filename), tmp_file)
        os.replace(tmp_file, os.path.join(config.output_dir, filename))

    return "OK"


def copy_binaries():
    config = Config.shared()
    if config.main_container_version != config.primary_version:
        for binary in config.copy_binaries:
            path = Path(f"/usr/bin/{binary}")
            target_path = Path(
                f"{config.output_dir}/bin/{config.primary_version}/{binary}"
            )
            if not target_path.exists():
                target_path.parent.mkdir(parents=True, exist_ok=True)
                tmp_file = f"{target_path}.tmp"
                shutil.copy(path, tmp_file)
                os.replace(tmp_file, target_path)
                target_path.chmod(0o744)
    return "OK"


def copy_libraries():
    config = Config.shared()
    for version in config.copy_libraries:
        path = Path(f"/var/fdb/lib/libfdb_c_{version}.so")
        if version == config.copy_libraries[0]:
            target_path = Path(f"{config.output_dir}/lib/libfdb_c.so")
        else:
            target_path = Path(
                f"{config.output_dir}/lib/multiversion/libfdb_c_{version}.so"
            )
        if not target_path.exists():
            target_path.parent.mkdir(parents=True, exist_ok=True)
            tmp_file = f"{target_path}.tmp"
            shutil.copy(path, tmp_file)
            os.replace(tmp_file, target_path)
    return "OK"


def copy_monitor_conf():
    config = Config.shared()
    if config.input_monitor_conf:
        with open(
            os.path.join(config.input_dir, config.input_monitor_conf)
        ) as monitor_conf_file:
            monitor_conf = monitor_conf_file.read()
        for variable in config.substitutions:
            monitor_conf = monitor_conf.replace(
                "$" + variable, config.substitutions[variable]
            )

        tmp_file = os.path.join(config.output_dir, "fdbmonitor.conf.tmp")
        target_file = os.path.join(config.output_dir, "fdbmonitor.conf")

        with open(tmp_file, "w") as output_conf_file:
            output_conf_file.write(monitor_conf)

        os.replace(tmp_file, target_file)
    return "OK"


def get_substitutions():
    return json.dumps(Config.shared().substitutions)


def ready():
    return "OK"


def refresh_certs():
    if not Config.shared().enable_tls:
        raise RequestException("Server is not using TLS")
    Server.load_ssl_context()
    return "OK"


class RequestException(Exception):
    def __init__(self, message):
        super().__init__(message)
        self.message = message


if __name__ == "__main__":
    logging.basicConfig(format="%(asctime)-15s %(levelname)s %(message)s")
    copy_files()
    copy_binaries()
    copy_libraries()
    copy_monitor_conf()

    if not Config.shared().init_mode:
        Server.start()
