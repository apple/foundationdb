#!/usr/bin/env python3
import argparse
from pathlib import Path
import shutil
import subprocess
import sys
import os
import glob
import unittest
import json
import re

from threading import Thread
import time
from fdb_version import CURRENT_VERSION, PREV_RELEASE_VERSION, PREV2_RELEASE_VERSION
from binary_download import FdbBinaryDownloader
from local_cluster import LocalCluster, PortProvider
from test_util import random_alphanum_string

args = None
downloader = None


def version_from_str(ver_str):
    ver = [int(s) for s in ver_str.split(".")]
    assert len(ver) == 3, "Invalid version string {}".format(ver_str)
    return ver


def api_version_from_str(ver_str):
    ver_tuple = version_from_str(ver_str)
    return ver_tuple[0] * 100 + ver_tuple[1] * 10


class TestCluster(LocalCluster):
    def __init__(
        self,
        version: str,
    ):
        self.client_config_tester_bin = Path(args.client_config_tester_bin).resolve()
        assert self.client_config_tester_bin.exists(), "{} does not exist".format(self.client_config_tester_bin)
        self.build_dir = Path(args.build_dir).resolve()
        assert self.build_dir.exists(), "{} does not exist".format(args.build_dir)
        assert self.build_dir.is_dir(), "{} is not a directory".format(args.build_dir)
        self.tmp_dir = self.build_dir.joinpath("tmp", random_alphanum_string(16))
        print("Creating temp dir {}".format(self.tmp_dir), file=sys.stderr)
        self.tmp_dir.mkdir(parents=True)
        self.version = version
        super().__init__(
            self.tmp_dir,
            downloader.binary_path(version, "fdbserver"),
            downloader.binary_path(version, "fdbmonitor"),
            downloader.binary_path(version, "fdbcli"),
            1,
        )
        self.set_env_var("LD_LIBRARY_PATH", downloader.lib_dir(version))

    def setup(self):
        self.__enter__()
        self.create_database()

    def tear_down(self):
        self.__exit__(None, None, None)
        shutil.rmtree(self.tmp_dir)

    def upgrade_to(self, version):
        self.stop_cluster()
        self.fdbmonitor_binary = downloader.binary_path(version, "fdbmonitor")
        self.fdbserver_binary = downloader.binary_path(version, "fdbserver")
        self.fdbcli_binary = downloader.binary_path(version, "fdbcli")
        self.set_env_var("LD_LIBRARY_PATH", "%s:%s" % (downloader.lib_dir(version), os.getenv("LD_LIBRARY_PATH")))
        self.save_config()
        self.ensure_ports_released()
        self.start_cluster()


# Client configuration tests using a cluster of the current version
class ClientConfigTest:
    def __init__(self, tc: unittest.TestCase):
        self.tc = tc
        self.cluster = tc.cluster
        self.external_lib_dir = None
        self.external_lib_path = None
        self.test_dir = self.cluster.tmp_dir.joinpath(random_alphanum_string(16))
        self.test_dir.mkdir(parents=True)
        self.log_dir = self.test_dir.joinpath("log")
        self.log_dir.mkdir(parents=True)
        self.tmp_dir = self.test_dir.joinpath("tmp")
        self.tmp_dir.mkdir(parents=True)
        self.test_cluster_file = self.cluster.cluster_file
        self.port_provider = PortProvider()
        self.status_json = None

        # Configuration parameters to be set directly as needed
        self.disable_local_client = False
        self.disable_client_bypass = False
        self.ignore_external_client_failures = False
        self.fail_incompatible_client = True
        self.api_version = None
        self.expected_error = None
        self.transaction_timeout = None
        self.print_status = False
        self.trace_file_identifier = None
        self.trace_initialize_on_setup = False
        self.trace_format = None

    # ----------------------------
    # Configuration methods
    # ----------------------------

    def create_external_lib_dir(self, versions):
        self.external_lib_dir = self.test_dir.joinpath("extclients")
        self.external_lib_dir.mkdir(parents=True)
        for version in versions:
            src_file_path = downloader.lib_path(version)
            self.tc.assertTrue(src_file_path.exists(), "{} does not exist".format(src_file_path))
            target_file_path = self.external_lib_dir.joinpath("libfdb_c.{}.so".format(version))
            shutil.copyfile(src_file_path, target_file_path)
            self.tc.assertTrue(target_file_path.exists(), "{} does not exist".format(target_file_path))

    def create_external_lib_path(self, version):
        src_file_path = downloader.lib_path(version)
        self.tc.assertTrue(src_file_path.exists(), "{} does not exist".format(src_file_path))
        self.external_lib_path = self.test_dir.joinpath("libfdb_c.{}.so".format(version))
        shutil.copyfile(src_file_path, self.external_lib_path)
        self.tc.assertTrue(self.external_lib_path.exists(), "{} does not exist".format(self.external_lib_path))

    def create_cluster_file_with_wrong_port(self):
        self.test_cluster_file = self.test_dir.joinpath("{}.cluster".format(random_alphanum_string(16)))
        port = self.cluster.port_provider.get_free_port()
        with open(self.test_cluster_file, "w") as file:
            file.write("abcde:fghijk@127.0.0.1:{}".format(port))

    def create_invalid_cluster_file(self):
        self.test_cluster_file = self.test_dir.joinpath("{}.cluster".format(random_alphanum_string(16)))
        port = self.cluster.port_provider.get_free_port()
        with open(self.test_cluster_file, "w") as file:
            file.write("abcde:fghijk@")

    # ----------------------------
    # Status check methods
    # ----------------------------

    def check_initialization_state(self, expected_state):
        self.tc.assertIsNotNone(self.status_json)
        self.tc.assertTrue("InitializationState" in self.status_json)
        self.tc.assertEqual(expected_state, self.status_json["InitializationState"])

    def check_available_clients(self, expected_clients):
        self.tc.assertIsNotNone(self.status_json)
        self.tc.assertTrue("AvailableClients" in self.status_json)
        actual_clients = [client["ReleaseVersion"] for client in self.status_json["AvailableClients"]]
        self.tc.assertEqual(set(expected_clients), set(actual_clients))

    def check_protocol_version_not_set(self):
        self.tc.assertIsNotNone(self.status_json)
        self.tc.assertFalse("ProtocolVersion" in self.status_json)

    def check_current_client(self, expected_client):
        self.tc.assertIsNotNone(self.status_json)
        self.tc.assertTrue("AvailableClients" in self.status_json)
        self.tc.assertTrue("ProtocolVersion" in self.status_json)
        matching_clients = [
            client["ReleaseVersion"]
            for client in self.status_json["AvailableClients"]
            if client["ProtocolVersion"] == self.status_json["ProtocolVersion"]
        ]
        if expected_client is None:
            self.tc.assertEqual(0, len(matching_clients))
        else:
            self.tc.assertEqual(1, len(matching_clients))
            self.tc.assertEqual(expected_client, matching_clients[0])

    def check_healthy_status_report(self):
        self.tc.assertIsNotNone(self.status_json)
        expected_mvc_attributes = {
            "Healthy",
            "InitializationState",
            "DatabaseStatus",
            "ProtocolVersion",
            "AvailableClients",
            "ConnectionRecord",
        }
        self.tc.assertEqual(expected_mvc_attributes, set(self.status_json.keys()))
        self.tc.assertEqual("created", self.status_json["InitializationState"])
        self.tc.assertGreater(len(self.status_json["AvailableClients"]), 0)

        expected_db_attributes = {
            "Healthy",
            "Coordinators",
            "CurrentCoordinator",
            "ClusterID",
            "GrvProxies",
            "CommitProxies",
            "StorageServers",
            "Connections",
            "NumConnectionsFailed",
        }
        db_status = self.status_json["DatabaseStatus"]
        self.tc.assertEqual(expected_db_attributes, set(db_status.keys()))
        self.tc.assertTrue(db_status["Healthy"])
        self.tc.assertGreater(len(db_status["Coordinators"]), 0)
        self.tc.assertGreater(len(db_status["GrvProxies"]), 0)
        self.tc.assertGreater(len(db_status["CommitProxies"]), 0)
        self.tc.assertGreater(len(db_status["StorageServers"]), 0)
        self.tc.assertGreater(len(db_status["Connections"]), 0)
        self.tc.assertEqual(0, db_status["NumConnectionsFailed"])
        self.tc.assertTrue(self.status_json["Healthy"])

    def check_healthy_status(self, expected_is_healthy):
        self.tc.assertIsNotNone(self.status_json)
        self.tc.assertTrue("Healthy" in self.status_json)
        self.tc.assertEqual(expected_is_healthy, self.status_json["Healthy"])

    def list_trace_files(self):
        return glob.glob(os.path.join(self.log_dir, "*"))

    # ----------------------------
    # Executing the test
    # ----------------------------

    def exec(self):
        cmd_args = [self.cluster.client_config_tester_bin, "--cluster-file", self.test_cluster_file]

        if self.tmp_dir is not None:
            cmd_args += ["--tmp-dir", self.tmp_dir]

        if self.log_dir is not None:
            cmd_args += ["--log", "--log-dir", self.log_dir]

        if self.disable_local_client:
            cmd_args += ["--network-option-disable_local_client", ""]

        if self.disable_client_bypass:
            cmd_args += ["--network-option-disable_client_bypass", ""]

        if self.external_lib_path is not None:
            cmd_args += ["--external-client-library", self.external_lib_path]

        if self.external_lib_dir is not None:
            cmd_args += ["--external-client-dir", self.external_lib_dir]

        if self.ignore_external_client_failures:
            cmd_args += ["--network-option-ignore_external_client_failures", ""]

        if self.fail_incompatible_client:
            cmd_args += ["--network-option-fail_incompatible_client", ""]

        if self.trace_file_identifier is not None:
            cmd_args += ["--network-option-trace_file_identifier", self.trace_file_identifier]

        if self.trace_initialize_on_setup:
            cmd_args += ["--network-option-trace_initialize_on_setup", ""]

        if self.trace_format is not None:
            cmd_args += ["--network-option-trace_format", self.trace_format]

        if self.api_version is not None:
            cmd_args += ["--api-version", str(self.api_version)]

        if self.expected_error is not None:
            cmd_args += ["--expected-error", str(self.expected_error)]

        if self.transaction_timeout is not None:
            cmd_args += ["--transaction-timeout", str(self.transaction_timeout)]

        if self.print_status:
            cmd_args += ["--print-status"]

        print("\nExecuting test command: {}".format(" ".join([str(c) for c in cmd_args])), file=sys.stderr)
        tester_proc = subprocess.Popen(cmd_args, stdout=subprocess.PIPE, stderr=sys.stderr)
        out, _ = tester_proc.communicate()
        self.tc.assertEqual(0, tester_proc.returncode)
        if self.print_status:
            # Parse the output as status json
            try:
                self.status_json = json.loads(out)
            except json.JSONDecodeError as e:
                print("Error '{}' parsing output {}".format(e, out.decode()), file=sys.stderr)
            self.tc.assertIsNotNone(self.status_json)
            print("Status: ", self.status_json, file=sys.stderr)
        else:
            # Otherwise redirect the output to the console
            print(out.decode(), file=sys.stderr)


class ClientConfigTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.cluster = TestCluster(CURRENT_VERSION)
        cls.cluster.setup()

    @classmethod
    def tearDownClass(cls):
        cls.cluster.tear_down()

    def test_local_client_only(self):
        # Local client only
        test = ClientConfigTest(self)
        test.print_status = True
        test.exec()
        test.check_healthy_status(True)

    def test_disable_mvc_bypass(self):
        # Local client only
        test = ClientConfigTest(self)
        test.print_status = True
        test.disable_client_bypass = True
        test.exec()
        test.check_healthy_status_report()
        test.check_available_clients([CURRENT_VERSION])
        test.check_current_client(CURRENT_VERSION)

    def test_single_external_client_only(self):
        # Single external client only
        test = ClientConfigTest(self)
        test.print_status = True
        test.create_external_lib_path(CURRENT_VERSION)
        test.disable_local_client = True
        test.exec()
        test.check_healthy_status_report()
        test.check_available_clients([CURRENT_VERSION])
        test.check_current_client(CURRENT_VERSION)

    def test_same_local_and_external_client(self):
        # Same version local & external client
        test = ClientConfigTest(self)
        test.print_status = True
        test.create_external_lib_path(CURRENT_VERSION)
        test.exec()
        test.check_healthy_status_report()
        test.check_available_clients([CURRENT_VERSION])
        test.check_current_client(CURRENT_VERSION)

    def test_multiple_external_clients(self):
        # Multiple external clients, normal case
        test = ClientConfigTest(self)
        test.print_status = True
        test.create_external_lib_dir([CURRENT_VERSION, PREV_RELEASE_VERSION, PREV2_RELEASE_VERSION])
        test.disable_local_client = True
        test.api_version = api_version_from_str(PREV2_RELEASE_VERSION)
        test.exec()
        test.check_healthy_status_report()
        test.check_available_clients([CURRENT_VERSION, PREV_RELEASE_VERSION, PREV2_RELEASE_VERSION])
        test.check_current_client(CURRENT_VERSION)

    def test_no_external_client_support_api_version(self):
        # Multiple external clients, API version supported by none of them
        test = ClientConfigTest(self)
        test.create_external_lib_dir([PREV2_RELEASE_VERSION, PREV_RELEASE_VERSION])
        test.disable_local_client = True
        test.api_version = api_version_from_str(CURRENT_VERSION)
        test.expected_error = 2204  # API function missing
        test.exec()

    def test_no_external_client_support_api_version_ignore(self):
        # Multiple external clients; API version supported by none of them; Ignore failures
        test = ClientConfigTest(self)
        test.create_external_lib_dir([PREV2_RELEASE_VERSION, PREV_RELEASE_VERSION])
        test.disable_local_client = True
        test.api_version = api_version_from_str(CURRENT_VERSION)
        test.ignore_external_client_failures = True
        test.expected_error = 2124  # All external clients failed
        test.exec()

    def test_one_external_client_wrong_api_version(self):
        # Multiple external clients, API version unsupported by one of othem
        test = ClientConfigTest(self)
        test.create_external_lib_dir([CURRENT_VERSION, PREV_RELEASE_VERSION, PREV2_RELEASE_VERSION])
        test.disable_local_client = True
        test.api_version = api_version_from_str(CURRENT_VERSION)
        test.expected_error = 2204  # API function missing
        test.exec()

    def test_one_external_client_wrong_api_version_ignore(self):
        # Multiple external clients;  API version unsupported by one of them; Ignore failures
        test = ClientConfigTest(self)
        test.print_status = True
        test.create_external_lib_dir([CURRENT_VERSION, PREV_RELEASE_VERSION, PREV2_RELEASE_VERSION])
        test.disable_local_client = True
        test.api_version = api_version_from_str(CURRENT_VERSION)
        test.ignore_external_client_failures = True
        test.exec()
        test.check_healthy_status_report()
        test.check_available_clients([CURRENT_VERSION])
        test.check_current_client(CURRENT_VERSION)

    def test_external_client_not_matching_cluster_version(self):
        # Trying to connect to a cluster having only
        # an external client with not matching protocol version
        test = ClientConfigTest(self)
        test.print_status = True
        test.create_external_lib_path(PREV_RELEASE_VERSION)
        test.disable_local_client = True
        test.api_version = api_version_from_str(PREV_RELEASE_VERSION)
        test.transaction_timeout = 5000
        test.expected_error = 2125  # Incompatible client
        test.exec()
        test.check_initialization_state("incompatible")
        test.check_healthy_status(False)
        test.check_available_clients([PREV_RELEASE_VERSION])
        test.check_current_client(None)

    def test_external_client_not_matching_cluster_version_ignore(self):
        # Trying to connect to a cluster having only
        # an external client with not matching protocol version;
        # Ignore incompatible client
        test = ClientConfigTest(self)
        test.print_status = True
        test.create_external_lib_path(PREV_RELEASE_VERSION)
        test.disable_local_client = True
        test.api_version = api_version_from_str(PREV_RELEASE_VERSION)
        test.fail_incompatible_client = False
        test.transaction_timeout = 100
        test.expected_error = 1031  # Timeout
        test.exec()
        test.check_initialization_state("incompatible")
        test.check_healthy_status(False)
        test.check_available_clients([PREV_RELEASE_VERSION])
        test.check_current_client(None)

    def test_cannot_connect_to_coordinator(self):
        # Testing a cluster file with a valid address, but no server behind it
        test = ClientConfigTest(self)
        test.print_status = True
        test.create_external_lib_path(CURRENT_VERSION)
        test.disable_local_client = True
        test.api_version = api_version_from_str(CURRENT_VERSION)
        test.transaction_timeout = 100
        test.create_cluster_file_with_wrong_port()
        test.expected_error = 1031  # Timeout
        test.exec()
        test.check_initialization_state("initializing")
        test.check_healthy_status(False)
        test.check_available_clients([CURRENT_VERSION])
        test.check_protocol_version_not_set()

    def test_invalid_cluster_file(self):
        # Testing with an invalid cluster file
        test = ClientConfigTest(self)
        test.print_status = True
        test.create_external_lib_path(CURRENT_VERSION)
        test.disable_local_client = True
        test.api_version = api_version_from_str(CURRENT_VERSION)
        test.transaction_timeout = 5000
        test.create_invalid_cluster_file()
        test.expected_error = 2104  # Connection string invalid
        test.exec()
        test.check_initialization_state("initialization_failed")
        test.check_healthy_status(False)
        test.check_available_clients([CURRENT_VERSION])
        test.check_protocol_version_not_set()


# Client configuration tests using a cluster of previous release version
class ClientConfigPrevVersionTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.cluster = TestCluster(PREV_RELEASE_VERSION)
        cls.cluster.setup()

    @classmethod
    def tearDownClass(cls):
        cls.cluster.tear_down()

    def test_external_client(self):
        # Using an external client to connect
        test = ClientConfigTest(self)
        test.print_status = True
        test.create_external_lib_path(PREV_RELEASE_VERSION)
        test.api_version = api_version_from_str(PREV_RELEASE_VERSION)
        test.exec()
        test.check_initialization_state("created")
        test.check_healthy_status(False)
        test.check_available_clients([PREV_RELEASE_VERSION, CURRENT_VERSION])
        test.check_current_client(PREV_RELEASE_VERSION)

    def test_external_client_unsupported_api(self):
        # Leaving an unsupported API version
        test = ClientConfigTest(self)
        test.create_external_lib_path(PREV_RELEASE_VERSION)
        test.expected_error = 2204  # API function missing
        test.exec()

    def test_external_client_unsupported_api_ignore(self):
        # Leaving an unsupported API version, ignore failures
        test = ClientConfigTest(self)
        test.print_status = True
        test.create_external_lib_path(PREV_RELEASE_VERSION)
        test.transaction_timeout = 5000
        test.expected_error = 2125  # Incompatible client
        test.ignore_external_client_failures = True
        test.exec()
        test.check_initialization_state("incompatible")
        test.check_healthy_status(False)
        test.check_available_clients([CURRENT_VERSION])
        test.check_current_client(None)


# Client configuration tests using a separate cluster for each test
class ClientConfigSeparateCluster(unittest.TestCase):
    def test_wait_cluster_to_upgrade(self):
        # Test starting a client incompatible to a cluster and connecting
        # successfuly after cluster upgrade
        self.cluster = TestCluster(PREV_RELEASE_VERSION)
        self.cluster.setup()
        try:
            test = ClientConfigTest(self)
            test.print_status = True
            test.create_external_lib_path(CURRENT_VERSION)
            test.transaction_timeout = 10000
            test.fail_incompatible_client = False

            def upgrade(cluster):
                time.sleep(0.1)
                cluster.upgrade_to(CURRENT_VERSION)

            t = Thread(target=upgrade, args=(self.cluster,))
            t.start()
            test.exec()
            test.check_healthy_status_report()
            test.check_available_clients([CURRENT_VERSION])
            test.check_current_client(CURRENT_VERSION)
            t.join()
        finally:
            self.cluster.tear_down()


# Test client-side tracing
class ClientTracingTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.cluster = TestCluster(CURRENT_VERSION)
        cls.cluster.setup()

    @classmethod
    def tearDownClass(cls):
        cls.cluster.tear_down()

    def test_default_config_normal_case(self):
        # Test trace files created with a default trace configuration
        # in a normal case
        test = self.test
        test.create_external_lib_dir([CURRENT_VERSION, PREV_RELEASE_VERSION])
        test.api_version = api_version_from_str(PREV_RELEASE_VERSION)
        test.disable_local_client = True

        self.exec_test()
        self.assertEqual(3, len(self.trace_files))
        primary_trace = self.find_trace_file(with_ip=True)
        self.find_and_check_event(primary_trace, "ClientStart", ["Machine"], [])
        cur_ver_trace = self.find_trace_file(with_ip=True, version=CURRENT_VERSION, thread_idx=0)
        self.find_and_check_event(cur_ver_trace, "ClientStart", ["Machine"], [])
        prev_ver_trace = self.find_trace_file(with_ip=True, version=PREV_RELEASE_VERSION, thread_idx=0)
        self.find_and_check_event(prev_ver_trace, "ClientStart", ["Machine"], [])

    def test_default_config_error_case(self):
        # Test that no trace files are created with a default configuration
        # when an a client fails to initialize
        test = self.test
        test.create_external_lib_dir([CURRENT_VERSION, PREV_RELEASE_VERSION])
        test.api_version = api_version_from_str(CURRENT_VERSION)
        test.disable_local_client = True
        test.expected_error = 2204  # API function missing

        self.exec_test()
        self.assertEqual(0, len(self.trace_files))

    def test_init_on_setup_normal_case(self):
        # Test trace files created with trace_initialize_on_setup option
        # in a normal case
        test = self.test
        test.create_external_lib_dir([CURRENT_VERSION])
        test.api_version = api_version_from_str(CURRENT_VERSION)
        test.disable_local_client = True
        test.trace_initialize_on_setup = True

        self.exec_test()
        self.assertEqual(2, len(self.trace_files))
        primary_trace = self.find_trace_file()
        # The machine address will be available only in the second ClientStart event
        self.find_and_check_event(primary_trace, "ClientStart", [], ["Machine"])
        self.find_and_check_event(primary_trace, "ClientStart", ["Machine"], [], seqno=1)
        cur_ver_trace = self.find_trace_file(version=CURRENT_VERSION, thread_idx=0)
        self.find_and_check_event(cur_ver_trace, "ClientStart", [], ["Machine"])
        self.find_and_check_event(cur_ver_trace, "ClientStart", ["Machine"], [], seqno=1)

    def test_init_on_setup_trace_error_case(self):
        # Test trace files created with trace_initialize_on_setup option
        # when an a client fails to initialize
        test = self.test
        test.create_external_lib_dir([CURRENT_VERSION, PREV_RELEASE_VERSION])
        test.api_version = api_version_from_str(CURRENT_VERSION)
        test.disable_local_client = True
        test.trace_initialize_on_setup = True
        test.expected_error = 2204  # API function missing

        self.exec_test()
        self.assertEqual(1, len(self.trace_files))
        primary_trace = self.find_trace_file()
        self.find_and_check_event(primary_trace, "ClientStart", [], ["Machine"])

    def test_trace_identifier(self):
        # Test trace files created with file identifier
        test = self.test
        test.create_external_lib_dir([CURRENT_VERSION])
        test.api_version = api_version_from_str(CURRENT_VERSION)
        test.disable_local_client = True
        test.trace_file_identifier = "fdbclient"

        self.exec_test()
        self.assertEqual(2, len(self.trace_files))
        self.find_trace_file(with_ip=True, identifier="fdbclient")
        self.find_trace_file(with_ip=True, identifier="fdbclient", version=CURRENT_VERSION, thread_idx=0)

    def test_init_on_setup_and_trace_identifier(self):
        # Test trace files created with trace_initialize_on_setup option
        # and file identifier
        test = self.test
        test.create_external_lib_dir([CURRENT_VERSION])
        test.api_version = api_version_from_str(CURRENT_VERSION)
        test.disable_local_client = True
        test.trace_initialize_on_setup = True
        test.trace_file_identifier = "fdbclient"

        self.exec_test()
        self.assertEqual(2, len(self.trace_files))
        self.find_trace_file(identifier="fdbclient")
        self.find_trace_file(identifier="fdbclient", version=CURRENT_VERSION, thread_idx=0)

    # ---------------
    # Helper methods
    # ---------------

    def setUp(self):
        self.test = ClientConfigTest(self)
        self.trace_files = None
        self.test.trace_format = "json"

    def exec_test(self):
        self.test.exec()
        self.trace_files = self.test.list_trace_files()
        if self.test.trace_format == "json":
            self.load_trace_file_events()

    def load_trace_file_events(self):
        self.trace_file_events = {}
        for trace in self.trace_files:
            events = []
            with open(trace, "r") as f:
                for line in f:
                    events.append(json.loads(line))
            self.trace_file_events[trace] = events

    def find_trace_file(self, with_ip=False, identifier=None, version=None, thread_idx=None):
        self.assertIsNotNone(self.trace_files)
        for trace_file in self.trace_files:
            name = os.path.basename(trace_file)
            # trace prefix must be in all files
            self.assertTrue(name.startswith("trace."))
            pattern = "^trace\."
            if with_ip:
                pattern += "127\.0\.0\.1\."
            else:
                pattern += "0\.0\.0\.0\."
            if identifier is not None:
                pattern += identifier
            else:
                pattern += "\d+"
            if version is not None:
                pattern += "_v{}".format(version.replace(".", "_"))
            if thread_idx is not None:
                pattern += "t{}".format(thread_idx)
            pattern += "\.\d+\.\w+\.\d+\.\d+\.{}$".format(self.test.trace_format)
            if re.match(pattern, name):
                return trace_file
        self.fail("No maching trace file found")

    def find_and_check_event(self, trace_file, event_type, attr_present, attr_missing, seqno=0):
        self.assertTrue(trace_file in self.trace_file_events)
        for event in self.trace_file_events[trace_file]:
            if event["Type"] == event_type:
                if seqno > 0:
                    seqno -= 1
                    continue
                for attr in attr_present:
                    self.assertTrue(attr in event)
                for attr in attr_missing:
                    self.assertFalse(attr in event)
                return
        self.fail("No matching event found")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        formatter_class=argparse.RawDescriptionHelpFormatter,
        description="""
        Unit tests for running FDB client with different configurations. 
        Also accepts python unit tests command line arguments.
        """,
    )
    parser.add_argument(
        "--build-dir",
        "-b",
        metavar="BUILD_DIRECTORY",
        help="FDB build directory",
        required=True,
    )
    parser.add_argument(
        "--client-config-tester-bin",
        type=str,
        help="Path to the fdb_c_client_config_tester executable.",
        required=True,
    )
    parser.add_argument("unittest_args", nargs=argparse.REMAINDER)

    args = parser.parse_args()
    sys.argv[1:] = args.unittest_args

    downloader = FdbBinaryDownloader(args.build_dir)
    downloader.download_old_binaries(PREV_RELEASE_VERSION)
    downloader.download_old_binaries(PREV2_RELEASE_VERSION)

    unittest.main(verbosity=2)
