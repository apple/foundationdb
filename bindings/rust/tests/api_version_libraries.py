#!/usr/bin/env python3
"""Exercise independent Rust API-selection state against one shared C client."""

import argparse
import contextlib
import ctypes
from pathlib import Path
import shutil
import subprocess
import sys
import tempfile
import threading


def load_client(path):
    client = ctypes.CDLL(str(path), mode=ctypes.RTLD_LOCAL)
    client.fdb_select_api_version_impl.argtypes = [ctypes.c_int, ctypes.c_int]
    client.fdb_select_api_version_impl.restype = ctypes.c_int
    client.fdb_get_selected_api_versions.argtypes = [
        ctypes.POINTER(ctypes.c_int),
        ctypes.POINTER(ctypes.c_int),
    ]
    client.fdb_get_selected_api_versions.restype = None
    for name in ("fdb_setup_network", "fdb_run_network", "fdb_stop_network"):
        function = getattr(client, name)
        function.argtypes = []
        function.restype = ctypes.c_int
    client.fdb_create_database.argtypes = [
        ctypes.c_char_p,
        ctypes.POINTER(ctypes.c_void_p),
    ]
    client.fdb_create_database.restype = ctypes.c_int
    client.fdb_database_destroy.argtypes = [ctypes.c_void_p]
    client.fdb_database_destroy.restype = None
    client.fdb_get_error.argtypes = [ctypes.c_int]
    client.fdb_get_error.restype = ctypes.c_char_p
    return client


def load_rust(path):
    library = ctypes.CDLL(str(path), mode=ctypes.RTLD_LOCAL)
    library.test_select_api.argtypes = [ctypes.c_int]
    library.test_select_api.restype = ctypes.c_int
    library.test_versionstamped_mutations.argtypes = [ctypes.c_void_p]
    library.test_versionstamped_mutations.restype = ctypes.c_int
    return library


def require(condition, message):
    if not condition:
        raise AssertionError(message)


def check(client, error, operation):
    if error:
        description = client.fdb_get_error(error).decode("utf-8")
        raise RuntimeError(f"{operation}: {error} ({description})")


def selected_versions(client):
    runtime = ctypes.c_int(-1)
    header = ctypes.c_int(-1)
    client.fdb_get_selected_api_versions(ctypes.byref(runtime), ctypes.byref(header))
    return runtime.value, header.value


def concurrent_selections(*operations):
    barrier = threading.Barrier(len(operations), timeout=10)
    results = [None] * len(operations)
    errors = [None] * len(operations)

    def select(index, operation):
        try:
            barrier.wait()
            # CDLL releases the GIL while each independent library enters C.
            results[index] = operation()
        except Exception as error:
            errors[index] = error

    threads = [
        threading.Thread(target=select, args=(index, operation), daemon=True)
        for index, operation in enumerate(operations)
    ]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join(timeout=10)
        require(not thread.is_alive(), "API selection did not finish within 10 seconds")
    for error in errors:
        if error is not None:
            raise RuntimeError("concurrent API selection failed") from error
    return results


@contextlib.contextmanager
def running_database(client, cluster_file):
    check(client, client.fdb_setup_network(), "set up network")
    network_result = []
    network = threading.Thread(
        target=lambda: network_result.append(client.fdb_run_network()), daemon=True
    )
    network.start()
    database = ctypes.c_void_p()
    try:
        cluster_path = str(cluster_file).encode() if cluster_file else None
        check(
            client,
            client.fdb_create_database(cluster_path, ctypes.byref(database)),
            "create database",
        )
        yield database
    finally:
        if database.value:
            client.fdb_database_destroy(database)
        stop_error = client.fdb_stop_network()
        network.join(timeout=10)
        require(not network.is_alive(), "network thread did not stop within 10 seconds")
        check(client, stop_error, "stop network")
        require(len(network_result) == 1, "network thread did not return a result")
        check(client, network_result[0], "run network")


def run_case(args):
    client = load_client(args.client_library)
    require(selected_versions(client) == (0, 0), "API was selected before the test")
    with tempfile.TemporaryDirectory(prefix="fdb-rust-api-libraries-") as directory:
        first_path = Path(directory) / ("first" + args.rust_library.suffix)
        second_path = Path(directory) / ("second" + args.rust_library.suffix)
        shutil.copyfile(args.rust_library, first_path)
        shutil.copyfile(args.rust_library, second_path)
        require(
            not first_path.samefile(second_path),
            "Rust fixtures must be distinct files so dlopen does not reuse one image",
        )
        first = load_rust(first_path)
        if args.case == "incompatible-header":
            check(
                client,
                client.fdb_select_api_version_impl(730, 730),
                "select native API",
            )
            require(
                first.test_select_api(730) == 2201, "accepted a mismatched header API"
            )
            require(selected_versions(client) == (730, 730), "changed the native API")
            return

        second = load_rust(second_path)
        if args.case == "concurrent-matching":
            results = concurrent_selections(
                lambda: first.test_select_api(740),
                lambda: second.test_select_api(740),
            )
            require(results == [0, 0], f"matching API selections failed: {results}")
            expected_versions = (740, 740)
        elif args.case == "concurrent-runtime-mismatch":
            results = concurrent_selections(
                lambda: first.test_select_api(740),
                lambda: second.test_select_api(730),
            )
            require(
                sorted(results) == [0, 2201],
                f"conflicting runtime selections must have one winner: {results}",
            )
            expected_versions = ((740, 730)[results.index(0)], 740)
        elif args.case == "concurrent-header-mismatch":
            results = concurrent_selections(
                lambda: first.test_select_api(730),
                lambda: client.fdb_select_api_version_impl(730, 730),
            )
            require(
                sorted(results) == [0, 2201],
                f"conflicting header selections must have one winner: {results}",
            )
            expected_versions = (730, (740, 730)[results.index(0)])
            require(
                selected_versions(client) == expected_versions, "changed the API pair"
            )
            if expected_versions[1] == 730:
                for library in (first, second):
                    require(
                        library.test_select_api(730) == 2201,
                        "accepted the incompatible winning header API",
                    )
                return
        else:
            check(
                client, first.test_select_api(740), "select API in first Rust library"
            )
            require(
                selected_versions(client) == (740, 740),
                "first library used another client",
            )
            require(
                second.test_select_api(730) == 2201, "accepted a mismatched runtime API"
            )
            expected_versions = (740, 740)

        require(
            selected_versions(client) == expected_versions, "wrong API pair selected"
        )
        for name, library in (("first", first), ("second", second)):
            check(
                client,
                library.test_select_api(expected_versions[0]),
                f"adopt or repeat winning API in {name} Rust library",
            )
        require(
            selected_versions(client) == expected_versions, "changed the selected API"
        )
        with running_database(client, args.cluster_file) as database:
            for name, library in (("first", first), ("second", second)):
                check(
                    client,
                    library.test_versionstamped_mutations(database),
                    f"versionstamp mutations in {name} Rust library",
                )


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--client-library", required=True, type=Path)
    parser.add_argument("--rust-library", required=True, type=Path)
    parser.add_argument("--cluster-file", type=Path)
    cases = (
        "shared-selection",
        "incompatible-header",
        "concurrent-matching",
        "concurrent-runtime-mismatch",
        "concurrent-header-mismatch",
    )
    parser.add_argument("--case", choices=cases, help=argparse.SUPPRESS)
    args = parser.parse_args()
    if args.case:
        run_case(args)
        print(f"Rust API library regression passed: {args.case}", flush=True)
        return

    # API selection is irreversible, so each compatibility boundary needs a
    # fresh process. An abort from either Rust library must fail the parent.
    for case in cases:
        command = [
            sys.executable,
            str(Path(__file__).resolve()),
            "--client-library",
            str(args.client_library.resolve()),
            "--rust-library",
            str(args.rust_library.resolve()),
            "--case",
            case,
        ]
        if args.cluster_file:
            command.extend(("--cluster-file", str(args.cluster_file.resolve())))
        subprocess.run(command, check=True, timeout=60)


if __name__ == "__main__":
    main()
