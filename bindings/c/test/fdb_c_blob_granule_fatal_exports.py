#!/usr/bin/env python3

import argparse
import ctypes
import os
from pathlib import Path
import signal
import subprocess
import sys


class FDBGranuleSummary(ctypes.Structure):
    pass


StartLoad = ctypes.CFUNCTYPE(
    ctypes.c_int64,
    ctypes.c_char_p,
    ctypes.c_int,
    ctypes.c_int64,
    ctypes.c_int64,
    ctypes.c_int64,
    ctypes.c_void_p,
)
GetLoad = ctypes.CFUNCTYPE(ctypes.c_void_p, ctypes.c_int64, ctypes.c_void_p)
FreeLoad = ctypes.CFUNCTYPE(None, ctypes.c_int64, ctypes.c_void_p)


class FDBReadBlobGranuleContext(ctypes.Structure):
    _fields_ = [
        ("userContext", ctypes.c_void_p),
        ("start_load_f", StartLoad),
        ("get_load_f", GetLoad),
        ("free_load_f", FreeLoad),
        ("debugNoMaterialize", ctypes.c_int),
        ("granuleParallelism", ctypes.c_int),
    ]


Handle = ctypes.c_void_p
Bytes = ctypes.POINTER(ctypes.c_uint8)
Int = ctypes.c_int
Version = ctypes.c_int64
RangeArguments = (Handle, Bytes, Int, Bytes, Int)

FUNCTION_ARGUMENTS = {
    "fdb_future_get_granule_summary_array": (
        Handle,
        ctypes.POINTER(ctypes.POINTER(FDBGranuleSummary)),
        ctypes.POINTER(Int),
    ),
    "fdb_database_purge_blob_granules": RangeArguments + (Version, Int),
    "fdb_database_wait_purge_granules_complete": (Handle, Bytes, Int),
    "fdb_database_blobbify_range": RangeArguments,
    "fdb_database_blobbify_range_blocking": RangeArguments,
    "fdb_database_unblobbify_range": RangeArguments,
    "fdb_database_list_blobbified_ranges": RangeArguments + (Int,),
    "fdb_database_verify_blob_range": RangeArguments + (Version,),
    "fdb_database_flush_blob_range": RangeArguments + (Int, Version),
    "fdb_tenant_purge_blob_granules": RangeArguments + (Version, Int),
    "fdb_tenant_wait_purge_granules_complete": (Handle, Bytes, Int),
    "fdb_tenant_blobbify_range": RangeArguments,
    "fdb_tenant_blobbify_range_blocking": RangeArguments,
    "fdb_tenant_unblobbify_range": RangeArguments,
    "fdb_tenant_list_blobbified_ranges": RangeArguments + (Int,),
    "fdb_tenant_verify_blob_range": RangeArguments + (Version,),
    "fdb_tenant_flush_blob_range": RangeArguments + (Int, Version),
    "fdb_transaction_get_blob_granule_ranges": RangeArguments + (Int,),
    "fdb_transaction_read_blob_granules": RangeArguments
    + (Version, Version, FDBReadBlobGranuleContext),
    "fdb_transaction_summarize_blob_granules": RangeArguments + (Version, Int),
    "fdb_transaction_read_blob_granules_start": RangeArguments
    + (Version, Version, ctypes.POINTER(Version)),
    "fdb_transaction_read_blob_granules_finish": (
        Handle,
        Handle,
        Bytes,
        Int,
        Bytes,
        Int,
        Version,
        Version,
        ctypes.POINTER(FDBReadBlobGranuleContext),
    ),
}


def invoke(library_path, function_name):
    if os.name != "nt":
        import resource

        # Each invocation intentionally aborts; keep it from writing a core file.
        resource.setrlimit(resource.RLIMIT_CORE, (0, 0))

    library = ctypes.CDLL(str(library_path))
    select_api = library.fdb_select_api_version_impl
    select_api.argtypes = (Int, Int)
    select_api.restype = Int
    error = select_api(730, 730)
    if error:
        raise RuntimeError("API selection failed with error {}".format(error))

    function = getattr(library, function_name)
    argument_types = FUNCTION_ARGUMENTS[function_name]
    function.argtypes = argument_types
    function.restype = (
        Int if function_name == "fdb_future_get_granule_summary_array" else Handle
    )
    function(*(argument_type() for argument_type in argument_types))
    print("Removed function returned unexpectedly", file=sys.stderr)
    return 99


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--library", type=Path, required=True)
    parser.add_argument("--invoke", choices=FUNCTION_ARGUMENTS)
    args = parser.parse_args()
    library_path = args.library.resolve()

    if args.invoke:
        return invoke(library_path, args.invoke)

    library = ctypes.CDLL(str(library_path))
    missing = [name for name in FUNCTION_ARGUMENTS if not hasattr(library, name)]
    if missing:
        raise RuntimeError(
            "Missing blob granule exports: {}".format(", ".join(missing))
        )

    failures = []
    for function_name in FUNCTION_ARGUMENTS:
        try:
            result = subprocess.run(
                [
                    sys.executable,
                    str(Path(__file__).resolve()),
                    "--library",
                    str(library_path),
                    "--invoke",
                    function_name,
                ],
                capture_output=True,
                text=True,
                timeout=10,
            )
        except subprocess.TimeoutExpired:
            failures.append(
                "{} did not terminate within 10 seconds".format(function_name)
            )
            continue

        expected_message = (
            "FoundationDB blob granule function {} was removed in 8.0; aborting."
        ).format(function_name)
        expected_exit = (
            result.returncode not in (0, 99)
            if os.name == "nt"
            else result.returncode == -signal.SIGABRT
        )
        if not expected_exit or expected_message not in result.stderr:
            failures.append(
                "{}: return code {}, stderr: {}".format(
                    function_name, result.returncode, result.stderr.strip()
                )
            )

    if failures:
        raise RuntimeError("\n".join(failures))
    print("Verified {} fatal blob granule exports".format(len(FUNCTION_ARGUMENTS)))
    return 0


if __name__ == "__main__":
    sys.exit(main())
