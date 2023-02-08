#!/usr/bin/env python3

from argparse import ArgumentParser, RawDescriptionHelpFormatter


def CreateTmpFdbClusterArgParser(description):
    parser = ArgumentParser(formatter_class=RawDescriptionHelpFormatter,
                            description=description)
    parser.add_argument(
        "--build-dir",
        "-b",
        metavar="BUILD_DIRECTORY",
        help="FDB build directory",
        required=True,
    )

    parser.add_argument(
        "--process-number",
        "-p",
        help="Number of fdb processes running",
        type=int,
        default=1,
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
    parser.add_argument(
        "--authorization-kty",
        help="Public/Private key pair type to be used in signing and verifying authorization tokens. Must be either unset (empty string), EC or RSA. Unset argument (default) disables authorization",
        type=str,
        choices=["", "EC", "RSA"],
        default="",
    )
    parser.add_argument(
        "--authorization-keypair-id",
        help="Name of the public/private key pair to be used in signing and verifying authorization tokens. Setting this argument takes effect only with authorization enabled.",
        type=str,
        default="",
    )
    parser.add_argument(
        "--no-remove-at-exit",
        help="whether to remove the cluster directory upon exit",
        action="store_true",
        default=False,
    )
    return parser
