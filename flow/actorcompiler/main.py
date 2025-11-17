from __future__ import annotations

import argparse
import os
import stat
import sys
from pathlib import Path

from . import ActorCompilerError
from .actor_parser import ActorParser, ErrorMessagePolicy


def overwrite_by_move(target: Path, temporary: Path) -> None:
    if target.exists():
        target.chmod(stat.S_IWUSR | stat.S_IRUSR | stat.S_IRGRP | stat.S_IROTH)
        target.unlink()
    os.replace(temporary, target)
    target.chmod(stat.S_IRUSR | stat.S_IRGRP | stat.S_IROTH)


def parse_arguments(argv: list[str]) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        prog="actorcompiler",
        description="Python port of the Flow actor compiler",
        add_help=False,
        usage="actorcompiler <input> <output> [--disable-diagnostics] [--generate-probes]",
    )
    parser.add_argument("input", nargs="?")
    parser.add_argument("output", nargs="?")
    parser.add_argument("--disable-diagnostics", action="store_true")
    parser.add_argument("--generate-probes", action="store_true")
    parser.add_argument("--help", action="help", help=argparse.SUPPRESS)
    args = parser.parse_args(argv)
    if not args.input or not args.output:
        parser.print_usage(sys.stderr)
        sys.exit(100)
    return args


def main(argv: list[str] | None = None) -> int:
    if argv is None:
        argv = sys.argv[1:]
    args = parse_arguments(argv)
    input_path = Path(args.input)
    output_path = Path(args.output)
    output_tmp = output_path.with_suffix(output_path.suffix + ".tmp")
    output_uid = output_path.with_suffix(output_path.suffix + ".uid")

    policy = ErrorMessagePolicy()
    policy.DisableDiagnostics = args.disable_diagnostics

    try:
        print("actorcompiler", " ".join(sys.argv[1:]))
        text = input_path.read_text()
        parser = ActorParser(
            text,
            str(input_path).replace("\\", "/"),
            policy,
            args.generate_probes,
        )

        with output_tmp.open("w", newline="\n") as out_file:
            parser.Write(out_file, str(output_path).replace("\\", "/"))
        overwrite_by_move(output_path, output_tmp)

        with output_tmp.open("w", newline="\n") as uid_file:
            for (hi, lo), value in parser.uidObjects.items():
                uid_file.write(f"{hi}|{lo}|{value}\n")
        overwrite_by_move(output_uid, output_tmp)

        return 0
    except ActorCompilerError as exc:
        print(
            f"{input_path}({exc.source_line}): error FAC1000: {exc}",
            file=sys.stderr,
        )
        if output_tmp.exists():
            output_tmp.unlink()
        if output_path.exists():
            output_path.chmod(stat.S_IWUSR | stat.S_IRUSR)
            output_path.unlink()
        return 1
    except Exception as exc:  # pylint: disable=broad-except
        print(
            f"{input_path}(1): error FAC2000: Internal {exc}",
            file=sys.stderr,
        )
        if output_tmp.exists():
            output_tmp.unlink()
        if output_path.exists():
            output_path.chmod(stat.S_IWUSR | stat.S_IRUSR)
            output_path.unlink()
        return 3


if __name__ == "__main__":
    sys.exit(main())
