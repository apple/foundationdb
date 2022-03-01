from argparse import ArgumentParser
import glob
import io
import json
import os
import platform
import shlex
import subprocess
import tarfile
import tempfile


class JoshuaBuilder:
    def __init__(self, build_dir: str, src_dir: str):
        self.files = {}
        self.build_dir = build_dir
        self.src_dir = src_dir

    def add_arg(self, arg: str) -> str:
        """Infer files to add to the joshua package from a command line arg to a test"""
        if os.path.exists(arg) and arg.endswith(".py"):
            dirname = os.path.dirname(arg)
            for potential_dep in glob.glob("{}/*.py".format(dirname)):
                self._add_arg(potential_dep)
        if ".jar:" in arg:
            # Assume it's a classpath
            return ":".join(self._add_arg(jar) for jar in arg.split(":"))
        return self._add_arg(arg)

    def _add_arg(self, arg: str) -> str:
        if os.path.exists(arg):
            if not os.path.relpath(arg, self.build_dir).startswith(".."):
                relpath = "build/" + os.path.relpath(arg, self.build_dir)
                self.files[arg] = relpath
                return relpath
            elif not os.path.relpath(arg, self.src_dir).startswith(".."):
                relpath = "src/" + os.path.relpath(arg, self.src_dir)
                self.files[arg] = relpath
                return relpath
            elif os.access(arg, os.X_OK):
                # Hope it's on the path
                name = os.path.basename(arg)
                if name.startswith("python3"):
                    name = "python3"
                return name
            else:
                assert False, "Not sure what to do with {}".format(arg)
        return arg

    @staticmethod
    def _add_file(tar, file, arcfile):
        if "bin/" in arcfile or "lib" in arcfile:
            print("Stripping debug symbols and adding {} as {}".format(file, arcfile))
            with tempfile.NamedTemporaryFile() as tmp:
                subprocess.check_output(["strip", "-S", file, "-o", tmp.name])
                tar.add(tmp.name, arcfile)
        else:
            print("Adding {} as {}".format(file, arcfile))
            tar.add(file, arcfile)

    def write_tarball(self, output, joshua_test):
        with tarfile.open(output, "w:gz") as tar:
            for file, arcfile in self.files.items():
                if not os.path.isdir(file):
                    self._add_file(tar, file, arcfile)
            tarinfo = tarfile.TarInfo("joshua_test")
            tarinfo.mode = 0o755
            joshua_bytes = joshua_test.encode("utf-8")
            tarinfo.size = len(joshua_bytes)
            tar.addfile(tarinfo, io.BytesIO(joshua_bytes))


def get_ctest_json(build_dir, extra_args):
    return json.loads(
        subprocess.check_output(
            ["ctest", "-N", "--show-only=json-v1"] + extra_args, cwd=build_dir
        ).decode("utf-8")
    )


def main():
    parser = ArgumentParser(
        description="""
Convert fdb build directory and src directory to a joshua package that runs the ctest tests.
Unknown arguments are forwarded to ctest, so you may use -R to filter tests e.g."""
    )
    parser.add_argument(
        "--build-dir",
        metavar="BUILD_DIRECTORY",
        help="FDB build directory",
        required=True,
    )
    parser.add_argument(
        "--src-dir", metavar="SRC_DIRECTORY", help="FDB source directory", required=True
    )
    parser.add_argument(
        "--output",
        metavar="OUTPUT",
        help="Where to write the joshua package",
        required=True,
    )
    args, unknown_args = parser.parse_known_args()
    ctest_json = get_ctest_json(args.build_dir, unknown_args)
    joshua_builder = JoshuaBuilder(args.build_dir, args.src_dir)
    commands = []
    for test in ctest_json["tests"]:
        command = test.get("command")
        if command is not None:
            commands.append(
                " ".join(shlex.quote(joshua_builder.add_arg(arg)) for arg in command)
            )
            print("Found test: {}".format(commands[-1]))
    joshua_builder.add_arg(os.path.join(args.build_dir, "bin/fdbbackup"))
    joshua_builder.add_arg(os.path.join(args.build_dir, "bin/fdbcli"))
    joshua_builder.add_arg(os.path.join(args.build_dir, "bin/fdbmonitor"))
    joshua_builder.add_arg(os.path.join(args.build_dir, "bin/fdbserver"))
    if platform.system() == "Darwin":
        joshua_builder.add_arg(os.path.join(args.build_dir, "lib/libfdb_c.dylib"))
    else:
        joshua_builder.add_arg(os.path.join(args.build_dir, "lib/libfdb_c.so"))

    joshua_test = '#!/bin/bash\nexport BASH_XTRACEFD=1\nset -euxo pipefail\nexport {library_path}=build/lib:"${library_path}"\n'.format(
        library_path="DYLD_LIBRARY_PATH"
        if platform.system() == "Darwin"
        else "LD_LIBRARY_PATH"
    )

    joshua_builder.write_tarball(
        args.output,
        joshua_test + "\n".join(command + " 2>&1" for command in commands),
    )


if __name__ == "__main__":
    main()
