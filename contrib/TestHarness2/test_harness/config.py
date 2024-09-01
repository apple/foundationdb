from __future__ import annotations

import argparse
import collections
import copy
import os
import random
from enum import Enum
from pathlib import Path
from typing import List, Any, OrderedDict, Dict


class BuggifyOptionValue(Enum):
    ON = 1
    OFF = 2
    RANDOM = 3


class BuggifyOption:
    def __init__(self, val: str | None = None):
        self.value = BuggifyOptionValue.RANDOM
        if val is not None:
            v = val.lower()
            if v in ["on", "1", "true"]:
                self.value = BuggifyOptionValue.ON
            elif v in ["off", "0", "false"]:
                self.value = BuggifyOptionValue.OFF
            elif v in ["random", "rnd", "r"]:
                pass
            else:
                assert False, "Invalid value {} -- use true, false, or random".format(v)


class ConfigValue:
    def __init__(self, name: str, **kwargs):
        self.name = name
        self.value = None
        self.kwargs = kwargs
        if "default" in self.kwargs:
            self.value = self.kwargs["default"]

    def get_arg_name(self) -> str:
        if "long_name" in self.kwargs:
            return self.kwargs["long_name"]
        else:
            return self.name

    def add_to_args(self, parser: argparse.ArgumentParser):
        kwargs = copy.copy(self.kwargs)
        long_name = self.name
        short_name = None
        if "long_name" in kwargs:
            long_name = kwargs["long_name"]
            del kwargs["long_name"]
        if "short_name" in kwargs:
            short_name = kwargs["short_name"]
            del kwargs["short_name"]
        if "action" in kwargs and kwargs["action"] in ["store_true", "store_false"]:
            del kwargs["type"]
        long_name = long_name.replace("_", "-")
        if short_name is None:
            # line below is useful for debugging
            # print('add_argument(\'--{}\', [{{{}}}])'.format(long_name, ', '.join(['\'{}\': \'{}\''.format(k, v)
            #                                                                       for k, v in kwargs.items()])))
            parser.add_argument("--{}".format(long_name), **kwargs)
        else:
            # line below is useful for debugging
            # print('add_argument(\'-{}\', \'--{}\', [{{{}}}])'.format(short_name, long_name,
            #                                                          ', '.join(['\'{}\': \'{}\''.format(k, v)
            #                                                                     for k, v in kwargs.items()])))
            parser.add_argument(
                "-{}".format(short_name), "--{}".format(long_name), **kwargs
            )

    def get_value(self, args: argparse.Namespace) -> tuple[str, Any]:
        return self.name, args.__getattribute__(self.get_arg_name())


class Config:
    """
    This is the central configuration class for test harness. The values in this class are exposed globally through
    a global variable test_harness.config.config. This class provides some "magic" to keep test harness flexible.
    Each parameter can further be configured using an `_args` member variable which is expected to be a dictionary.
    * The value of any variable can be set through the command line. For a variable named `variable_name` we will
      by default create a new command line option `--variable-name` (`_` is automatically changed to `-`). This
      default can be changed by setting the `'long_name'` property in the `_arg` dict.
    * In addition the user can also optionally set a short-name. This can be achieved by setting the `'short_name'`
      property in the `_arg` dictionary.
    * All additional properties in `_args` are passed to `argparse.add_argument`.
    * If the default of a variable is `None` the user should explicitly set the `'type'` property to an appropriate
      type.
    * In addition to command line flags, all configuration options can also be controlled through environment variables.
      By default, `variable-name` can be changed by setting the environment variable `TH_VARIABLE_NAME`. This default
      can be changed by setting the `'env_name'` property.
    * Test harness comes with multiple executables. Each of these should use the config facility. For this,
      `Config.build_arguments` should be called first with the `argparse` parser. Then `Config.extract_args` needs
      to be called with the result of `argparse.ArgumentParser.parse_args`. A sample example could look like this:
      ```
      parser = argparse.ArgumentParser('TestHarness', formatter_class=argparse.ArgumentDefaultsHelpFormatter)
      config.build_arguments(parser)
      args = parser.parse_args()
      config.extract_args(args)
      ```
    * Changing the default value for all executables might not always be desirable. If it should be only changed for
      one executable Config.change_default should be used.
    """

    def __init__(self):
        self.random = random.Random()
        self.cluster_file: str | None = None
        self.cluster_file_args = {
            "short_name": "C",
            "type": str,
            "help": "Path to fdb cluster file",
            "required": False,
            "env_name": "JOSHUA_CLUSTER_FILE",
        }
        self.joshua_dir: str | None = None
        self.joshua_dir_args = {
            "type": str,
            "help": "Where to write FDB data to",
            "required": False,
            "env_name": "JOSHUA_APP_DIR",
        }
        self.stats: str | None = None
        self.stats_args = {
            "type": str,
            "help": "A base64 encoded list of statistics (used to reproduce runs)",
            "required": False,
        }
        self.random_seed: int | None = None
        self.random_seed_args = {
            "type": int,
            "help": "Force given seed given to fdbserver -- mostly useful for debugging",
            "required": False,
        }
        self.kill_seconds: int = 30 * 60
        self.kill_seconds_args = {"help": "Timeout for individual test"}
        self.buggify_on_ratio: float = 0.8
        self.buggify_on_ratio_args = {"help": "Probability that buggify is turned on"}
        self.write_run_times = False
        self.write_run_times_args = {
            "help": "Write back probabilities after each test run",
            "action": "store_true",
        }
        self.unseed_check_ratio: float = 0.05
        self.unseed_check_ratio_args = {
            "help": "Probability for doing determinism check"
        }
        self.test_dirs: List[str] = ["slow", "fast", "restarting", "rare", "noSim"]
        self.test_dirs_args: dict = {
            "nargs": "*",
            "help": "test_directories to look for files in",
        }
        self.trace_format: str = "json"
        self.trace_format_args = {
            "choices": ["json", "xml"],
            "help": "What format fdb should produce",
        }
        self.crash_on_error: bool = True
        self.crash_on_error_args = {
            "long_name": "no_crash",
            "action": "store_false",
            "help": "Don't crash on first error",
        }
        self.max_warnings: int = 10
        self.max_warnings_args = {"short_name": "W"}
        self.max_errors: int = 10
        self.max_errors_args = {"short_name": "E"}
        self.old_binaries_path: Path = Path("/app/deploy/global_data/oldBinaries/")
        self.old_binaries_path_args = {
            "help": "Path to the directory containing the old fdb binaries"
        }
        self.tls_plugin_path: Path = Path("/app/deploy/runtime/.tls_5_1/FDBLibTLS.so")
        self.tls_plugin_path_args = {
            "help": "Path to the tls plugin used for binaries < 5.2.0"
        }
        self.disable_kaio: bool = False
        self.use_valgrind: bool = False
        self.use_valgrind_args = {"action": "store_true"}
        self.buggify = BuggifyOption("random")
        self.buggify_args = {"short_name": "b", "choices": ["on", "off", "random"]}
        self.pretty_print: bool = False
        self.pretty_print_args = {"short_name": "P", "action": "store_true"}
        self.clean_up: bool = True
        self.clean_up_args = {"long_name": "no_clean_up", "action": "store_false"}
        self.run_dir: Path = Path("tmp")
        self.joshua_seed: int = random.randint(0, 2**32 - 1)
        self.joshua_seed_args = {
            "short_name": "s",
            "help": "A random seed",
            "env_name": "JOSHUA_SEED",
        }
        self.print_coverage = False
        self.print_coverage_args = {"action": "store_true"}
        self.binary = Path("bin") / (
            "fdbserver.exe" if os.name == "nt" else "fdbserver"
        )
        self.binary_args = {"help": "Path to executable"}
        self.hit_per_runs_ratio: int = 20000
        self.hit_per_runs_ratio_args = {
            "help": "Maximum test runs before each code probe hit at least once"
        }
        self.output_format: str = "xml"
        self.output_format_args = {
            "short_name": "O",
            "choices": ["json", "xml"],
            "help": "What format TestHarness should produce",
        }
        self.include_test_files: str = r".*"
        self.include_test_files_args = {
            "help": "Only consider test files whose path match against the given regex"
        }
        self.exclude_test_files: str = r".^"
        self.exclude_test_files_args = {
            "help": "Don't consider test files whose path match against the given regex"
        }
        self.include_test_classes: str = r".*"
        self.include_test_classes_args = {
            "help": "Only consider tests whose names match against the given regex"
        }
        self.exclude_test_names: str = r".^"
        self.exclude_test_names_args = {
            "help": "Don't consider tests whose names match against the given regex"
        }
        self.details: bool = False
        self.details_args = {
            "help": "Print detailed results",
            "short_name": "c",
            "action": "store_true",
        }
        self.success: bool = False
        self.success_args = {"help": "Print successful results", "action": "store_true"}
        self.cov_include_files: str = r".*"
        self.cov_include_files_args = {
            "help": "Only consider coverage traces that originated in files matching regex"
        }
        self.cov_exclude_files: str = r".^"
        self.cov_exclude_files_args = {
            "help": "Ignore coverage traces that originated in files matching regex"
        }
        self.max_stderr_bytes: int = 10000
        self.write_stats: bool = True
        self.read_stats: bool = True
        self.reproduce_prefix: str | None = None
        self.reproduce_prefix_args = {
            "type": str,
            "required": False,
            "help": "When printing the results, prepend this string to the command",
        }
        self.long_running: bool = False
        self.long_running_args = {"action": "store_true"}
        self._env_names: Dict[str, str] = {}
        self._config_map = self._build_map()
        self._read_env()
        self.random.seed(self.joshua_seed, version=2)

    def change_default(self, attr: str, default_val):
        assert attr in self._config_map, "Unknown config attribute {}".format(attr)
        self.__setattr__(attr, default_val)
        self._config_map[attr].kwargs["default"] = default_val

    def _get_env_name(self, var_name: str) -> str:
        return self._env_names.get(var_name, "TH_{}".format(var_name.upper()))

    def dump(self):
        for attr in dir(self):
            obj = getattr(self, attr)
            if (
                attr == "random"
                or attr.startswith("_")
                or callable(obj)
                or attr.endswith("_args")
            ):
                continue
            print("config.{}: {} = {}".format(attr, type(obj), obj))

    def _build_map(self) -> OrderedDict[str, ConfigValue]:
        config_map: OrderedDict[str, ConfigValue] = collections.OrderedDict()
        for attr in dir(self):
            obj = getattr(self, attr)
            if attr == "random" or attr.startswith("_") or callable(obj):
                continue
            if attr.endswith("_args"):
                name = attr[0 : -len("_args")]
                assert name in config_map
                assert isinstance(obj, dict)
                for k, v in obj.items():
                    if k == "env_name":
                        self._env_names[name] = v
                    else:
                        config_map[name].kwargs[k] = v
            else:
                # attribute_args has to be declared after the attribute
                assert attr not in config_map
                val_type = type(obj)
                kwargs = {"type": val_type, "default": obj}
                config_map[attr] = ConfigValue(attr, **kwargs)
        return config_map

    def _read_env(self):
        for attr in dir(self):
            obj = getattr(self, attr)
            if (
                attr == "random"
                or attr.startswith("_")
                or attr.endswith("_args")
                or callable(obj)
            ):
                continue
            env_name = self._get_env_name(attr)
            attr_type = self._config_map[attr].kwargs["type"]
            assert type(None) != attr_type
            e = os.getenv(env_name)
            if e is not None:
                # Use the env var to supply the default value, so that if the
                # environment variable is set and the corresponding command line
                # flag is not, the environment variable has an effect.
                self._config_map[attr].kwargs["default"] = attr_type(e)

    def build_arguments(self, parser: argparse.ArgumentParser):
        for val in self._config_map.values():
            val.add_to_args(parser)

    def extract_args(self, args: argparse.Namespace):
        for val in self._config_map.values():
            k, v = val.get_value(args)
            if v is not None:
                config.__setattr__(k, v)
        self.random.seed(self.joshua_seed, version=2)


config = Config()

if __name__ == "__main__":
    # test the config setup
    parser = argparse.ArgumentParser(
        "TestHarness Config Tester",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    config.build_arguments(parser)
    args = parser.parse_args()
    config.extract_args(args)
    config.dump()
