from __future__ import annotations

import argparse
import collections
import copy
import os
import random
from enum import Enum
from pathlib import Path
from typing import List, Any, OrderedDict


class BuggifyOptionValue(Enum):
    ON = 1
    OFF = 2
    RANDOM = 3


class BuggifyOption:
    def __init__(self, val: str | None = None):
        self.value = BuggifyOptionValue.RANDOM
        if val is not None:
            v = val.lower()
            if v == 'on' or v == '1' or v == 'true':
                self.value = BuggifyOptionValue.ON
            elif v == 'off' or v == '0' or v == 'false':
                self.value = BuggifyOptionValue.OFF


class ConfigValue:
    def __init__(self, name: str, **kwargs):
        self.name = name
        self.value = None
        self.kwargs = kwargs
        if 'default' in self.kwargs:
            self.value = self.kwargs['default']

    def get_arg_name(self) -> str:
        if 'long_name' in self.kwargs:
            return self.kwargs['long_name']
        else:
            return self.name

    def add_to_args(self, parser: argparse.ArgumentParser):
        kwargs = copy.copy(self.kwargs)
        long_name = self.name
        short_name = None
        if 'long_name' in kwargs:
            long_name = kwargs['long_name']
            del kwargs['long_name']
        if 'short_name' in kwargs:
            short_name = kwargs['short_name']
            del kwargs['short_name']
        long_name = long_name.replace('_', '-')
        if short_name is None:
            parser.add_argument('--{}'.format(long_name), **kwargs)
        else:
            parser.add_argument('-{}'.format(short_name), '--{}'.format(long_name), **kwargs)

    def get_value(self, args: argparse.Namespace) -> tuple[str, Any]:
        return self.name, args.__getattribute__(self.get_arg_name())


configuration: List[ConfigValue] = [
    ConfigValue('kill_seconds', default=60 * 30, help='Timeout for individual test', type=int),
    ConfigValue('buggify_on_ratio', default=0.8, help='Probability that buggify is turned on', type=float),
    ConfigValue('write_run_times', default=False, help='Write back probabilities after each test run',
                action='store_true'),
    ConfigValue('unseed_check_ratio', default=0.05, help='Probability for doing determinism check', type=float),
    ConfigValue('test_dirs', default=['slow', 'fast', 'restarting', 'rare', 'noSim'], nargs='*'),
    ConfigValue('trace_format', default='json', choices=['json', 'xml']),
    ConfigValue('crash_on_error', long_name='no_crash', default=True, action='store_false'),
    ConfigValue('max_warnings', default=10, short_name='W', type=int),
    ConfigValue('max_errors', default=10, short_name='E', type=int),
    ConfigValue('old_binaries_path', default=Path('/opt/joshua/global_data/oldBinaries'), type=Path),
    ConfigValue('use_valgrind', default=False, action='store_true'),
    ConfigValue('buggify', short_name='b', default=BuggifyOption('random'), type=BuggifyOption,
                choices=['on', 'off', 'random']),
    ConfigValue('pretty_print', short_name='P', default=False, action='store_true'),
    ConfigValue('clean_up', default=True),
    ConfigValue('run_dir', default=Path('tmp'), type=Path),
    ConfigValue('joshua_seed', default=int(os.getenv('JOSHUA_SEED', str(random.randint(0, 2 ** 32 - 1)))), type=int),
    ConfigValue('print_coverage', default=False, action='store_true'),
    ConfigValue('binary', default=Path('bin') / ('fdbserver.exe' if os.name == 'nt' else 'fdbserver'),
                help='Path to executable', type=Path),
    ConfigValue('output_format', short_name='O', type=str, choices=['json', 'xml'], default='xml'),
]


class Config:
    def __init__(self):
        self.kill_seconds: int = 30 * 60
        self.kill_seconds_args = {'help': 'Timeout for individual test'}
        self.buggify_on_ratio: float = 0.8
        self.buggify_on_ratio_args = {'help': 'Probability that buggify is turned on'}
        self.write_run_times = False
        self.write_run_times_args = {'help': 'Write back probabilities after each test run',
                                     'action': 'store_true'}
        self.unseed_check_ratio: float = 0.05
        self.unseed_check_ratio_args = {'help': 'Probability for doing determinism check'}
        self.test_dirs: List[str] = ['slow', 'fast', 'restarting', 'rare', 'noSim']
        self.test_dirs_args: dict = {'nargs': '*'}
        self.trace_format: str = 'json'
        self.trace_format_args = {'choices': ['json', 'xml']}
        self.crash_on_error: bool = True
        self.crash_on_error_args = {'long_name': 'no_crash', 'action': 'store_false'}
        self.max_warnings: int = 10
        self.max_warnings_args = {'short_name': 'W'}
        self.max_errors: int = 10
        self.max_errors_args = {'short_name': 'E'}
        self.old_binaries_path: Path = Path('/opt/joshua/global_data/oldBinaries')
        self.old_binaries_path = {'help': 'Path to the directory containing the old fdb binaries'}
        self.use_valgrind: bool = False
        self.use_valgrind_args = {'action': 'store_true'}
        self.buggify = BuggifyOption('random')
        self.buggify_args = {'short_name': 'b', 'choices': ['on', 'off', 'random']}
        self.pretty_print: bool = False
        self.pretty_print_args = {'short_name': 'P', 'action': 'store_true'}
        self.clean_up = True
        self.clean_up_args = {'action': 'store_false'}
        self.run_dir: Path = Path('tmp')
        self.joshua_seed: int = int(os.getenv('JOSHUA_SEED', str(random.randint(0, 2 ** 32 - 1))))
        self.joshua_seed_args = {'short_name': 's', 'help': 'A random seed'}
        self.print_coverage = False
        self.print_coverage_args = {'action': 'store_true'}
        self.binary = Path('bin') / ('fdbserver.exe' if os.name == 'nt' else 'fdbserver')
        self.binary_args = {'help': 'Path to executable'}
        self.output_format: str = 'xml'
        self.output_format_args = {'short_name': 'O', 'choices': ['json', 'xml']}
        self.config_map = self._build_map()

    def _build_map(self):
        config_map: OrderedDict[str, ConfigValue] = collections.OrderedDict()
        for attr in dir(self):
            obj = getattr(self, attr)
            if attr.startswith('_') or callable(obj):
                continue
            if attr.endswith('_args'):
                name = attr[0:-len('_args')]
                assert name in config_map
                print('assert isinstance({}, dict) type={}, obj={}'.format(attr, type(obj), obj))
                assert isinstance(obj, dict)
                for k, v in obj.items():
                    config_map[name].kwargs[k] = v
            else:
                kwargs = {'type': type(obj), 'default': obj}
                config_map[attr] = ConfigValue(attr, **kwargs)
        return config_map

    def build_arguments(self, parser: argparse.ArgumentParser):
        for val in self.config_map.values():
            val.add_to_args(parser)

    def extract_args(self, args: argparse.Namespace):
        for val in self.config_map.values():
            k, v = val.get_value(args)
            config.__setattr__(k, v)


config = Config()
