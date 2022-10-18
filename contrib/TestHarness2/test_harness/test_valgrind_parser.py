import sys

from test_harness.valgrind import parse_valgrind_output
from pathlib import Path


if __name__ == '__main__':
    errors = parse_valgrind_output(Path(sys.argv[1]))
    for valgrind_error in errors:
        print('ValgrindError: what={}, kind={}'.format(valgrind_error.what.what, valgrind_error.kind))
        print('Backtrace: {}'.format(valgrind_error.what.backtrace))
        counter = 0
        for aux in valgrind_error.aux:
            print('Aux {}:'.format(counter))
            print('  What: {}'.format(aux.what))
            print('  Backtrace: {}'.format(aux.backtrace))
