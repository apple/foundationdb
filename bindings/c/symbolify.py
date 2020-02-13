if __name__ == '__main__':
    import re
    import sys
    r = re.compile('DLLEXPORT[^(]*(fdb_[^(]*)[(]')
    (fdb_c_h, symbols_file) = sys.argv[1:]
    with open(fdb_c_h, 'r') as f:
        symbols = sorted(set('_' + m.group(1) for m in r.finditer(f.read())))
    with open(symbols_file, 'w') as f:
        print('\n'.join(symbols), file=f)
