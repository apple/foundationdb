if __name__ == "__main__":
    import re
    import sys

    r = re.compile("^DLLEXPORT[^(]*(fdb_[^(]*)[(].*$", re.MULTILINE)
    header_files = sys.argv[1:-1]
    symbols_file = sys.argv[-1]
    symbols = set()
    for header_file in header_files:
        with open(header_file, "r") as f:
            symbols.update("_" + m.group(1) for m in r.finditer(f.read()))
    symbols = sorted(symbols)
    with open(symbols_file, "w") as f:
        f.write("\n".join(symbols))
        f.write("\n")
