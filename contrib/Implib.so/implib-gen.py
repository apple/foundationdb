#!/usr/bin/python3

# Copyright 2017-2022 Yury Gribov
#
# The MIT License (MIT)
#
# Use of this source code is governed by MIT license that can be
# found in the LICENSE.txt file.

"""
Generates static import library for POSIX shared library
"""

import sys
import os.path
import re
import subprocess
import argparse
import string
import configparser

me = os.path.basename(__file__)
root = os.path.dirname(__file__)


def warn(msg):
    """Emits a nicely-decorated warning."""
    sys.stderr.write(f"{me}: warning: {msg}\n")


def error(msg):
    """Emits a nicely-decorated error and exits."""
    sys.stderr.write(f"{me}: error: {msg}\n")
    sys.exit(1)


def run(args, stdin=""):
    """Runs external program and aborts on error."""
    env = os.environ.copy()
    # Force English language
    env["LC_ALL"] = "c"
    try:
        del env["LANG"]
    except KeyError:
        pass
    with subprocess.Popen(args, stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.PIPE, env=env) as p:
        out, err = p.communicate(input=stdin.encode("utf-8"))
    out = out.decode("utf-8")
    err = err.decode("utf-8")
    if p.returncode != 0 or err:
        error(f"{args[0]} failed with retcode {p.returncode}:\n{err}")
    return out, err


def make_toc(words, renames=None):
    "Make an mapping of words to their indices in list"
    renames = renames or {}
    toc = {}
    for i, n in enumerate(words):
        name = renames.get(n, n)
        toc[i] = name
    return toc


def parse_row(words, toc, hex_keys):
    "Make a mapping from column names to values"
    vals = {k: (words[i] if i < len(words) else "") for i, k in toc.items()}
    for k in hex_keys:
        if vals[k]:
            vals[k] = int(vals[k], 16)
    return vals


def collect_syms(f):
    """Collect ELF dynamic symtab."""

    # --dyn-syms does not always work for some reason so dump all symtabs
    out, _ = run(["readelf", "-sW", f])

    toc = None
    syms = []
    syms_set = set()
    for line in out.splitlines():
        line = line.strip()
        if not line:
            # Next symtab
            toc = None
            continue
        words = re.split(r" +", line)
        if line.startswith("Num"):  # Header?
            if toc is not None:
                error("multiple headers in output of readelf")
            # Colons are different across readelf versions so get rid of them.
            toc = make_toc(map(lambda n: n.replace(":", ""), words))
        elif toc is not None:
            sym = parse_row(words, toc, ["Value"])
            name = sym["Name"]
            if name in syms_set:
                continue
            syms_set.add(name)
            sym["Size"] = int(sym["Size"], 0)  # Readelf is inconistent on Size format
            if "@" in name:
                sym["Default"] = "@@" in name
                name, ver = re.split(r"@+", name)
                sym["Name"] = name
                sym["Version"] = ver
            else:
                sym["Default"] = True
                sym["Version"] = None
            syms.append(sym)

    if toc is None:
        error(f"failed to analyze symbols in {f}")

    # Also collected demangled names
    if syms:
        out, _ = run(["c++filt"], "\n".join((sym["Name"] for sym in syms)))
        for i, name in enumerate(out.split("\n")):
            syms[i]["Demangled Name"] = name

    return syms


def collect_relocs(f):
    """Collect ELF dynamic relocs."""

    out, _ = run(["readelf", "-rW", f])

    toc = None
    rels = []
    for line in out.splitlines():
        line = line.strip()
        if not line:
            toc = None
            continue
        if line == "There are no relocations in this file.":
            return []
        if re.match(r"^\s*Offset", line):  # Header?
            if toc is not None:
                error("multiple headers in output of readelf")
            words = re.split(r"\s\s+", line)  # "Symbol's Name + Addend"
            toc = make_toc(words)
        elif toc is not None:
            line = re.sub(r" \+ ", "+", line)
            words = re.split(r"\s+", line)
            rel = parse_row(words, toc, ["Offset", "Info"])
            rels.append(rel)
            # Split symbolic representation
            sym_name = "Symbol's Name + Addend"
            if sym_name not in rel and "Symbol's Name" in rel:
                # Adapt to different versions of readelf
                rel[sym_name] = rel["Symbol's Name"] + "+0"
            if rel[sym_name]:
                p = rel[sym_name].split("+")
                if len(p) == 1:
                    p = ["", p[0]]
                rel[sym_name] = (p[0], int(p[1], 16))

    if toc is None:
        error(f"failed to analyze relocations in {f}")

    return rels


def collect_sections(f):
    """Collect section info from ELF."""

    out, _ = run(["readelf", "-SW", f])

    toc = None
    sections = []
    for line in out.splitlines():
        line = line.strip()
        if not line:
            continue
        line = re.sub(r"\[\s+", "[", line)
        words = re.split(r" +", line)
        if line.startswith("[Nr]"):  # Header?
            if toc is not None:
                error("multiple headers in output of readelf")
            toc = make_toc(words, {"Addr": "Address"})
        elif line.startswith("[") and toc is not None:
            sec = parse_row(words, toc, ["Address", "Off", "Size"])
            if "A" in sec["Flg"]:  # Allocatable section?
                sections.append(sec)

    if toc is None:
        error(f"failed to analyze sections in {f}")

    return sections


def read_unrelocated_data(input_name, syms, secs):
    """Collect unrelocated data from ELF."""
    data = {}
    with open(input_name, "rb") as f:

        def is_symbol_in_section(sym, sec):
            sec_end = sec["Address"] + sec["Size"]
            is_start_in_section = sec["Address"] <= sym["Value"] < sec_end
            is_end_in_section = sym["Value"] + sym["Size"] <= sec_end
            return is_start_in_section and is_end_in_section

        for name, s in sorted(syms.items(), key=lambda s: s[1]["Value"]):
            # TODO: binary search (bisect)
            sec = [sec for sec in secs if is_symbol_in_section(s, sec)]
            if len(sec) != 1:
                error(f"failed to locate section for interval [{s['Value']:x}, {s['Value'] + s['Size']:x})")
            sec = sec[0]
            f.seek(sec["Off"])
            data[name] = f.read(s["Size"])
    return data


def collect_relocated_data(syms, bites, rels, ptr_size, reloc_types):
    """Identify relocations for each symbol"""
    data = {}
    for name, s in sorted(syms.items()):
        b = bites.get(name)
        assert b is not None
        if s["Demangled Name"].startswith("typeinfo name"):
            data[name] = [("byte", int(x)) for x in b]
            continue
        data[name] = []
        for i in range(0, len(b), ptr_size):
            val = int.from_bytes(b[i * ptr_size : (i + 1) * ptr_size], byteorder="little")
            data[name].append(("offset", val))
        start = s["Value"]
        finish = start + s["Size"]
        # TODO: binary search (bisect)
        for rel in rels:
            if rel["Type"] in reloc_types and start <= rel["Offset"] < finish:
                i = (rel["Offset"] - start) // ptr_size
                assert i < len(data[name])
                data[name][i] = "reloc", rel
    return data


def generate_vtables(cls_tables, cls_syms, cls_data):
    """Generate code for vtables"""
    c_types = {"reloc": "const void *", "byte": "unsigned char", "offset": "size_t"}

    ss = []
    ss.append(
        """\
#ifdef __cplusplus
extern "C" {
#endif

"""
    )

    # Print externs

    printed = set()
    for name, data in sorted(cls_data.items()):
        for typ, val in data:
            if typ != "reloc":
                continue
            sym_name, addend = val["Symbol's Name + Addend"]
            sym_name = re.sub(r"@.*", "", sym_name)  # Can we pin version in C?
            if sym_name not in cls_syms and sym_name not in printed:
                ss.append(
                    f"""\
extern const char {sym_name}[];

"""
                )

    # Collect variable infos

    code_info = {}

    for name, s in sorted(cls_syms.items()):
        data = cls_data[name]
        if s["Demangled Name"].startswith("typeinfo name"):
            declarator = "const unsigned char %s[]"
        else:
            field_types = (f"{c_types[typ]} field_{i};" for i, (typ, _) in enumerate(data))
            declarator = "const struct { %s } %%s" % " ".join(
                field_types
            )  # pylint: disable=C0209  # consider-using-f-string
        vals = []
        for typ, val in data:
            if typ != "reloc":
                vals.append(str(val) + "UL")
            else:
                sym_name, addend = val["Symbol's Name + Addend"]
                sym_name = re.sub(r"@.*", "", sym_name)  # Can we pin version in C?
                vals.append(f"(const char *)&{sym_name} + {addend}")
        code_info[name] = (declarator, "{ %s }" % ", ".join(vals))  # pylint: disable= C0209  # consider-using-f-string

    # Print declarations

    for name, (decl, _) in sorted(code_info.items()):
        type_name = name + "_type"
        type_decl = decl % type_name
        ss.append(
            f"""\
typedef {type_decl};
extern __attribute__((weak)) {type_name} {name};
"""
        )

    # Print definitions

    for name, (_, init) in sorted(code_info.items()):
        type_name = name + "_type"
        ss.append(
            f"""\
const {type_name} {name} = {init};
"""
        )

    ss.append(
        """\
#ifdef __cplusplus
}  // extern "C"
#endif
"""
    )

    return "".join(ss)


def main():
    """Driver function"""
    parser = argparse.ArgumentParser(
        description="Generate wrappers for shared library functions.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=f"""\
Examples:
  $ python3 {me} /usr/lib/x86_64-linux-gnu/libaccountsservice.so.0
  Generating libaccountsservice.so.0.tramp.S...
  Generating libaccountsservice.so.0.init.cpp...
""",
    )

    parser.add_argument("library", metavar="LIB", help="Library to be wrapped.")
    parser.add_argument("--verbose", "-v", help="Print diagnostic info", action="count", default=0)
    parser.add_argument(
        "--dlopen-callback", help="Call user-provided custom callback to load library instead of dlopen", default=""
    )
    parser.add_argument("--library-load-name", help="Use custom name for dlopened library (default is LIB)")
    parser.add_argument(
        "--vtables", help="Intercept virtual tables (EXPERIMENTAL)", dest="vtables", action="store_true", default=False
    )
    parser.add_argument(
        "--no-vtables", help="Do not intercept virtual tables (default)", dest="vtables", action="store_false"
    )
    parser.add_argument(
        "--target",
        help="Target platform triple e.g. x86_64-unknown-linux-gnu or arm-none-eabi "
        "(atm x86_64, i[0-9]86, arm/armhf/armeabi, aarch64/armv8 "
        "and e2k are supported)",
        default=os.uname()[-1],
    )
    parser.add_argument(
        "--symbol-list", help="Path to file with symbols that should be present in wrapper " "(all by default)"
    )
    parser.add_argument("--symbol-prefix", metavar="PFX", help="Prefix wrapper symbols with PFX", default="")
    parser.add_argument("-q", "--quiet", help="Do not print progress info", action="store_true")
    parser.add_argument("--outdir", "-o", help="Path to create wrapper at", default="./")

    args = parser.parse_args()

    input_name = args.library
    verbose = args.verbose
    dlopen_callback = args.dlopen_callback
    load_name = args.library_load_name or os.path.basename(input_name)
    if args.target.startswith("arm"):
        target = "arm"  # Handle armhf-..., armel-...
    elif re.match(r"^i[0-9]86", args.target):
        target = "i386"
    else:
        target = args.target.split("-")[0]
    quiet = args.quiet
    outdir = args.outdir

    if args.symbol_list is None:
        funs = None
    else:
        with open(args.symbol_list, "r") as f:
            funs = []
            for line in re.split(r"\r?\n", f.read()):
                line = re.sub(r"#.*", "", line)
                line = line.strip()
                if line:
                    funs.append(line)

    # Collect target info

    target_dir = os.path.join(root, "arch", target)

    if not os.path.exists(target_dir):
        error(f"unknown architecture '{target}'")

    cfg = configparser.ConfigParser(inline_comment_prefixes=";")
    cfg.read(target_dir + "/config.ini")

    ptr_size = int(cfg["Arch"]["PointerSize"])
    symbol_reloc_types = set(re.split(r"\s*,\s*", cfg["Arch"]["SymbolReloc"]))

    def is_exported(s):
        return (
            s["Bind"] != "LOCAL"
            and s["Type"] != "NOTYPE"
            and s["Ndx"] != "UND"
            and s["Name"] not in ["", "_init", "_fini"]
        )

    syms = list(filter(is_exported, collect_syms(input_name)))

    def is_data_symbol(s):
        return (
            s["Type"] == "OBJECT"
            # Allow vtables if --vtables is on
            and not (" for " in s["Demangled Name"] and args.vtables)
        )

    exported_data = [s["Name"] for s in syms if is_data_symbol(s)]
    if exported_data:
        # TODO: we can generate wrappers for const data without relocations (or only code relocations)
        warn(f"library '{input_name}' contains data symbols which won't be intercepted: " + ", ".join(exported_data))

    # Collect functions
    # TODO: warn if user-specified functions are missing

    orig_funs = filter(lambda s: s["Type"] == "FUNC", syms)

    all_funs = set()
    warn_versioned = False
    for s in orig_funs:
        if s["Version"] is not None:
            # TODO: support versions
            if not warn_versioned:
                warn(f"library {input_name} contains versioned symbols which are NYI")
                warn_versioned = True
            if verbose:
                print(f"Skipping versioned symbol {s['Name']}")
            continue
        all_funs.add(s["Name"])

    if funs is None:
        funs = sorted(list(all_funs))
        if not funs and not quiet:
            warn(f"no public functions were found in {input_name}")
    else:
        missing_funs = [name for name in funs if name not in all_funs]
        if missing_funs:
            warn("some user-specified functions are not present in library: " + ", ".join(missing_funs))
        funs = [name for name in funs if name in all_funs]

    if verbose:
        print("Exported functions:")
        for i, fun in enumerate(funs):
            print(f"  {i}: {fun}")

    # Collect vtables

    if args.vtables:
        cls_tables = {}
        cls_syms = {}

        for s in syms:
            m = re.match(r"^(vtable|typeinfo|typeinfo name) for (.*)", s["Demangled Name"])
            if m is not None and is_exported(s):
                typ, cls = m.groups()
                name = s["Name"]
                cls_tables.setdefault(cls, {})[typ] = name
                cls_syms[name] = s

        if verbose:
            print("Exported classes:")
            for cls, _ in sorted(cls_tables.items()):
                print(f"  {cls}")

        secs = collect_sections(input_name)
        if verbose:
            print("Sections:")
            for sec in secs:
                print(f"  {sec['Name']}: [{sec['Address']:x}, {sec['Address'] + sec['Size']:x}), " f"at {sec['Off']:x}")

        bites = read_unrelocated_data(input_name, cls_syms, secs)

        rels = collect_relocs(input_name)
        if verbose:
            print("Relocs:")
            for rel in rels:
                sym_add = rel["Symbol's Name + Addend"]
                print(f"  {rel['Offset']}: {sym_add}")

        cls_data = collect_relocated_data(cls_syms, bites, rels, ptr_size, symbol_reloc_types)
        if verbose:
            print("Class data:")
            for name, data in sorted(cls_data.items()):
                demangled_name = cls_syms[name]["Demangled Name"]
                print(f"  {name} ({demangled_name}):")
                for typ, val in data:
                    print("    " + str(val if typ != "reloc" else val["Symbol's Name + Addend"]))

    # Generate assembly code

    suffix = os.path.basename(load_name)
    lib_suffix = re.sub(r"[^a-zA-Z_0-9]+", "_", suffix)

    tramp_file = f"{suffix}.tramp.S"
    with open(os.path.join(outdir, tramp_file), "w") as f:
        if not quiet:
            print(f"Generating {tramp_file}...")
        with open(target_dir + "/table.S.tpl", "r") as t:
            table_text = string.Template(t.read()).substitute(
                lib_suffix=lib_suffix, table_size=ptr_size * (len(funs) + 1)
            )
        f.write(table_text)

        with open(target_dir + "/trampoline.S.tpl", "r") as t:
            tramp_tpl = string.Template(t.read())

        for i, name in enumerate(funs):
            tramp_text = tramp_tpl.substitute(
                lib_suffix=lib_suffix, sym=args.symbol_prefix + name, offset=i * ptr_size, number=i
            )
            f.write(tramp_text)

    # Generate C code

    init_file = f"{suffix}.init.cpp"
    with open(os.path.join(outdir, init_file), "w") as f:
        if not quiet:
            print(f"Generating {init_file}...")
        with open(os.path.join(root, "arch/common/init.cpp.tpl"), "r") as t:
            if funs:
                sym_names = ",\n  ".join(f'"{name}"' for name in funs) + ","
            else:
                sym_names = ""
            init_text = string.Template(t.read()).substitute(
                lib_suffix=lib_suffix,
                load_name=load_name,
                dlopen_callback=dlopen_callback,
                has_dlopen_callback=int(bool(dlopen_callback)),
                sym_names=sym_names,
            )
            f.write(init_text)
        if args.vtables:
            vtable_text = generate_vtables(cls_tables, cls_syms, cls_data)
            f.write(vtable_text)


if __name__ == "__main__":
    main()
