#!/usr/bin/env python
import sys


def main():
    if len(sys.argv) != 2:
        print("Usage: txt-to-toml.py [src.txt]")
        return 1

    filename = sys.argv[1]

    indent = "    "
    in_workload = False
    first_test = False
    keys_before_test = False

    for line in open(filename):
        k = ""
        v = ""

        if line.strip().startswith(";"):
            print((indent if in_workload else "") + line.strip().replace(";", "#"))
            continue

        if "=" in line:
            (k, v) = line.strip().split("=")
            (k, v) = (k.strip(), v.strip())

        if k == "testTitle":
            first_test = True
            if in_workload:
                print("")
            in_workload = False
            if keys_before_test:
                print("")
                keys_before_test = False
            print("[[test]]")

        if k == "testName":
            in_workload = True
            print("")
            print(indent + "[[test.workload]]")

        if not first_test:
            keys_before_test = True

        if v.startswith("."):
            v = "0" + v

        if any(c.isalpha() or c in ["/", "!"] for c in v):
            if v != "true" and v != "false":
                v = "'" + v + "'"

        if k == "buggify":
            print("buggify = " + ("true" if v == "'on'" else "false"))
        elif k:
            print((indent if in_workload else "") + k + " = " + v)

    return 0


if __name__ == "__main__":
    sys.exit(main())
