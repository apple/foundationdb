"""CLI entry point for vexillographer."""

import sys
import argparse
from pathlib import Path
from typing import Type

from .parser import parse_options
from .generators.base import BaseGenerator
from .generators.c import CGenerator
from .generators.cpp import CppGenerator
from .generators.java import JavaGenerator
from .generators.python_gen import PythonGenerator
from .generators.ruby import RubyGenerator


# Generator registry
GENERATORS = {
    "c": CGenerator,
    "cpp": CppGenerator,
    "java": JavaGenerator,
    "python": PythonGenerator,
    "ruby": RubyGenerator,
}


def main() -> int:
    """Main entry point for vexillographer CLI."""
    parser = argparse.ArgumentParser(
        description="Generate language bindings from fdb.options XML file",
        prog="vexillographer",
    )
    parser.add_argument("input_file", help="Path to fdb.options XML file")
    parser.add_argument(
        "language",
        choices=list(GENERATORS.keys()),
        help="Target language for code generation",
    )
    parser.add_argument("output", help="Output file path or directory (for Java)")

    args = parser.parse_args()

    try:
        # Validate input file exists
        input_path = Path(args.input_file)
        if not input_path.exists():
            print(f"Error: Input file not found: {args.input_file}", file=sys.stderr)
            return 1

        # Parse options from XML
        print(f"Parsing {args.input_file}...", file=sys.stderr)
        options = parse_options(args.input_file, args.language)
        print(f"Parsed {len(options)} options", file=sys.stderr)

        # Get the appropriate generator
        generator_class: Type[BaseGenerator] = GENERATORS[args.language]
        generator = generator_class(options)

        # Generate output files
        print(
            f"Generating {args.language} bindings to {args.output}...", file=sys.stderr
        )
        generator.write_files(args.output)
        print("Generation complete!", file=sys.stderr)

        return 0

    except FileNotFoundError as e:
        print(f"Error: {e}", file=sys.stderr)
        return 1
    except ValueError as e:
        print(f"Error parsing options: {e}", file=sys.stderr)
        return 1
    except Exception as e:
        print(f"Unexpected error: {e}", file=sys.stderr)
        import traceback

        traceback.print_exc()
        return 31


if __name__ == "__main__":
    sys.exit(main())
