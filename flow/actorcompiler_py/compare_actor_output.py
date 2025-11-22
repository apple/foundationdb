import argparse
import sys
import re
import difflib
from pathlib import Path

def compare_outputs(file1_path: Path, file2_path: Path) -> bool:
    try:
        file1_content = file1_path.read_text()
        file2_content = file2_path.read_text()

        if file1_content == file2_content:
            return True

        # Normalize #line directives - replace the filename part with a constant
        # Pattern: #line <number> "filename"
        line_directive_pattern = re.compile(r'(#line\s+\d+\s+")[^"]*(")')

        file1_normalized = line_directive_pattern.sub(
            r"\1NORMALIZED_PATH\2", file1_content
        )
        file2_normalized = line_directive_pattern.sub(
            r"\1NORMALIZED_PATH\2", file2_content
        )

        if file1_normalized == file2_normalized:
            return True

        # Generate detailed diff of normalized content
        file1_lines = file1_normalized.splitlines(keepends=True)
        file2_lines = file2_normalized.splitlines(keepends=True)

        diff = difflib.unified_diff(
            file1_lines,
            file2_lines,
            fromfile=f"{file1_path} (normalized)",
            tofile=f"{file2_path} (normalized)",
            lineterm="",
        )

        diff_text = "".join(diff)
        print(diff_text, file=sys.stderr)
        return False

    except Exception as e:
        print(f"Error comparing files: {e}", file=sys.stderr)
        return False

def main():
    parser = argparse.ArgumentParser(description="Compare actor compiler outputs")
    parser.add_argument("file1", type=Path, help="First file to compare")
    parser.add_argument("file2", type=Path, help="Second file to compare")
    args = parser.parse_args()

    if not compare_outputs(args.file1, args.file2):
        sys.exit(1)

if __name__ == "__main__":
    main()

