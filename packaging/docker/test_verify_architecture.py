#!/usr/bin/env python3
"""Check Docker artifact architecture validation without building an image."""

from pathlib import Path
import struct
import subprocess
import tempfile
import unittest


SCRIPT = Path(__file__).with_name("verify-architecture.sh")


class VerifyArchitectureTest(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.root = Path(self.tmp.name)

    def elf(self, name, machine, elf_class=2, byte_order=1):
        identification = b"\x7fELF" + bytes([elf_class, byte_order, 1]) + bytes(9)
        header = struct.pack(
            "<16sHHIQQQIHHHHHH",
            identification,
            3,
            machine,
            1,
            0,
            0,
            0,
            0,
            64,
            0,
            0,
            0,
            0,
            0,
        )
        path = self.root / name
        path.write_bytes(header)
        return path

    def verify(self, arch, *paths):
        return subprocess.run(
            ["sh", str(SCRIPT), arch, *map(str, paths)],
            capture_output=True,
            text=True,
        )

    def test_matching_architectures(self):
        for arch, machine in (("amd64", 62), ("arm64", 183)):
            with self.subTest(arch=arch):
                path = self.elf(f"libfdb_c.{arch}.so", machine)
                result = self.verify(arch, path)
                self.assertEqual(result.returncode, 0, result.stderr)

    def test_x86_library_with_arm_filename(self):
        path = self.elf("libfdb_c.aarch64.so", 62)
        result = self.verify("arm64", path)
        self.assertNotEqual(result.returncode, 0)
        self.assertIn("Architecture mismatch", result.stderr)
        self.assertIn(str(path), result.stderr)
        self.assertIn("arm64", result.stderr)

    def test_arm_library_for_amd64(self):
        result = self.verify("amd64", self.elf("libfdb_c.x86_64.so", 183))
        self.assertNotEqual(result.returncode, 0)

    def test_invalid_elf_identification(self):
        for elf_class, byte_order in ((1, 1), (2, 2)):
            with self.subTest(elf_class=elf_class, byte_order=byte_order):
                path = self.elf("library", 183, elf_class, byte_order)
                self.assertNotEqual(self.verify("arm64", path).returncode, 0)

    def test_invalid_or_missing_files(self):
        for content in (b"", b"\x7fELF", b"not an ELF library" * 4):
            with self.subTest(content=content):
                path = self.root / "library"
                path.write_bytes(content)
                self.assertNotEqual(self.verify("arm64", path).returncode, 0)
        self.assertNotEqual(self.verify("arm64", self.root / "missing").returncode, 0)

    def test_every_file_is_checked(self):
        first = self.elf("first.so", 183)
        second = self.elf("second.so", 62)
        result = self.verify("arm64", first, second)
        self.assertNotEqual(result.returncode, 0)
        self.assertIn(str(second), result.stderr)

    def test_unsupported_architecture(self):
        result = self.verify("unknown", self.elf("library", 183))
        self.assertNotEqual(result.returncode, 0)
        self.assertIn("Unsupported target architecture", result.stderr)


if __name__ == "__main__":
    unittest.main()
