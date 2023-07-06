#! /usr/bin/env python3

"""protocol_version.py

Tool to manipulate FoundationDB Protocol Versions for other programming languages
"""

import abc
import argparse
import io
import json
import os
import re
import sys

from typing import Dict, List

import jinja2


SCRIPT_NAME = os.path.basename(__file__)
SCRIPT_DIRECTORY = os.path.dirname(os.path.abspath(__file__))


def _version_to_hex_string(version: int) -> str:
    return "0x{:016X}".format(version)


class ProtocolVersion:
    def __init__(self):
        self._default_version = None
        self._future_version = None
        self._min_compatibile_version = None
        self._min_invalid_version = None
        self._left_most_check = None
        self._lsb_mask = None

        self._features: Dict[int, List[str]] = dict()

    def set_default_version(self, version: int):
        self._default_version = version

    def set_future_version(self, version: int):
        self._future_version = version

    def set_min_compatible_version(self, version: int):
        self._min_compatibile_version = version

    def set_min_invalid_version(self, version: int):
        self._min_invalid_version = version

    def set_left_most_check(self, version: int):
        self._left_most_check = version

    def set_lsb_mask(self, version: int):
        self._lsb_mask = version

    @property
    def default_version(self):
        return self._default_version

    @property
    def future_version(self):
        return self._future_version

    @property
    def min_compatible_version(self):
        return self._min_compatibile_version

    @property
    def min_invalid_version(self):
        return self._min_invalid_version

    @property
    def left_most_check(self):
        return self._left_most_check

    @property
    def lsb_mask(self):
        return self._lsb_mask

    def add_feature(self, version: int, feature: str):
        if version not in self._features:
            self._features[version] = []
        self._features[version].append(feature)

        return self

    @property
    def features(self):
        return self._features


def _remove_prefix(text: str, prefix: str) -> str:
    if text.startswith(prefix):
        return text[len(prefix) :]
    return text


def _remove_postfix(text: str, postfix: str) -> str:
    if text.endswith(postfix):
        return text[: len(text) - len(postfix)]
    return text


class ProtocolVersionSerializerBase(abc.ABC):
    @abc.abstractmethod
    def _load(self, stream: io.TextIOWrapper) -> ProtocolVersion:
        raise NotImplementedError()

    def load(self, stream: io.TextIOWrapper) -> ProtocolVersion:
        return self._load(stream)

    @abc.abstractmethod
    def _save(self, protocol_version: ProtocolVersion, stream: io.TextIOWrapper):
        raise NotImplementedError()

    def save(self, protocol_version: ProtocolVersion, stream: io.TextIOWrapper):
        return self._save(protocol_version, stream)


class CMakeProtocolVersionSerializer(ProtocolVersionSerializerBase):
    SPECIAL_FIELDS = {
        "DEFAULT_VERSION": ProtocolVersion.set_default_version,
        "FUTURE_VERSION": ProtocolVersion.set_future_version,
        "MIN_COMPATIBLE_VERSION": ProtocolVersion.set_min_compatible_version,
        "MIN_INVALID_VERSION": ProtocolVersion.set_min_invalid_version,
        "LEFT_MOST_CHECK": ProtocolVersion.set_left_most_check,
        "LSB_MASK": ProtocolVersion.set_lsb_mask,
    }

    def _decode_version(self, encoded: str) -> int:
        """Decode version like 0x0FDB00B073000000LL into decimal value"""
        return int(_remove_postfix(encoded, "LL"), 16)

    def _load(self, stream: io.TextIOWrapper) -> ProtocolVersion:
        protocol_version_cmake_regex = re.compile(
            r"set\((?P<feature>[^\s]+)\s+\"(?P<version>[^\"]+)\"\)"
        )
        protocol_version = ProtocolVersion()
        for line in stream:
            match = protocol_version_cmake_regex.search(line)
            if not match:
                continue
            key_name = _remove_prefix(match.groupdict()["feature"], "FDB_PV_")
            value = self._decode_version(match.groupdict()["version"])

            if key_name in CMakeProtocolVersionSerializer.SPECIAL_FIELDS:
                CMakeProtocolVersionSerializer.SPECIAL_FIELDS[key_name](
                    protocol_version, value
                )
            else:
                protocol_version.add_feature(value, key_name)

        return protocol_version

    def _save(self, protocol_version: ProtocolVersion, stream: io.TextIOWrapper):
        raise NotImplementedError()


class JSONProtocolVersionSerialzer(ProtocolVersionSerializerBase):
    def _load(self, stream: io.TextIOWrapper) -> ProtocolVersion:
        raise NotImplementedError()

    def _save(self, protocol_version: ProtocolVersion, stream: io.TextIOWrapper):
        result_dict = {
            "default_version": protocol_version.default_version,
            "future_version": protocol_version.future_version,
            "min_compatible_version": protocol_version.min_compatible_version,
            "min_invalid_version": protocol_version.min_invalid_version,
            "left_most_check": protocol_version.left_most_check,
            "lsb_mask": protocol_version.lsb_mask,
            "features": protocol_version._features,
        }
        stream.write(json.dumps(result_dict, ident=2))


class NameTransformer(abc.ABC):
    """Transform the name in FDB_PV_UPPER_CASE into some other format"""

    @abc.abstractmethod
    def transform_feature_text(self, feature: str) -> str:
        raise NotImplementedError()


class CamelCaseNameTransformer(NameTransformer):
    def _all_caps_to_camel(self, text: str) -> str:
        """Translate ABC_DEF to AbcDef"""
        return "".join(item.capitalize() for item in text.split("_"))


class JavaCamelCaseNameTransformer(CamelCaseNameTransformer):
    """Transform the name in FDB_PV_UPPER_CASE into FdbPvUpperCase"""

    def transform_feature_text(self, feature: str) -> str:
        # Java stylechecker expects a tighter form of CamelCase, e.g. IPV6 -> Ipv6
        return self._all_caps_to_camel(feature)


class CxxCamelCaseNameTransformer(CamelCaseNameTransformer):
    """Transform the name in FDB_PV_UPPER_CASE into FdbPvUpperCase, with certain special cases for backward compatibility"""

    XXX_FIELD_MAPPING = {
        "IPV6": "IPv6",
        "INEXPENSIVE_MULTIVERSION_CLIENT": "InexpensiveMultiVersionClient",
        "TSS": "TSS",
        "DR_BACKUP_RANGES": "DRBackupRanges",
        "OTEL_SPAN_CONTEXT": "OTELSpanContext",
        "FDB_ENCRYPTED_SNAPSHOT_BACKUP_FILE": "EncryptedSnapshotBackupFile",
        "PROCESS_ID": "ProcessID",
        "SHARD_ENCODE_LOCATION_METADATA": "ShardEncodeLocationMetaData",
        "TLOG_VERSION": "TLogVersion",
        "SW_VERSION_TRACKING": "SWVersionTracking",
        "MULTIGENERATION_TLOG": "MultiGenerationTLog",
        "TLOG_QUEUE_ENTRY_REF": "TLogQueueEntryRef",
        "PROCESS_ID_FILE": "ProcessIDFile",
    }

    def transform_feature_text(self, feature: str) -> str:
        if feature in CxxCamelCaseNameTransformer.XXX_FIELD_MAPPING:
            return CxxCamelCaseNameTransformer.XXX_FIELD_MAPPING[feature]
        return self._all_caps_to_camel(feature)


class SnakeNameTransformer(NameTransformer):
    XXX_FIELD_MAPPING = {
        "IPV6": "IPv6",
    }

    def transform_feature_text(self, feature: str) -> str:
        if feature in SnakeNameTransformer.XXX_FIELD_MAPPING:
            return SnakeNameTransformer.XXX_FIELD_MAPPING[feature]
        return feature.lower()


class CodeGenBase(abc.ABC):
    def __init__(self, protocol_version: ProtocolVersion):
        self._protocol_version = protocol_version

    @property
    def protocol_version(self) -> ProtocolVersion:
        return self._protocol_version

    @abc.abstractmethod
    def _render(self):
        raise NotImplementedError()

    def _get_environment(
        self, encode_version, name_transformer: NameTransformer
    ) -> jinja2.Environment:
        env = jinja2.Environment(autoescape=True)
        env = jinja2.Environment(autoescape=True, trim_blocks=True, lstrip_blocks=True)
        env.filters["encode_version"] = encode_version
        env.filters[
            "feature_name_transformer"
        ] = name_transformer.transform_feature_text

        return env

    def render(self):
        return self._render()


class JavaCodeGen(CodeGenBase):
    JAVA_TEMPLATE_FILE = os.path.join(SCRIPT_DIRECTORY, "ProtocolVersion.java.template")

    def _encode_version(self, version: int) -> str:
        return "{}L".format(_version_to_hex_string(version))

    def _render(self):
        env = self._get_environment(
            self._encode_version, JavaCamelCaseNameTransformer()
        )
        with open(JavaCodeGen.JAVA_TEMPLATE_FILE) as template_stream:
            template = env.from_string(template_stream.read())

        return template.render({"all_features": self.protocol_version.features})


class CxxHeaderFileCodeGen(CodeGenBase):
    CXX_TEMPLATE_FILE = os.path.join(SCRIPT_DIRECTORY, "ProtocolVersion.h.template")

    def _encode_version(self, version: int) -> str:
        return "{}LL".format(_version_to_hex_string(version))

    def _render(self):
        env = self._get_environment(self._encode_version, CxxCamelCaseNameTransformer())
        with open(CxxHeaderFileCodeGen.CXX_TEMPLATE_FILE) as template_stream:
            template = env.from_string(template_stream.read())

        return template.render(
            {
                "all_features": self.protocol_version.features,
                "defaultVersion": self.protocol_version.default_version,
                "futureVersion": self.protocol_version.future_version,
                "minInvalidVersion": self.protocol_version.min_invalid_version,
                "minCompatibleVersion": self.protocol_version.min_compatible_version,
                "leftMostCheck": self.protocol_version.left_most_check,
                "lsbMask": self.protocol_version.lsb_mask,
            }
        )


class PythonLibraryCodeGen(CodeGenBase):
    PYTHON_TEMPLATE_FILE = os.path.join(
        SCRIPT_DIRECTORY, "protocol_version.py.template"
    )

    def _encode_version(self, version: int) -> str:
        return _version_to_hex_string(version)

    def _render(self):
        env = self._get_environment(self._encode_version, SnakeNameTransformer())
        with open(PythonLibraryCodeGen.PYTHON_TEMPLATE_FILE) as template_stream:
            template = env.from_string(template_stream.read())

        return template.render({"all_features": self.protocol_version.features})


def _setup_args():
    parser = argparse.ArgumentParser(prog=SCRIPT_NAME)

    parser.add_argument(
        "--source",
        type=str,
        required=True,
        help="Source of protocol versions",
    )

    parser.add_argument(
        "--generator", type=str, required=True, help="Code generator (cpp/java)"
    )

    parser.add_argument("--output", type=str, required=True, help="Output file")

    return parser.parse_args()


def main():
    args = _setup_args()

    with open(args.source) as stream:
        protocol_version = CMakeProtocolVersionSerializer().load(stream)

    if args.generator == "cpp":
        generator = CxxHeaderFileCodeGen
    elif args.generator == "java":
        generator = JavaCodeGen
    elif args.generator == "python":
        generator = PythonLibraryCodeGen
    else:
        raise RuntimeError("Unknown generator {}".format(args.generator))

    with open(args.output, "w") as stream:
        stream.write(generator(protocol_version).render())


if __name__ == "__main__":
    sys.exit(main())
