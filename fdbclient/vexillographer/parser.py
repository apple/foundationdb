"""XML parser for fdb.options file."""

import xml.etree.ElementTree as ET
from typing import List
from .models import Option, Scope, ParamType


def parse_options(xml_path: str, binding: str) -> List[Option]:
    """Parse the fdb.options XML file and return a list of Option objects.

    Args:
        xml_path: Path to the fdb.options XML file
        binding: The target binding name (e.g., 'c', 'cpp', 'java', 'python', 'ruby')

    Returns:
        List of Option objects, filtered by disableOn attribute

    Raises:
        FileNotFoundError: If the XML file doesn't exist
        ValueError: If XML is malformed or required attributes are missing
    """
    try:
        tree = ET.parse(xml_path)
        root = tree.getroot()

        if root.tag != "Options":
            raise ValueError(f"Expected root element 'Options', got '{root.tag}'")

        options = []

        # Mapping from XML scope names to Scope enum
        scope_map = {
            "NetworkOption": Scope.NETWORK_OPTION,
            "DatabaseOption": Scope.DATABASE_OPTION,
            "TransactionOption": Scope.TRANSACTION_OPTION,
            "StreamingMode": Scope.STREAMING_MODE,
            "MutationType": Scope.MUTATION_TYPE,
            "ConflictRangeType": Scope.CONFLICT_RANGE_TYPE,
            "ErrorPredicate": Scope.ERROR_PREDICATE,
        }

        for scope_elem in root.findall("Scope"):
            scope_name = _get_required_attr(scope_elem, "name", "Scope")
            scope = scope_map.get(scope_name)
            if scope is None:
                raise ValueError(f"Unknown scope: {scope_name}")

            for option_elem in scope_elem.findall("Option"):
                # Check if this option is disabled for the current binding
                disable_on = _get_optional_attr(option_elem, "disableOn")
                if disable_on:
                    disabled_bindings = [b.strip() for b in disable_on.split(",")]
                    if binding in disabled_bindings:
                        continue  # Skip this option

                # Parse parameter type
                param_type_str = _get_optional_attr(option_elem, "paramType")
                if param_type_str:
                    try:
                        param_type = ParamType[param_type_str.upper()]
                    except KeyError:
                        raise ValueError(f"Unknown parameter type: {param_type_str}")
                else:
                    param_type = ParamType.NONE

                # Parse optional attributes
                hidden = _get_optional_attr(option_elem, "hidden") == "true"
                persistent = _get_optional_attr(option_elem, "persistent") == "true"
                sensitive = _get_optional_attr(option_elem, "sensitive") == "true"

                default_for_str = _get_optional_attr(option_elem, "defaultFor")
                default_for = int(default_for_str) if default_for_str else None

                # Create option object
                option = Option(
                    scope=scope,
                    name=_get_required_attr(option_elem, "name", "Option"),
                    code=int(_get_required_attr(option_elem, "code", "Option")),
                    param_type=param_type,
                    param_desc=_get_optional_attr(option_elem, "paramDescription"),
                    comment=_get_optional_attr(option_elem, "description") or "",
                    hidden=hidden,
                    persistent=persistent,
                    sensitive=sensitive,
                    default_for=default_for,
                )
                options.append(option)

        return options

    except ET.ParseError as e:
        raise ValueError(f"Failed to parse XML: {e}")
    except FileNotFoundError:
        raise FileNotFoundError(f"Options file not found: {xml_path}")


def _get_required_attr(element: ET.Element, attr_name: str, element_type: str) -> str:
    """Get a required attribute value or raise an error."""
    value = element.get(attr_name)
    if value is None:
        raise ValueError(
            f"{element_type} element missing required attribute '{attr_name}'"
        )
    return value


def _get_optional_attr(element: ET.Element, attr_name: str) -> str:
    """Get an optional attribute value or return None."""
    return element.get(attr_name)
