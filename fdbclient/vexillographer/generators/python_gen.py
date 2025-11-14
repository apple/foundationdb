"""Python language generator."""

from typing import List
from .base import BaseGenerator
from ..models import Option, Scope, ParamType


class PythonGenerator(BaseGenerator):
    """Generator for Python bindings."""

    def __init__(self, options: List[Option]):
        super().__init__(options)
        self.param_type_map = {
            ParamType.NONE: "type(None)",
            ParamType.INT: "type(0)",
            ParamType.STRING: "type('')",
            ParamType.BYTES: "type(b'')",
        }

    def write_files(self, output_path: str) -> None:
        """Generate Python output file."""
        # Group options by scope, preserving enum order
        all_scopes = self.group_by_scope()

        # Create ordered list of (scope, options) tuples in enum order
        scopes_ordered = [(scope, all_scopes.get(scope, [])) for scope in Scope]

        # Render template
        content = self.render_template(
            "python.py.j2",
            {"scopes": scopes_ordered, "param_type_map": self.param_type_map},
        )

        # Write file
        self.write_file(output_path, content)
