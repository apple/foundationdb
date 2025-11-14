"""C++ language generator."""

from typing import List
from .base import BaseGenerator
from ..models import Option, Scope


class CppGenerator(BaseGenerator):
    """Generator for C++ bindings."""

    def write_files(self, output_path: str) -> None:
        """Generate C++ header and implementation files."""
        # Group options by scope, preserving enum order
        all_scopes = self.group_by_scope()

        # Create ordered list of (scope, options) tuples in enum order
        scopes_ordered = [(scope, all_scopes.get(scope, [])) for scope in Scope]

        # Render and write header file
        header_content = self.render_template(
            "cpp_header.h.j2", {"scopes": scopes_ordered}
        )
        self.write_file(output_path + ".h", header_content)

        # Render and write implementation file
        impl_content = self.render_template(
            "cpp_impl.cpp.j2", {"scopes": scopes_ordered}
        )
        self.write_file(output_path + ".cpp", impl_content)
