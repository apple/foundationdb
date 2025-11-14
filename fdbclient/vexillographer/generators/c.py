"""C language generator."""

from typing import List
from .base import BaseGenerator
from ..models import Option, Scope


class CGenerator(BaseGenerator):
    """Generator for C bindings."""

    def write_files(self, output_path: str) -> None:
        """Generate C header file."""
        # Group options by scope, preserving enum order
        all_scopes = self.group_by_scope()

        # Create ordered list of (scope, options) tuples in enum order
        scopes_ordered = [(scope, all_scopes.get(scope, [])) for scope in Scope]

        # Render template
        content = self.render_template("c.h.j2", {"scopes": scopes_ordered})

        # Write file
        self.write_file(output_path, content)
