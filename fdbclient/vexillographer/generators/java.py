"""Java language generator."""

from pathlib import Path
from typing import List
from .base import BaseGenerator
from ..models import Option, Scope, ParamType


class JavaGenerator(BaseGenerator):
    """Generator for Java bindings."""

    def __init__(self, options: List[Option]):
        super().__init__(options)

        # Java type mapping
        self.java_type_map = {
            ParamType.INT: "long",
            ParamType.BYTES: "byte[]",
            ParamType.STRING: "String",
        }

        # Scope comments for JavaDoc
        self.scope_comments = {
            Scope.NETWORK_OPTION: "A set of options that can be set globally for the {@link FDB FoundationDB API}.",
            Scope.DATABASE_OPTION: "A set of options that can be set on a {@link Database}.",
            Scope.TRANSACTION_OPTION: "A set of options that can be set on a {@link Transaction}.",
            Scope.STREAMING_MODE: "Options that control the way the Java binding performs range reads. These options can be passed to {@link Transaction#getRange(byte[], byte[], int, boolean, StreamingMode) Transaction.getRange(...)}.",
            Scope.MUTATION_TYPE: "A set of operations that can be performed atomically on a database. These are used as parameters to {@link Transaction#mutate(MutationType, byte[], byte[])}.",
            Scope.CONFLICT_RANGE_TYPE: "Conflict range types used internally by the C API.",
            Scope.ERROR_PREDICATE: "Error code predicates for binding writers and non-standard layer implementers.",
        }

        # Which scopes are public
        self.scope_is_public = {
            Scope.NETWORK_OPTION: True,
            Scope.DATABASE_OPTION: True,
            Scope.TRANSACTION_OPTION: True,
            Scope.STREAMING_MODE: True,
            Scope.MUTATION_TYPE: True,
            Scope.CONFLICT_RANGE_TYPE: False,
            Scope.ERROR_PREDICATE: True,
        }

    def write_files(self, output_path: str) -> None:
        """Generate Java files for each scope."""
        # Ensure output directory exists
        output_dir = Path(output_path)
        if not output_dir.exists():
            raise FileNotFoundError(f"Output directory does not exist: {output_path}")

        # Group options by scope
        scopes = self.group_by_scope()

        # Generate a file for each scope
        for scope, options in scopes.items():
            self._write_scope_file(output_dir, scope, options)

    def _write_scope_file(
        self, output_dir: Path, scope: Scope, options: List[Option]
    ) -> None:
        """Write the Java file for a specific scope."""
        # Determine file name and template
        if scope == Scope.ERROR_PREDICATE:
            file_name = "FDBException.java"
            template_name = "java_exception.java.j2"
        elif self._is_settable_option(scope):
            file_name = f"{scope.value}s.java"
            template_name = "java_options.java.j2"
        else:
            file_name = f"{scope.value}.java"
            template_name = "java_enum.java.j2"

        # Sort options by if they have a comment
        options.sort(key=lambda x: (not x.comment))

        # Render template
        content = self.render_template(
            template_name,
            {
                "scope": scope,
                "options": options,
                "java_type_map": self.java_type_map,
                "scope_comments": self.scope_comments,
                "scope_is_public": self.scope_is_public,
            },
        )

        # Write file with standard Unix line endings
        file_path = output_dir / file_name
        with open(file_path, "w", encoding="utf-8") as f:
            f.write(content)

    def _is_settable_option(self, scope: Scope) -> bool:
        """Check if a scope is a settable option (vs an enum)."""
        return scope in (
            Scope.NETWORK_OPTION,
            Scope.DATABASE_OPTION,
            Scope.TRANSACTION_OPTION,
            Scope.ERROR_PREDICATE,
        )
