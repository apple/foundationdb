"""Utility functions for vexillographer."""

import re


def _camel_component(component: str) -> str:
    """Return component for CamelCase while preserving all-uppercase acronyms."""
    if component.isupper():
        return component
    return component.capitalize()


def snake_to_camel(name: str) -> str:
    """Convert snake_case to CamelCase.

    Examples:
        trace_enable -> TraceEnable
        local_address -> LocalAddress
    """
    components = name.split("_")
    return "".join(_camel_component(x) for x in components)


def snake_to_lower_camel(name: str) -> str:
    """Convert snake_case to lowerCamelCase.

    Examples:
        trace_enable -> traceEnable
        local_address -> localAddress
    """
    components = name.split("_")
    if not components:
        return name
    first = components[0]
    first_component = first if first.isupper() else first.lower()
    return first_component + "".join(_camel_component(x) for x in components[1:])


def snake_to_upper_snake(name: str) -> str:
    """Convert snake_case to UPPER_SNAKE_CASE.

    Examples:
        trace_enable -> TRACE_ENABLE
        local_address -> LOCAL_ADDRESS
    """
    return name.upper()


def format_c_comment(text: str, indent: str = "") -> str:
    """Format a comment for C-style output.

    Wraps text at approximately 80 characters and adds appropriate comment markers.
    """
    if not text:
        return ""

    lines = []
    words = text.split()
    current_line = []
    current_length = len(indent) + 3  # Account for "// "

    for word in words:
        word_len = len(word) + 1  # +1 for space
        if current_length + word_len > 80 and current_line:
            lines.append(f"{indent}// {' '.join(current_line)}")
            current_line = [word]
            current_length = len(indent) + 3 + len(word)
        else:
            current_line.append(word)
            current_length += word_len

    if current_line:
        lines.append(f"{indent}// {' '.join(current_line)}")

    return "\n".join(lines)


def format_javadoc_comment(text: str, indent: str = "    ") -> str:
    """Format a comment as JavaDoc.

    Converts double-backticks to {@code } and wraps appropriately.
    """
    if not text:
        return ""

    # Convert double-backticks to {@code }
    text = re.sub(r"``([^`]+)``", r"{@code \1}", text)

    lines = [f"{indent}/**"]
    words = text.split()
    current_line = []
    current_length = len(indent) + 3  # Account for " * "

    for word in words:
        word_len = len(word) + 1
        if current_length + word_len > 100 and current_line:
            lines.append(f"{indent} * {' '.join(current_line)}")
            current_line = [word]
            current_length = len(indent) + 3 + len(word)
        else:
            current_line.append(word)
            current_length += word_len

    if current_line:
        lines.append(f"{indent} * {' '.join(current_line)}")

    lines.append(f"{indent} */")
    return "\n".join(lines)


def format_python_comment(text: str, indent: str = "    ") -> str:
    """Format a comment for Python output."""
    if not text:
        return ""
    return f"{indent}# {text}"


def escape_string(text: str, language: str) -> str:
    """Escape special characters for different languages."""
    if language in ("c", "cpp", "java"):
        return text.replace("\\", "\\\\").replace('"', '\\"').replace("\n", "\\n")
    elif language == "python":
        return text.replace("\\", "\\\\").replace("'", "\\'").replace("\n", "\\n")
    elif language == "ruby":
        return text.replace("\\", "\\\\").replace('"', '\\"').replace("\n", "\\n")
    return text


def replace_backticks_for_java(text: str) -> str:
    """Replace double backticks with JavaDoc {@code } tags.

    Converts ``example`` to {@code example}.
    """
    if not text:
        return text

    result = []
    i = 0
    while i < len(text):
        if i < len(text) - 1 and text[i : i + 2] == "``":
            # Found opening backticks
            closing = text.find("``", i + 2)
            if closing != -1:
                # Found closing backticks
                code_content = text[i + 2 : closing]
                result.append("{@code ")
                result.append(code_content)
                result.append("}")
                i = closing + 2
            else:
                # No closing backticks, just output the character
                result.append(text[i])
                i += 1
        else:
            result.append(text[i])
            i += 1

    return "".join(result)
