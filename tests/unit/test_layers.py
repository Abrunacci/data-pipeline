"""``core`` is the pipeline's rules: it must not depend on any library or on the rest of the
package, so it can be reused and tested without I/O."""

from __future__ import annotations

import ast
import sys
from pathlib import Path

import data_pipeline.core

CORE = Path(data_pipeline.core.__file__).parent


def imported_modules(path: Path) -> set[str]:
    modules: set[str] = set()
    for node in ast.walk(ast.parse(path.read_text())):
        if isinstance(node, ast.Import):
            modules.update(alias.name for alias in node.names)
        elif isinstance(node, ast.ImportFrom) and node.module is not None:
            modules.add(node.module)
    return modules


def test_core_imports_only_the_standard_library_and_itself() -> None:
    for path in sorted(CORE.glob("*.py")):
        for module in imported_modules(path):
            top = module.split(".")[0]
            allowed = top in sys.stdlib_module_names or module.startswith("data_pipeline.core")
            assert allowed, f"{path.name} imports {module}"
