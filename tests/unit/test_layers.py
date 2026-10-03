"""``core`` is the pipeline's rules: it must not depend on any library or on the rest of the
package, so it can be reused and tested without I/O."""

from __future__ import annotations

import ast
import sys
from pathlib import Path

import data_pipeline.core
import data_pipeline.destinations
import data_pipeline.runner.heartbeat

CORE = Path(data_pipeline.core.__file__).parent
DESTINATIONS = Path(data_pipeline.destinations.__file__).parent
PACKAGE = CORE.parent


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


def test_the_health_check_imports_only_the_standard_library() -> None:
    # Docker starts it on every check; a slow import would be a failed check.
    path = Path(data_pipeline.runner.heartbeat.__file__)
    for module in imported_modules(path):
        assert module.split(".")[0] in sys.stdlib_module_names, f"heartbeat imports {module}"


def test_destinations_import_only_core_and_libraries() -> None:
    # They know an app's contract, not how the runner works.
    for path in sorted(DESTINATIONS.glob("*.py")):
        for module in imported_modules(path):
            if module.startswith("data_pipeline."):
                assert module.startswith("data_pipeline.core"), f"{path.name} imports {module}"
