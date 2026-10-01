# tests/test_no_legacy.py
import ast
from pathlib import Path


def test_no_legacy_constructs():
    src_dir = Path("src/lakehouse_engine")
    py_files = list(src_dir.rglob("*.py"))

    banned_func_calls = {"fetchall", "fetchdf", "df", "to_pandas"}
    banned_duckdb_module_calls = {"arrow", "sql", "query", "execute"}
    banned_imports = {"pyspark", "py4j", "jpype"}
    banned_arrow_c_methods = {"_export_to_c", "_import_from_c"}

    for path in py_files:
        content = path.read_text(encoding="utf-8")
        tree = ast.parse(content, filename=str(path))

        for node in ast.walk(tree):
            # Check print calls
            if isinstance(node, ast.Call) and isinstance(node.func, ast.Name):
                assert node.func.id != "print", f"print() call is banned in {path}"

            # Check banned imports
            if isinstance(node, ast.Import):
                for alias in node.names:
                    assert alias.name not in banned_imports, (
                        f"Banned import '{alias.name}' in {path}"
                    )
            elif isinstance(node, ast.ImportFrom) and node.module:
                mod_root = node.module.split(".")[0]
                assert mod_root not in banned_imports, (
                    f"Banned import from '{node.module}' in {path}"
                )

            # Check banned calls
            if isinstance(node, ast.Call) and isinstance(node.func, ast.Attribute):
                # Check legacy Arrow C methods
                assert node.func.attr not in banned_arrow_c_methods, (
                    f"Banned legacy Arrow C method '.{node.func.attr}' in {path}"
                )

                # Check method calls like .fetchall(
                assert node.func.attr not in banned_func_calls, (
                    f"Banned method call '.{node.func.attr}' in {path}"
                )

                # Check DuckDB module level calls like duckdb.arrow(
                if isinstance(node.func.value, ast.Name) and node.func.value.id == "duckdb":
                    assert node.func.attr not in banned_duckdb_module_calls, (
                        f"Banned module-level duckdb call 'duckdb.{node.func.attr}' in {path}"
                    )

                # Check Table.append
                if node.func.attr == "append" and isinstance(node.func.value, ast.Name):
                    assert node.func.value.id != "table", f"Table.append is banned in {path}"

            # Check json.loads inside ingestion/
            if (
                "ingestion" in path.parts
                and isinstance(node, ast.Call)
                and isinstance(node.func, ast.Attribute)
                and isinstance(node.func.value, ast.Name)
                and node.func.value.id == "json"
            ):
                assert node.func.attr != "loads", f"json.loads is banned in ingestion/ ({path})"

            # Check streaming=True keyword arg
            if isinstance(node, ast.Call):
                for kw in node.keywords:
                    if (
                        kw.arg == "streaming"
                        and isinstance(kw.value, ast.Constant)
                        and kw.value.value is True
                    ):
                        raise AssertionError(f"streaming=True kwarg is banned in {path}")
