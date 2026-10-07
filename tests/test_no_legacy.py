from pathlib import Path

BANNED_CONSTRUCTS = [
    "streaming=True",
    "_export_to_c",
    "_import_from_c",
    "duckdb.arrow",
    "duckdb.sql",
    "duckdb.query",
    "duckdb.execute",
    ".fetchall(",
    ".fetchdf(",
    ".df(",
    ".to_pandas(",
    "json.loads",
    "typing.List",
    "typing.Dict",
    "typing.Optional",
    "typing.Union",
    "typing.Tuple",
    "Table.append(",
    "print(",
    "pyspark",
    "py4j",
    "jpype",
]


def test_no_legacy_constructs() -> None:
    src_dir = Path(__file__).parent.parent / "src" / "lakehouse_engine"
    for py_file in src_dir.rglob("*.py"):
        content = py_file.read_text()
        if "json.loads" in content and "ingestion" in str(py_file):
            raise AssertionError(f"Found banned json.loads in {py_file}")
        for banned in BANNED_CONSTRUCTS:
            if banned == "json.loads":
                continue
            if banned in content:
                if banned == "print(" and "__main__.py" in str(py_file):
                    continue
                raise AssertionError(f"Found banned construct '{banned}' in {py_file}")


# os.system
