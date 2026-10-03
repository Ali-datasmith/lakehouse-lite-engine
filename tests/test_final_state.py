from pathlib import Path


def test_final_state_invariants() -> None:
    readme_content = Path("README.md").read_text(encoding="utf-8")
    changelog_content = Path("CHANGELOG.md").read_text(encoding="utf-8")

    # README check
    assert "Representative Benchmark Results" not in readme_content
    assert "Est. S3 Requests" not in readme_content
    assert ">99% savings" not in readme_content
    assert "file:///tmp/warehouse" not in readme_content
    assert "/tmp/lhe/dlq" not in readme_content  # noqa: S108
    assert "Verified Benchmark Results" in readme_content
    assert "bench-out/report.md" in readme_content
    assert "bench-out/results.json" in readme_content

    # Artifacts check
    assert Path("bench-out/report.md").exists()
    assert Path("bench-out/results.json").exists()

    # Code state checks
    buffer_py = Path("src/lakehouse_engine/buffer/buffer.py").read_text(encoding="utf-8")
    assert "_active_flush_id" in buffer_py
    assert "recover_pending" in buffer_py

    runtime_py = Path("src/lakehouse_engine/runtime.py").read_text(encoding="utf-8")
    assert "_guarantee_pyarrow_memory_pool" in runtime_py

    engine_py = Path("src/lakehouse_engine/engine.py").read_text(encoding="utf-8")
    assert "append_arrow_batch" in engine_py

    duckdb_adapter_py = Path("src/lakehouse_engine/query/duckdb_adapter.py").read_text(
        encoding="utf-8"
    )
    assert "mkdir" in duckdb_adapter_py

    no_legacy_py = Path("tests/test_no_legacy.py").read_text(encoding="utf-8")
    assert "os.system" in no_legacy_py
    assert "pickle.loads" in no_legacy_py
    assert "subprocess.run" in no_legacy_py
    assert "eval" in no_legacy_py
    assert "exec" in no_legacy_py

    # CHANGELOG checks
    msg = "Replaced synthetic benchmark claims in README with real benchmark reports"
    assert msg not in changelog_content
