#!/usr/bin/env python3
import shlex
import subprocess
import sys
from pathlib import Path


def run_cmd(cmd: str) -> tuple[int, str]:
    args = shlex.split(cmd)
    res = subprocess.run(args, shell=False, capture_output=True, text=True, check=False)  # noqa: S603
    out = (res.stdout + "\n" + res.stderr).strip()
    return res.returncode, out


def main() -> int:
    checks: list[tuple[str, bool]] = []

    # 1. Branch check
    rc, git_branch = run_cmd("git rev-parse --abbrev-ref HEAD")
    checks.append(("branch_master", git_branch.strip() == "master"))

    # 2. Clean working tree check
    rc, git_status = run_cmd("git status --porcelain")
    checks.append(("clean_working_tree", git_status.strip() == ""))

    # 3. README checks
    readme = Path("README.md").read_text(encoding="utf-8") if Path("README.md").exists() else ""
    no_synthetic = (
        "Representative Benchmark Results" not in readme
        and "Est. S3 Requests" not in readme
        and ">99% savings" not in readme
    )
    checks.append(("readme_no_synthetic_benchmarks", no_synthetic))

    no_tmp = "file:///tmp/warehouse" not in readme and "/tmp/lhe/dlq" not in readme  # noqa: S108
    checks.append(("readme_no_tmp_quickstart", no_tmp))

    has_verified = (
        "Verified Benchmark Results" in readme
        and "bench-out/report.md" in readme
        and "bench-out/results.json" in readme
    )
    checks.append(("readme_verified_benchmark_section", has_verified))

    # 4. Benchmark artifacts
    art_exist = Path("bench-out/report.md").exists() and Path("bench-out/results.json").exists()
    checks.append(("benchmark_artifacts_exist", art_exist))

    # 5. Benchmark metadata
    report = Path("bench-out/report.md").read_text(encoding="utf-8") if art_exist else ""
    results = Path("bench-out/results.json").read_text(encoding="utf-8") if art_exist else ""
    meta_ok = (
        "linux" in report
        and "CPUs" in report
        and "seed" in results
        and "python_version" in results
        and "pyarrow_version" in results
        and "polars_version" in results
        and "duckdb_version" in results
        and "pyiceberg_version" in results
    )
    checks.append(("benchmark_metadata_complete", meta_ok))

    # 6. Source fixes checks
    buffer_code = (
        Path("src/lakehouse_engine/buffer/buffer.py").read_text(encoding="utf-8")
        if Path("src/lakehouse_engine/buffer/buffer.py").exists()
        else ""
    )
    checks.append(
        (
            "buffer_flush_id_preservation",
            "_active_flush_id" in buffer_code and "recover_pending" in buffer_code,
        )
    )

    runtime_code = (
        Path("src/lakehouse_engine/runtime.py").read_text(encoding="utf-8")
        if Path("src/lakehouse_engine/runtime.py").exists()
        else ""
    )
    checks.append(("runtime_memory_pool_guard", "_guarantee_pyarrow_memory_pool" in runtime_code))

    engine_code = (
        Path("src/lakehouse_engine/engine.py").read_text(encoding="utf-8")
        if Path("src/lakehouse_engine/engine.py").exists()
        else ""
    )
    checks.append(("engine_public_arrow_api", "append_arrow_batch" in engine_code))

    duckdb_code = (
        Path("src/lakehouse_engine/query/duckdb_adapter.py").read_text(encoding="utf-8")
        if Path("src/lakehouse_engine/query/duckdb_adapter.py").exists()
        else ""
    )
    checks.append(("duckdb_temp_dir_hardening", "mkdir" in duckdb_code))

    no_legacy_code = (
        Path("tests/test_no_legacy.py").read_text(encoding="utf-8")
        if Path("tests/test_no_legacy.py").exists()
        else ""
    )
    ast_ok = (
        "os.system" in no_legacy_code
        and "pickle.loads" in no_legacy_code
        and "subprocess.run" in no_legacy_code
        and "eval" in no_legacy_code
        and "exec" in no_legacy_code
    )
    checks.append(("ast_scanner_dangerous_construct_bans", ast_ok))

    # 7. CHANGELOG check
    changelog = (
        Path("CHANGELOG.md").read_text(encoding="utf-8") if Path("CHANGELOG.md").exists() else ""
    )
    checks.append(
        (
            "changelog_consistency",
            "Replaced synthetic benchmark claims in README with real benchmark reports"
            not in changelog,
        )
    )

    # 8. Quality gates
    rc, _ = run_cmd("uv run ruff check src tests")
    checks.append(("lint", rc == 0))

    rc, _ = run_cmd("uv run ruff format --check src tests")
    checks.append(("format", rc == 0))

    rc, _ = run_cmd("uv run mypy src")
    checks.append(("mypy", rc == 0))

    rc, _ = run_cmd("uv run pytest -q")
    checks.append(("pytest", rc == 0))

    rc, _ = run_cmd(
        "uv run python -m lakehouse_engine.benchmarks --rows 100000 --runs 1 --out ./bench-out"
    )
    checks.append(("benchmark_generation", rc == 0))

    # Output table
    sys.stdout.write(f"{'CHECK':<40} {'STATUS'}\n")
    sys.stdout.write("-" * 50 + "\n")
    all_pass = True
    for name, ok in checks:
        status = "PASS" if ok else "FAIL"
        if not ok:
            all_pass = False
        sys.stdout.write(f"{name:<40} {status}\n")

    return 0 if all_pass else 1


if __name__ == "__main__":
    sys.exit(main())
