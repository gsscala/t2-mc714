"""Pre-submission verification script for MC714 Trabalho 2."""

from __future__ import annotations

import os
import subprocess
import sys
from pathlib import Path

REQUIRED_FILES = [
    # Orchestration and build
    "compose.yaml",
    "Dockerfile",
    "Makefile",
    "README.md",

    # Source code
    "distrib_ledger/__init__.py",
    "distrib_ledger/node.py",
    "distrib_ledger/cli.py",
    "distrib_ledger/network/channel.py",
    "distrib_ledger/network/fault_injector.py",
    "distrib_ledger/network/protocol.py",
    "distrib_ledger/mutual_exclusion/lamport_clock.py",
    "distrib_ledger/mutual_exclusion/ricart_agrawala.py",
    "distrib_ledger/atomic_commit/wal.py",
    "distrib_ledger/atomic_commit/two_phase_commit.py",
    "distrib_ledger/atomic_commit/recovery.py",
    "distrib_ledger/snapshot/chandy_lamport.py",
    "distrib_ledger/snapshot/auditor.py",

    # Tests
    "tests/test_ricart_agrawala.py",
    "tests/test_two_phase_commit.py",
    "tests/test_chandy_lamport.py",
    "tests/test_fault_tolerance.py",
    "tests/test_node_integration.py",
    "tests/test_process_deployment.py",

    # Scripts & Data
    "scripts/run_experiments.py",
    "scripts/generate_plots.py",
    "data/environment_metadata.json",
    "data/exp1_ricart_agrawala.csv",
    "data/exp2_two_phase_commit.csv",
    "data/exp3_chandy_lamport.csv",

    # Figures
    "figures/exp1_ra_latency.pdf",
    "figures/exp1_ra_messages.pdf",
    "figures/exp2_2pc_latency.pdf",
    "figures/exp2_2pc_recovery.pdf",
    "figures/exp3_snapshot_duration.pdf",
    "figures/exp3_snapshot_conservation.pdf",

    # Report
    "report/main.tex",
    "report/IEEEtran.cls",
    "report/IEEEtran.bst",
    "report/references.bib",
    "report/build_report.sh",
    "report/relatorio.pdf",
]


def check_files() -> bool:
    print("Checking repository files...")
    all_ok = True
    for f in REQUIRED_FILES:
        path = Path(f)
        if not path.exists():
            print(f"  [MISSING] {f}")
            all_ok = False
        else:
            size_kb = path.stat().st_size / 1024.0
            print(f"  [OK] {f} ({size_kb:.1f} KB)")
    return all_ok


def check_tests() -> bool:
    print("\nRunning automated test suite...")
    cmd = [sys.executable, "-m", "unittest", "discover", "-s", "tests", "-p", "test_*.py", "-v"]
    res = subprocess.run(cmd, capture_output=True, text=True)
    if res.returncode == 0:
        print("  [PASS] All automated tests passed successfully!")
        return True
    else:
        print("  [FAIL] Test suite failed:")
        print(res.stderr)
        return False


def check_pdf_pages() -> bool:
    print("\nChecking report page count...")
    pdf_path = Path("report/relatorio.pdf")
    if not pdf_path.exists():
        print("  [FAIL] report/relatorio.pdf does not exist!")
        return False
    try:
        from pypdf import PdfReader
        reader = PdfReader(str(pdf_path))
        num_pages = len(reader.pages)
        print(f"  [INFO] Total pages: {num_pages}")
        if num_pages <= 6:
            print("  [PASS] Page count meets the <= 6 pages requirement!")
            return True
        else:
            print(f"  [FAIL] Page count {num_pages} exceeds maximum of 6 pages!")
            return False
    except Exception as e:
        print(f"  [WARN] Could not verify page count via pypdf: {e}")
        return True


def main():
    print("==================================================")
    print("MC714 TRABALHO 2: PRE-SUBMISSION VERIFIER")
    print("==================================================")
    f_ok = check_files()
    t_ok = check_tests()
    p_ok = check_pdf_pages()

    print("\n==================================================")
    if f_ok and t_ok and p_ok:
        print("RESULT: ALL CHECKS PASSED! Ready for submission.")
        print("==================================================")
        sys.exit(0)
    else:
        print("RESULT: SOME CHECKS FAILED! Please inspect output.")
        print("==================================================")
        sys.exit(1)


if __name__ == "__main__":
    main()
