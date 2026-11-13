#!/usr/bin/env python3
"""
Entrypoint script for self-balancing-router experiment orchestrator.
"""

from pathlib import Path
import sys

# Ensure repository root is on sys.path
repo_root = Path(__file__).resolve().parent.parent
if str(repo_root) not in sys.path:
    sys.path.insert(0, str(repo_root))

from experiments.orchestrator.cli import main

if __name__ == "__main__":
    sys.exit(main())

