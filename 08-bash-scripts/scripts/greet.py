"""Tiny script called by call_script_dag.py from another folder."""

import platform
import sys

print(f"Hello from greet.py, running on {platform.node()} with Python {sys.version.split()[0]}")
