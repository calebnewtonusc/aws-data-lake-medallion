"""Shared pytest setup for the medallion data lake test suite.

Spark launches Python worker processes and, by default, resolves the worker
interpreter from PATH rather than from the driver interpreter running the
tests. On a machine with more than one Python on PATH that produces a
PYTHON_VERSION_MISMATCH failure. Pinning both PYSPARK_PYTHON and
PYSPARK_DRIVER_PYTHON to the current interpreter before any SparkSession is
created keeps the driver and workers on the same interpreter.
"""

from __future__ import annotations

import os
import sys

os.environ.setdefault("PYSPARK_PYTHON", sys.executable)
os.environ.setdefault("PYSPARK_DRIVER_PYTHON", sys.executable)
