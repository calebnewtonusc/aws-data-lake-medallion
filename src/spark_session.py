"""PySpark session factory for the local medallion pipeline.

The Python executable used by the driver and the workers is pinned to the
current interpreter so Spark never picks up a different Python from the PATH.
The session runs in local mode with a single warehouse directory kept inside
the project so it is easy to clean up and ignore in git.
"""

from __future__ import annotations

import os
import sys
from pathlib import Path

from pyspark.sql import SparkSession

# Pin the driver and executor Python to the interpreter running this code.
os.environ.setdefault("PYSPARK_PYTHON", sys.executable)
os.environ.setdefault("PYSPARK_DRIVER_PYTHON", sys.executable)


def build_spark(app_name: str = "medallion-data-lake") -> SparkSession:
    """Create or reuse a local SparkSession for the pipeline.

    Args:
        app_name: Spark application name shown in the UI and logs.

    Returns:
        A configured local-mode SparkSession.
    """
    warehouse = Path("spark-warehouse").resolve()
    return (
        SparkSession.builder.appName(app_name)
        .master("local[*]")
        .config("spark.sql.warehouse.dir", str(warehouse))
        .config("spark.sql.shuffle.partitions", "8")
        .config("spark.ui.enabled", "false")
        .config("spark.sql.sources.partitionOverwriteMode", "dynamic")
        .getOrCreate()
    )
