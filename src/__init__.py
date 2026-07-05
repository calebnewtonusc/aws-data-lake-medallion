"""Medallion architecture data lake for e-commerce order events.

This package implements a bronze, silver, and gold layered data lake on
AWS S3, built with PySpark and boto3. It runs fully offline using the moto
library to mock S3, and documents the path to a real AWS deployment.
"""

__all__ = ["config", "storage", "bronze", "silver", "gold", "quality", "run_pipeline"]
