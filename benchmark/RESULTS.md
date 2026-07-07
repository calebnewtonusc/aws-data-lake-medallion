# Query Optimization Benchmark Results

- Dataset: 20,000,000 synthetic transaction rows spanning 730 days
- Queries: 5 representative analytical queries
- Timing: median of 3 timed wall-clock runs per query after one warm-up
- Three layouts on the same data and queries, same Spark config:
  - Baseline A: raw unoptimized CSV (row format, unpartitioned, all columns, no stats)
  - Baseline B: unoptimized Parquet (columnar, but unpartitioned, unsorted, all columns)
  - Optimized: month-partitioned, compacted, column-pruned, sorted Parquet with row-group stats

## Two measured reductions

- **Format + layout (CSV to optimized Parquet): 92.14%** (12.72x). Includes the row-to-columnar file-format change.
- **Query optimization only (unoptimized Parquet to optimized Parquet): 36.14%** (1.57x). File format held constant, so this is attributable purely to partitioning, column pruning, file compaction, predicate pushdown, and SQL rewrites.

Totals across all queries: CSV 79.1065s, unoptimized Parquet 9.7366s, optimized 6.2177s.

## Query optimization only: unoptimized Parquet vs optimized Parquet

This is the honest number for the resume bullet's listed techniques.

| Query | Technique | Unopt. Parquet (s) | Optimized (s) | Speedup | Reduction |
| --- | --- | ---: | ---: | ---: | ---: |
| q1_date_range_agg | Date-range aggregation (partition pruning + stats) | 1.1785 | 0.6708 | 1.76x | 43.08% |
| q2_status_filter_group | Status filter + group by (column pruning + pushdown) | 1.7377 | 1.5176 | 1.15x | 12.67% |
| q3_single_day_lookup | Single-day point lookup (partition pruning) | 0.7067 | 0.2982 | 2.37x | 57.8% |
| q4_join_filter_before_join | Join with filter-before-join rewrite | 4.7046 | 2.6639 | 1.77x | 43.38% |
| q5_wide_scan_agg | Wide-table scan aggregation (column pruning) | 1.4091 | 1.0671 | 1.32x | 24.27% |

## Format + layout: raw CSV vs optimized Parquet

| Query | Technique | Raw CSV (s) | Optimized (s) | Reduction |
| --- | --- | ---: | ---: | ---: |
| q1_date_range_agg | Date-range aggregation (partition pruning + stats) | 18.6846 | 0.6708 | 96.41% |
| q2_status_filter_group | Status filter + group by (column pruning + pushdown) | 11.4082 | 1.5176 | 86.7% |
| q3_single_day_lookup | Single-day point lookup (partition pruning) | 14.2221 | 0.2982 | 97.9% |
| q4_join_filter_before_join | Join with filter-before-join rewrite | 15.3673 | 2.6639 | 82.67% |
| q5_wide_scan_agg | Wide-table scan aggregation (column pruning) | 19.4243 | 1.0671 | 94.51% |

