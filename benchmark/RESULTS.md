# Query Optimization Benchmark Results

- Dataset: 20,000,000 synthetic transaction rows spanning 730 days
- Queries: 5 representative analytical queries
- Timing: median of 3 timed wall-clock runs per query after one warm-up
- Baseline layout: a single large unpartitioned CSV, all columns, no columnar stats
- Optimized layout: month-partitioned, compacted, column-pruned, sorted Parquet with row-group stats

## Overall runtime reduction: 94.54%

Total baseline time 23.3834s vs optimized 1.2771s across all queries (overall speedup 18.31x).

| Query | Technique | Baseline (s) | Optimized (s) | Speedup | Reduction |
| --- | --- | ---: | ---: | ---: | ---: |
| q1_date_range_agg | Date-range aggregation (partition pruning + stats) | 5.1266 | 0.1023 | 50.12x | 98.0% |
| q2_status_filter_group | Status filter + group by (column pruning + pushdown) | 4.0546 | 0.2891 | 14.03x | 92.87% |
| q3_single_day_lookup | Single-day point lookup (partition pruning) | 4.4786 | 0.0634 | 70.59x | 98.58% |
| q4_join_filter_before_join | Join with filter-before-join rewrite | 5.3286 | 0.532 | 10.02x | 90.02% |
| q5_wide_scan_agg | Wide-table scan aggregation (column pruning) | 4.395 | 0.2902 | 15.14x | 93.4% |

