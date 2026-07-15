# Query Optimization Benchmark Results

All numbers below are measured, not assumed. Reproduce with `python -m benchmark.run_benchmark`.

- Data: four source domains (listings, reviews, bookings, transactions) spanning 730 days
- Fact scale: 60,000,000 transactions, 12,000,000 bookings, 12,000,000 reviews, 50,000 listings
- Workloads: 22 multi-table SQL queries (joins across the four domains, aggregations, and window functions: running totals, dense_rank top-N, moving averages, lag, cumulative share)
- Timing: median of 3 timed wall-clock runs per query after one warm-up
- Two layouts of the same data, same Spark config (file format held constant, both Parquet):
  - Baseline: unoptimized Parquet (unpartitioned, unsorted, all columns, few big files, no gold table)
  - Optimized: month-partitioned, compacted, column-pruned, sorted Parquet + a denormalized pre-aggregated gold star table

## Headline: query optimization only (file format held constant)

**Overall runtime reduction: 73.94%** (3.84x speedup), total across all 22 queries 43.9939s down to 11.4626s.

Both sides are Parquet, so the file format is held constant and this reduction is attributable purely to the query-optimization techniques: partition pruning, predicate pushdown, column pruning, file compaction, window-function rewrites, and optimized table design (the pre-aggregated gold star table). The optimized layout is also 4.7x smaller on disk (6.74 GB down to 1.42 GB) from column pruning and compaction.

## Per-query results

| Query | Technique | Baseline (s) | Optimized (s) | Speedup | Reduction |
| --- | --- | ---: | ---: | ---: | ---: |
| q01_date_range_revenue | partition pruning + stats | 0.9159 | 0.3151 | 2.91x | 65.6% |
| q02_method_success | column pruning + pushdown | 2.6623 | 1.8858 | 1.41x | 29.17% |
| q03_single_day | partition pruning | 0.7235 | 0.2201 | 3.29x | 69.57% |
| q04_txn_listing_join | join + partition prune | 1.2685 | 0.4031 | 3.15x | 68.22% |
| q05_txn_booking_join | join rewrite + prune | 2.7693 | 1.0516 | 2.63x | 62.03% |
| q06_three_way_join | multi-domain join + prune | 3.858 | 2.3061 | 1.67x | 40.22% |
| q07_review_listing_join | join + partition prune | 0.5612 | 0.3018 | 1.86x | 46.23% |
| q08_bookings_by_month | partition pruning (bookings) | 0.228 | 0.1238 | 1.84x | 45.7% |
| q09_running_total | window: running total + gold | 2.6525 | 0.0978 | 27.12x | 96.31% |
| q10_topn_per_month | window: dense_rank top-N + gold | 8.8428 | 0.8121 | 10.89x | 90.82% |
| q11_moving_avg_3m | window: moving avg + gold | 2.3643 | 0.119 | 19.87x | 94.97% |
| q12_nbhd_share | window: ratio + gold | 3.2506 | 0.4052 | 8.02x | 87.53% |
| q13_rank_nbhd | window: rank + join + prune | 2.3188 | 0.45 | 5.15x | 80.59% |
| q14_mom_delta | window: lag + gold | 1.1661 | 0.0838 | 13.92x | 92.82% |
| q15_pareto_share | window: cumulative + gold | 0.9685 | 0.1996 | 4.85x | 79.39% |
| q16_success_rate | conditional agg + prune | 0.5602 | 0.2087 | 2.68x | 62.76% |
| q17_conversion | join + partition prune | 0.2278 | 0.1466 | 1.55x | 35.62% |
| q18_reviews_vs_revenue | two-fact join + prune | 1.7388 | 0.5327 | 3.26x | 69.36% |
| q19_wide_scan | column pruning | 0.7187 | 1.1755 | 0.61x | -63.56% |
| q20_avg_daily_rev | join + prune + agg | 2.0343 | 0.271 | 7.51x | 86.68% |
| q21_gold_preagg | optimized table design (pre-agg) | 2.7678 | 0.1315 | 21.05x | 95.25% |
| q22_moving_avg_7d | window: moving avg + prune | 1.3961 | 0.2218 | 6.29x | 84.11% |

| **Total** | | **43.9939** | **11.4626** | **3.84x** | **73.94%** |

### Techniques demonstrated

- **Partition pruning**: date-bounded queries prune whole month directories instead of scanning the full fact.
- **Predicate pushdown**: rows are sorted within each partition, so tight row-group min/max stats let the Parquet reader skip row groups that cannot match a filter. The unsorted baseline has wide min/max ranges that skip little.
- **Column pruning**: reading only the columns a query needs, rather than every column of a wide row, so queries touch a fraction of the bytes.
- **File compaction**: the optimized data is one right-sized file per partition, avoiding the small-file tax.
- **Window functions**: running totals, dense_rank top-N per group, moving averages, lag deltas, and cumulative share run over the partitioned, sorted, pre-aggregated gold rather than re-scanning the raw fact.
- **Optimized table design**: a denormalized, pre-aggregated gold star table (revenue by listing and month, already joined to the listing dimension) that pre-agg and top-N queries read instead of re-joining and re-scanning the fact.

