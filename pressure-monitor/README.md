# HMS Metadata Pressure Monitor (CDP 7.1.9)

`metadata_pressure_monitor.py` finds what is putting excessive load on the Hive
Metastore (HMS) and its MySQL backend. It reads HMS logs and, for each problem,
prints a concrete remediation (batching, partition pruning, breaking loops,
delegation-token store, stats tuning, ...).

Fully standalone: **Python 3.6+, standard library only.** Nothing to install,
and it needs **no cluster connection** - it just reads log files.

## What it answers

- Which HMS methods dominate **call volume** and **total time**?
- Which **Kerberos IDs** (`ugi`) and **client IPs** generate the pressure?
- Which **HMS host** instances are hottest (fleet skew)?
- Which **tables** are hot?
- How does load **trend over time** - where are the storms?
- Which patterns look like **loops / repetitive calls**, and how to fix them?

## The 30-second version

```bash
# Analyze a directory of HMS logs (gzip ok), last 24h, and write a report
./metadata_pressure_monitor.py report --paths /var/log/hive --last 24h --output-dir ./out

# Read ./out/pressure_report_<ts>.txt   (JSON + CSVs are written alongside it)
```

You can pass files, globs (`'/logs/**/HIVEMETASTORE-*.gz'`), or directories
(scanned recursively). Add `--exclude-methods get_token` to hide
delegation-token noise, `--bucket 15m` to change the trend granularity, and
`--top 30` for more rows.

## Inputs

Cloudera HMS role logs (`hadoop-cmf-hive-HIVEMETASTORE-<host>.log.out...`) or
`hms-audit` / `hms-perf` logs. Two line shapes are parsed:

- **PERFLOG** (method + duration): `</PERFLOG method=get_table_req ... duration=2026 ... threadId=925 ...>`
- **AUDIT** (ugi + ip + cmd): `HiveMetaStore.audit: ... ugi=user/host@REALM ip=10.0.0.1 cmd=get_partitions_by_expr : tbl=cat.db.table ...`

The HMS hostname is read from the log filename for fleet attribution; the
Kerberos principal is split into short user / principal / realm.

## Modes

| Mode      | Purpose                                                     | Writes |
|-----------|-------------------------------------------------------------|--------|
| `report`  | Ad-hoc analysis of historical logs (fleet-wide, gz ok).     | TXT + JSON + CSVs |
| `monitor` | Scheduled incremental tail with de-duplicated alerts.       | stdout + optional email + state |

### report outputs (in `--output-dir`)

- `pressure_report_<ts>.txt` - executive summary: totals, top methods by volume
  and by time, per-category pressure, top Kerberos IDs, HMS host distribution,
  hot tables, an ASCII load-trend chart, and the findings + resolutions.
- `pressure_report_<ts>.json` - the same data, structured.
- CSVs: `by_method`, `by_ugi`, `by_host`, `by_bucket`, `by_ip`, `hot_tables`,
  `findings`.

### monitor (scheduled)

```bash
# 1) Copy the example config and edit log_paths / email
cp metadata_pressure_monitor.config.example.json metadata_pressure_monitor.config.json

# 2) One pass (cron); preview with --dry-run
./metadata_pressure_monitor.py monitor --config metadata_pressure_monitor.config.json --once
```

It incrementally tails **uncompressed** live logs from stored byte offsets,
keeps a rolling window in a small JSONL event store, and emits new findings (at
or above `alert_severity`) to stdout and, optionally, email. Alerts are
de-duplicated across runs. Without `--once` it loops every `interval_seconds`.

## Configuration

Copy [`metadata_pressure_monitor.config.example.json`](metadata_pressure_monitor.config.example.json)
to `metadata_pressure_monitor.config.json` and edit it. Key sections:
`log_paths`, `window` (`bucket`), `top_n`, `exclude_methods`, `alert_severity`,
`thresholds`, `event_store`, `email`. Every threshold has a built-in default, so
report mode can run with just `--paths` and no config at all.

> The real `metadata_pressure_monitor.config.json` (which may hold SMTP
> credentials) is git-ignored. Only the `.example.json` is tracked.

## Method categories

Pressure is summarized by category: `AUTH` (delegation tokens),
`METADATA_READ`, `PARTITION_READ`, `PARTITION_WRITE`, `STATS`, `CONSTRAINTS`,
`DDL`, `CACHE`, `OTHER`.

## Findings and remediation catalog

| Finding | Trigger | Remediation summary |
|---------|---------|---------------------|
| `delegation_token_storm` | AUTH (`get_token`, ...) is a large share of calls | Move token store off the DB (`hive.cluster.delegation.token.store.class` -> ZooKeeper); reuse HMS connections/tokens (pooling) instead of per-task re-auth; raise token lifetime. |
| `fetch_all_partitions` | High unfiltered `get_partitions*` | Enable partition pruning (`metastore.limit.partition.request`, Spark `spark.sql.hive.metastorePartitionPruning=true`); use `get_partitions_by_expr`/`by_filter`; add partition predicates. |
| `partition_write_burst` | Many `add/drop/alter_partition*` | Batch multi-partition ADD / dynamic-partition INSERT; avoid MSCK loops; use the orphan-cleanup tool for dangling drops. |
| `stats_storm` | Heavy `get_aggr_stats_for` / `*_statistics` / `*_column_statistics` | Review `hive.stats.fetch.partition.stats`; enable aggregate stats cache; ANALYZE off-peak; curate stale stats. |
| `constraint_amplification` | Constraint lookups a large share of calls | Inherent per table-open in Hive 3; reduce table-open churn via HS2 metastore caching and session reuse. |
| `repetitive_loop` | Same (ugi, ip, method, table) repeats rapidly | Break the client loop, cache/broadcast metadata once per job, add partition filters; correlate ugi/ip/time to the Spark app / HS2 queryId. |
| `dominant_method` | A single non-auth method exceeds a call-share threshold | Method-specific advice (pruning, caching, batching, stats). |
| `heavy_kerberos_id` | One `ugi` exceeds a call-share threshold | Work with the owning team to batch/cache; pool connections for fan-out service accounts. |
| `heavy_client_ip` | One IP exceeds a call-share threshold | Trace to the responsible service and apply batching/caching. |
| `hms_host_skew` | One HMS handles most calls | Check client load-balancing / `hive.metastore.uris` ordering. |
| `load_spike` | A time bucket far exceeds the window average | Inspect that interval's top methods/IDs to find the driving job. |
| `slow_method` | High average PERFLOG duration | Method-specific advice plus check MySQL contention / indexes on backing tables. |

## Scheduling

```cron
# Every 5 min incremental monitor with alerts
*/5 * * * * /opt/hive-tools/pressure-monitor/metadata_pressure_monitor.py monitor \
    --config /opt/hive-tools/pressure-monitor/metadata_pressure_monitor.config.json --once \
    >> /var/log/hms_pressure/monitor.log 2>&1

# Daily fleet report over archived logs
0 6 * * * /opt/hive-tools/pressure-monitor/metadata_pressure_monitor.py report \
    --paths /var/log/hive/archive --last 24h --output-dir /var/log/hms_pressure/daily \
    >> /var/log/hms_pressure/report.log 2>&1
```

## Exit codes

`0` success, `1` runtime error, `2` findings at/above the alert severity were
produced (useful for cron alerting), `3` soft warning (no lines matched).

## Notes

- Read-only: it never changes the cluster. Recommendations describe config/DML
  changes; it does not apply them.
- Memory is bounded (streaming scan, top-N, reservoir-sampled percentiles), so
  it handles multi-GB fleet log sets.
