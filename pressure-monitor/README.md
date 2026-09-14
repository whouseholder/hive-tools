# HMS Metadata Pressure Monitor (CDP 7.1.9)

`metadata_pressure_monitor.py` finds what is putting excessive load on the Hive
Metastore (HMS) and its MySQL backend. It reads HMS logs and, for each problem,
prints a concrete remediation (batching, partition pruning, breaking loops,
delegation-token store, stats tuning, ...). It also **correlates HMS methods to
the Hive operations behind them** (`DROP PARTITION`, `ALTER TABLE`, ...) and
names the **top contributors** (users, IPs, tables) and up to a **Top-5 likely
source per operation with a confidence score**.

Fully standalone: **Python 3.6+, standard library only.** Nothing to install,
and it needs **no cluster connection** - it just reads log files.

Built for repeated, low-latency runs:

- **Fast:** cheap line pre-filtering, a cached slice-based timestamp parser,
  file-mtime pruning, and a binary seek to the start of the window mean a
  `--last 1h` scan of a multi-GB log finishes in well under a second.
- **Incremental (`--state-dir`):** remembers byte offsets per log file and
  parses **only new bytes** on the next run - re-runs cost only the delta.
- **Fleet-wide without gathering logs (`--hosts` + `agg`):** fans out over SSH
  so each HMS node parses its **own** logs locally and returns a small JSON
  aggregate; the edge node merges them into one consolidated report.

## What it answers

- Which HMS methods dominate **call volume** and **total time**?
- Which **Hive operations** (`DROP PARTITION`, `ALTER TABLE`, `CREATE TABLE`,
  ...) are those methods coming from, and **who/what** is driving each one
  (top user + table, with a **confidence score** and up to a **Top-5**)?
- Which **Kerberos IDs** (`ugi`) and **client IPs** generate the pressure?
- Which **HMS host** instances are hottest (fleet skew)?
- Which **tables** are hot?
- How does load **trend over time** - where are the storms, and **which method +
  user dominated each bucket**?
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
| `report`  | Ad-hoc analysis of historical logs (single node, gz ok). Can fan out over SSH (`--hosts`) and re-run incrementally (`--state-dir`). | TXT + JSON + CSVs |
| `monitor` | Scheduled incremental tail with de-duplicated alerts.       | stdout + optional email + state |
| `agg`     | Worker mode: parse local logs and emit one JSON aggregate on stdout. Normally invoked **for** you by `report --hosts` over SSH; can also be run by hand on a node. | JSON (stdout) |

### report outputs (in `--output-dir`)

- `pressure_report_<ts>.txt` - executive summary: totals, top methods by volume
  and by time, per-category pressure, top Kerberos IDs, HMS host distribution,
  hot tables, a **TOP CONTRIBUTORS TO PRESSURE** section, a **WORKLOAD
  CORRELATION** section (HMS method -> Hive operation -> likely source with a
  confidence score), an ASCII **LOAD TREND** chart annotated with the top method
  and user per bucket, and the findings + resolutions.
- `pressure_report_<ts>.json` - the same data, structured (includes
  `top_contributors` and `workload_correlation`).
- CSVs: `by_method`, `by_ugi`, `by_host`, `by_bucket`, `by_ip`, `hot_tables`,
  `findings`, plus `top_contributors_<ts>.csv` and (when operations are
  attributable) `correlation_<ts>.csv`.

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

## Faster re-runs: incremental `report` (`--state-dir`)

For a `report` you run repeatedly (e.g. every 15 min), point it at a state
directory and it will parse **only the new bytes** appended since last time:

```bash
./metadata_pressure_monitor.py report --paths /var/log/hive --last 2h \
    --state-dir /var/lib/hms_pressure/state --retention-days 3 \
    --output-dir ./out
```

- Byte offsets are keyed by file **inode + size**, so log rotation is detected
  and a rotated file is re-read from the top.
- Parsed events are cached in **hour-sharded JSONL** under `--state-dir`; each
  run rebuilds the requested window from shards + the new tail, then prunes
  shards older than `--retention-days`.
- The very first run does a full parse of the window; every run after that is
  delta-only. Aggregates are **identical** to a fresh full scan of the same
  window - it is a pure speed optimization, not an approximation.

## Fleet-wide without collecting logs (remote fan-out)

Instead of copying logs from all 42 HMS nodes to one place, let each node parse
its **own** logs and return only a small JSON aggregate, then merge on the edge:

```bash
./metadata_pressure_monitor.py report \
    --hosts hms01,hms02,hms03,...,hms42 \
    --remote-paths '/var/log/hive/hadoop-cmf-hive-HIVEMETASTORE-*.log.out*' \
    --last 2h --ssh-user hive --parallel 12 \
    --remote-python /usr/bin/python3 --output-dir ./out
# or: --hosts-file /etc/hive/hms_hosts.txt   (one host per line, '#' comments ok)
```

How it works (all standard tooling - just `ssh` + `python3` on each host):

1. The edge resolves **one** time window so every host analyzes the same
   interval, then streams this very script to each host's `python3` over SSH and
   runs it in `agg` (worker) mode against `--remote-paths`.
2. Each host parses locally and prints one compact JSON aggregate (counts,
   per-method timers, top-N tables/users/IPs, per-bucket load, loop peaks) to
   stdout - **no raw logs cross the network**.
3. The edge merges all partials and writes the same consolidated TXT/JSON/CSV
   report, with per-host attribution preserved. Hosts are queried
   `--parallel` at a time; an unreachable host is logged and skipped.

Requirements: key-based SSH from the edge to each HMS host and a `python3` on
each host (`--remote-python` to set the path). Nothing is installed remotely.

## Top contributors & workload correlation

Two report sections turn "which method is hot" into "who/what to go fix":

- **TOP CONTRIBUTORS TO PRESSURE** - the leading methods, users (`ugi`), IPs,
  and tables by call share, each with its peak bucket; users and methods also
  show their top methods / mapped Hive operation.
- **WORKLOAD CORRELATION** - maps each hot HMS method to the **Hive operation**
  behind it (e.g. `add_partitions`->`ADD PARTITION`, `drop_table`->`DROP TABLE`,
  `alter_table*`->`ALTER TABLE`) and lists up to **5 likely sources** per
  operation (user + table + dominant IP) with a **confidence score**.

  Confidence is honest by design: HMS audit lines share no id with HS2, so a
  pure-HMS attribution is a ranked guess (it never claims 100% on its own). If
  you also point the tool at client logs, a time-and-table match raises
  confidence and attaches the actual `queryId` / `appId` (and SQL when present):

  ```bash
  ./metadata_pressure_monitor.py report --paths /var/log/hive --last 2h \
      --hs2-paths '/var/log/hive/hadoop-cmf-hive-HIVESERVER2-*.log.out*' \
      --yarn-paths '/var/log/hadoop-yarn/*.log*' \
      --spark-paths '/var/log/spark*/*.log*' \
      --correlate-top 8 --sources-per-op 5 --output-dir ./out
  ```

  A source is only marked `CONFIRMED` when a client query/app is matched on
  table **and** time window; otherwise you get the percentage. Full detail lands
  in `correlation_<ts>.csv`.

## Configuration

Copy [`metadata_pressure_monitor.config.example.json`](metadata_pressure_monitor.config.example.json)
to `metadata_pressure_monitor.config.json` and edit it. Key sections:
`log_paths`, `window` (`bucket`), `top_n`, `exclude_methods`, `alert_severity`,
`thresholds`, `event_store`, `email`. Every threshold has a built-in default, so
report mode can run with just `--paths` and no config at all.

The `report`-only capabilities above - `--state-dir` (incremental), `--hosts` /
`--remote-paths` (fan-out), and `--hs2-paths` / `--yarn-paths` / `--spark-paths`
(correlation) - are command-line flags, not config keys. A `report --config`
run still reuses `log_paths` (and, if set, `remote_paths`) from the same file.

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

# Every 15 min incremental report (delta-only after the first run)
*/15 * * * * /opt/hive-tools/pressure-monitor/metadata_pressure_monitor.py report \
    --paths /var/log/hive --last 2h \
    --state-dir /var/lib/hms_pressure/state --retention-days 3 \
    --output-dir /var/log/hms_pressure/rolling >> /var/log/hms_pressure/report.log 2>&1

# Daily fleet report over archived logs (single node)
0 6 * * * /opt/hive-tools/pressure-monitor/metadata_pressure_monitor.py report \
    --paths /var/log/hive/archive --last 24h --output-dir /var/log/hms_pressure/daily \
    >> /var/log/hms_pressure/report.log 2>&1

# Daily fleet-wide report over all HMS nodes (each parses its own logs)
30 6 * * * /opt/hive-tools/pressure-monitor/metadata_pressure_monitor.py report \
    --hosts-file /etc/hive/hms_hosts.txt \
    --remote-paths '/var/log/hive/hadoop-cmf-hive-HIVEMETASTORE-*.log.out*' \
    --last 24h --ssh-user hive --parallel 12 \
    --output-dir /var/log/hms_pressure/fleet >> /var/log/hms_pressure/fleet.log 2>&1
```

## Exit codes

`0` success, `1` runtime error, `2` findings at/above the alert severity were
produced (useful for cron alerting), `3` soft warning (no lines matched).

## Notes

- Read-only: it never changes the cluster (and the remote fan-out only *reads*
  logs on each host). Recommendations describe config/DML changes; it does not
  apply them.
- Memory is bounded (streaming scan, top-N, reservoir-sampled percentiles), so
  it handles multi-GB fleet log sets.
- The `--state-dir` store holds only compact parsed events for the last
  `--retention-days` (hour-sharded JSONL), and is pruned on every run; it is
  safe to delete at any time (the next run simply repopulates it).
- Correlation is best-effort attribution, not proof: treat the confidence score
  and the Top-5 as ranked leads. Provide HS2/YARN/Spark logs to turn the best
  matches into `CONFIRMED` links with real `queryId`/`appId`.
