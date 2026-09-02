# Hive Orphan Object Cleanup (CDP 7.1.9 + Isilon)

`hive_orphan_cleanup.py` finds and removes **orphaned** Hive objects: tables or
partitions that still exist in the Hive Metastore (HMS, backed by MySQL) but
whose storage `LOCATION` is gone from the filesystem (e.g. Isilon / HDFS).

Cleanup uses only **standard Hive DML** (`DROP TABLE`,
`ALTER TABLE ... DROP PARTITION`, `MSCK REPAIR ... DROP PARTITIONS`). Dropping an
object cascades away its rows in the metastore backing tables
(`TBLS`/`PARTITIONS`/`SDS`/`SERDE_PARAMS`/`PARTITION_PARAMS`/`PART_COL_STATS`,
...) - which is how it reduces object count on the MySQL side.

## Requirements

- A CDP edge/gateway node with `beeline` and `hdfs` on `PATH`.
- Python 3.6+ (standard library only - nothing to install).
- A valid Kerberos ticket (`kinit`), or pass `--keytab`/`--principal` and the
  tool will `kinit` for you.

## The 30-second version

```bash
# 1) See what's orphaned (READ-ONLY, changes nothing)
./hive_orphan_cleanup.py report \
    --jdbc-url 'jdbc:hive2://hs2.example.com:10000/default;principal=hive/_HOST@REALM' \
    --output-dir ./out

# 2) Review ./out/summary_<ts>.txt and ./out/orphans_<ts>.csv

# 3a) Clean everything found (preview first, then execute)
./hive_orphan_cleanup.py clean --output-dir ./out            # dry-run
./hive_orphan_cleanup.py clean --output-dir ./out --execute --yes

# 3b) OR clean only the rows you keep in an edited report
./hive_orphan_cleanup.py apply --report ./out/orphans_<ts>.csv --execute --yes
```

Nothing is ever changed without `--execute`.

## Modes

| Mode     | What it does                                                        | Mutates? |
|----------|--------------------------------------------------------------------|----------|
| `report` | Enumerate HMS, check storage, write CSV + JSON + human summary.    | No |
| `clean`  | Detect **and** clean. Dry-run by default; needs `--execute`.       | With `--execute` |
| `apply`  | Clean exactly the objects listed in a prior (optionally trimmed) report. | With `--execute` |

## How detection works

1. **Enumerate** all table and partition locations in one shot from the Hive 3
   `sys.*` views (`sys.TBLS`/`DBS`/`SDS`/`PARTITIONS`), falling back to
   `SHOW DATABASES` -> `SHOW TABLES` -> `DESCRIBE FORMATTED` / `SHOW PARTITIONS`.
2. **Check storage** for each location with `hdfs dfs`, grouping partitions per
   table to minimize calls.
3. **Classify** an object as orphaned only if its location is missing. The
   `--require-parent-exists` heuristic (on by default) additionally requires the
   parent directory to be reachable, so a transient Isilon/NameNode outage does
   not produce mass false positives.

## Reports

Each run writes to `--output-dir`:

- `orphans_<ts>.csv` - one row per orphaned object. Columns:
  `object_type, db, table, tbl_type, partition_spec, location, reason, detected_at`.
- `orphans_<ts>.json` - the same data plus scan stats and recommendations.
- `summary_<ts>.txt` - counts, per-table breakdown, an estimate of metastore
  rows reclaimed, and recommendations.

`clean`/`apply` also write `audit_<ts>.log` (one line per statement executed).

### Trimming a report for `apply`

Open a prior `orphans_*.csv`, delete the rows you do **not** want to act on
(keep the header), save, then run `apply --report <file> --execute`. Every
remaining row is re-verified against storage immediately before its DROP unless
you pass `--skip-reverify`.

## MySQL-load-reducing housekeeping (opt-in)

Report-only in `report` mode; executed in `clean` with `--execute`:

- **`--set-retention DAYS`** - on partitioned **external** tables, sets
  `discover.partitions=true` and `partition.retention.period=<DAYS>d` so Hive
  auto-expires old partition metadata.
- **`--compact`** - requests `ALTER TABLE ... COMPACT 'major'` on ACID tables;
  after the Compaction Cleaner runs, this shrinks `TXN_COMPONENTS`,
  `COMPLETED_TXN_COMPONENTS`, and `WRITE_SET`.
- **Stats bloat** - no separate action needed: dropping orphaned
  partitions/tables cascades their `PART_COL_STATS`/`TAB_COL_STATS` rows.

The report also emits **config-level recommendations** applied via Cloudera
Manager (not by this tool), e.g. lowering
`hive.metastore.event.db.listener.timetolive` to prune `NOTIFICATION_LOG`. The
tool never issues raw `DELETE` against metastore tables.

## Key options

| Option | Purpose |
|--------|---------|
| `--jdbc-url` | HiveServer2 JDBC URL for beeline (or set `HOC_JDBC_URL`). |
| `--beeline-path` / `--hdfs-path` | Override CLI binary locations. |
| `--keytab` / `--principal` | Optional pre-run `kinit`. |
| `--databases` / `--exclude-databases` / `--tables` / `--table-regex` | Scope the scan. |
| `--require-parent-exists` / `--no-require-parent-exists` | Toggle the transient-outage safety heuristic (default on). |
| `--max-workers` | Parallel `hdfs` existence checks (default 8). |
| `--execute` | Actually run DML (otherwise dry-run). |
| `--yes` | Skip the interactive confirmation (required for cron/non-TTY). |
| `--limit N` | Cap the number of per-table batches acted on. |
| `--use-msck` | Use `MSCK REPAIR ... DROP PARTITIONS` per table instead of explicit `ALTER ... DROP PARTITION`. |

Exit codes: `0` success, `1` runtime error, `2` some DML statements failed,
`3` soft warning (empty report / aborted without `--yes`).

## Scheduling

```cron
# Weekly Mon 02:00 read-only report
0 2 * * 1 /opt/hive-tools/orphan-cleanup/hive_orphan_cleanup.py report \
    --keytab /etc/security/keytabs/hive.keytab --principal hive/edge01@REALM \
    --jdbc-url 'jdbc:hive2://hs2:10000/default;principal=hive/_HOST@REALM' \
    --output-dir /var/log/hms_orphans >> /var/log/hms_orphans/cron.log 2>&1

# Monthly 1st 03:00 execute, after human review of the weekly reports
0 3 1 * * /opt/hive-tools/orphan-cleanup/hive_orphan_cleanup.py clean --execute --yes \
    --keytab /etc/security/keytabs/hive.keytab --principal hive/edge01@REALM \
    --output-dir /var/log/hms_orphans >> /var/log/hms_orphans/cron.log 2>&1
```

## Safety notes

- Dry-run is the default; nothing changes without `--execute`.
- Every object is re-verified against storage immediately before its DROP.
- Dropping an **external** table removes only metadata; for an orphan the data
  is already gone either way. Scope with `--databases`/`--tables` if unsure, and
  use `--limit` on the first execute run to bound blast radius.
