# Third-Party Safety & Compatibility Audit - `hive-tools`

**Scope:** `orphan-cleanup/hive_orphan_cleanup.py` and
`pressure-monitor/metadata_pressure_monitor.py`
**Target environment:** RHEL 8, CDP 7.1.9 (Hive 3), Isilon (OneFS HDFS), MySQL
metastore.
**Audit objective:** confirm the tools are conservative and production-safe -
they cannot break tables, they notify and require confirmation before any
drop/delete, and they are maximally compatible with the target stack.

**Outcome:** All findings below were remediated in-repo and verified with an
automated harness (fake `beeline`/`hdfs`) plus unit checks - **18/18** behavioral
assertions and **19/19** unit assertions pass. The pressure monitor is read-only
and required no safety changes.

---

## Risk posture (after remediation)

`orphan-cleanup` now enforces, by default:

1. **Dry-run** unless `--execute`.
2. **EXTERNAL-only.** MANAGED objects are skipped unless `--allow-managed`
   (dropping a managed object deletes data).
3. **ACID/transactional tables are never auto-dropped** (hard rule, not
   overridable).
4. **Positive proof of absence.** An object is dropped only when its parent
   directory lists successfully *and* the object is genuinely absent. Transient
   storage errors are treated as "do not touch."
5. **Bulk-safety guard.** Refuses suspiciously large operations
   (`--max-drops`, `--max-orphan-pct`) unless `--allow-bulk`.
6. **Notify + confirm.** Prints an itemized DROP plan and requires the operator
   to type `yes` (or pass `--yes` for automation). Gates #2, #3, #5 are **not**
   bypassed by `--yes`.
7. **Audit trail.** Every executed statement is logged to `audit_<ts>.log`.
8. **Standard Hive DML only** - never raw `DELETE` against the metastore DB.

---

## Findings & resolutions

| ID | Severity | Finding | Resolution | Status |
|----|----------|---------|------------|--------|
| F1 | **Critical** | `DROP TABLE` was issued for any orphaned table, incl. MANAGED tables - a false "missing" reading would delete real data. | External-only default; `--allow-managed` to opt in; itemized plan shows table type. | Fixed |
| F2 | **Critical** | `hdfs dfs -test -e` rc=1 was treated as "missing," but rc=1 also occurs on permission/transient errors -> false orphans. | New `confirm_missing()` requires a successful parent listing *and* the child to be absent; indeterminate -> skip. Detection and pre-drop re-verify both use it. | Fixed |
| F3 | **Critical** | No guard against a storage-wide outage making *everything* look orphaned. | `bulk_guard()` refuses > `--max-drops` (default 500) or > `--max-orphan-pct` (default 25%) of scanned tables unless `--allow-bulk`. | Fixed |
| F4 | High | `MSCK REPAIR ... DROP PARTITIONS` can drop *all* partitions if the table root is transiently unavailable. | Kept off by default (explicit `ALTER ... DROP PARTITION` is default); prominent warning; documented risk. | Fixed |
| F5 | High | Single blanket `[y/N]` confirmation; `--yes` bypassed everything; no itemized notice. | Itemized destructive plan is always printed; interactive callers must type the whole word `yes`; `--confirm-each` for per-table prompts; hard gates independent of `--yes`. | Fixed |
| F6 | Medium | `apply --skip-reverify` could drop from a stale report with no storage check. | Still available but now warns loudly and is subject to the managed/ACID and bulk-safety gates. | Fixed |
| F7 | Medium | Partition drops on MANAGED tables also delete data (same class as F1). | Partitions now carry `tbl_type`; external-only rule applies to partitions too. | Fixed |
| F8 | **High (bug)** | Shared options (`--jdbc-url`, `--output-dir`, filters) were on the top-level parser, so argparse rejected them *after* the subcommand - i.e. every documented example (`report --jdbc-url ...`) failed. | Refactored to a shared parent parser attached to each subcommand; mode-first ordering now works. | Fixed |
| F9 | Low | `estimate_rows_reclaimed` is approximate. | Documented as approximate; management-reporting only. | Acknowledged |
| F10 | Compat | RHEL 8 ships Python 3.6; needed to confirm no newer-only syntax/APIs. | Startup version check added; verified no 3.7+ constructs (no f-strings, walrus, `capture_output`, `fromisoformat`, etc.); uses `universal_newlines`, `communicate(timeout=)`. Compiles on 3.9 and 3.14. | Fixed |

---

## Compatibility assessment (RHEL 8 / CDP 7.1.9)

- **Python:** standard library only; 3.6-compatible (`from __future__ import
  print_function`, no 3.7+ syntax/APIs). A non-fatal warning is emitted below 3.6.
- **Hive 3 / CDP:** uses `sys.*` metastore views, with a `SHOW`/`DESCRIBE`
  fallback; DML (`DROP TABLE`, `ALTER TABLE ... DROP [IF EXISTS] PARTITION`,
  `MSCK REPAIR ... DROP PARTITIONS`, `ALTER ... COMPACT 'major'`,
  `partition.retention.period`) is Hive 3 syntax.
- **beeline/hdfs:** invoked as subprocesses with explicit timeouts; beeline uses
  `--outputformat=csv2` for stable parsing; Kerberos via existing ticket or
  optional `--keytab`/`--principal`.
- **Isilon (OneFS HDFS):** existence is proven via `hdfs dfs -ls` on the parent
  (robust to `-test -e` ambiguity), which suits Isilon's HDFS semantics.

---

## How this was verified

A self-contained harness (not shipped in the repo) substituted fake `beeline`
and `hdfs` binaries and asserted, end-to-end:

- read-only `report` writes reports and issues **no** DML;
- a table whose parent directory listing errors (simulated outage) is **not**
  flagged;
- `clean --execute` drops only the EXTERNAL orphan table/partition and **never**
  the MANAGED, ACID, present, or indeterminate objects;
- `--execute` on a non-TTY without `--yes` **refuses** and changes nothing;
- the bulk guard stops an oversized run;
- `--allow-managed` includes managed objects but **still** protects ACID tables.

Unit checks additionally proved `confirm_destructive` accepts only `yes`,
`split_actionable`/`bulk_guard` behave as specified, and `confirm_missing`
returns True/False/None for absent/present/indeterminate.

---

## Residual risks & operating recommendations

- **Managed-table cleanup remains inherently destructive.** If you must use
  `--allow-managed`, do it from a trimmed, human-reviewed report and keep
  `--require-parent-exists` on.
- **Verify storage health before any execute.** The bulk guard is a backstop,
  not a substitute for confirming Isilon/NameNode are healthy.
- **Schedule reporting only.** Keep execution a human-reviewed `apply` step.
- **Back up the metastore** (MySQL dump) before large cleanups, per standard
  change control.
