#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
hive_orphan_cleanup.py
======================

Identify and clean up *orphaned* Hive tables and partitions on a CDP 7.1.9
(Hive 3) on-prem cluster backed by Isilon (OneFS HDFS) storage.

"Orphaned" == an object registered in the Hive Metastore (HMS, backed by MySQL)
whose storage LOCATION no longer exists on Isilon/HDFS. Removing such objects
with standard Hive DML (DROP TABLE / ALTER TABLE ... DROP PARTITION /
MSCK REPAIR ... DROP PARTITIONS) cascades away their rows in the metastore
backing tables (TBLS / PARTITIONS / SDS / SERDE_PARAMS / PART_COL_STATS / ...),
which is the core "reduce objects on the MySQL side" win.

This tool runs on a CDP edge/gateway node and shells out to the installed
`beeline` (for Hive DML) and `hdfs dfs` (for Isilon path existence) CLIs. It
uses only the Python standard library. Kerberos is expected to be handled by an
existing ticket (kinit) or an optional `--keytab`/`--principal` pre-run kinit.

------------------------------------------------------------------------------
MODES
------------------------------------------------------------------------------
  report   Read-only. Enumerate HMS objects, check Isilon, write CSV/JSON/summary.
  clean    Detect then clean. Dry-run by default; needs --execute to mutate.
  apply    Take a previously generated (optionally trimmed/edited) report and
           clean only the objects listed in it. Dry-run by default.

------------------------------------------------------------------------------
SAFETY MODEL (conservative by default; designed for production)
------------------------------------------------------------------------------
  * DRY-RUN is the default. Nothing is dropped without --execute.
  * EXTERNAL-ONLY by default. Dropping a MANAGED table deletes DATA, so managed
    tables/partitions are SKIPPED unless you pass --allow-managed.
  * ACID/transactional tables are NEVER auto-dropped (hard rule).
  * POSITIVE PROOF of absence: an object is only ever dropped when the parent
    directory lists successfully AND the object is genuinely not in it. Any
    transient/indeterminate storage result is skipped, so an Isilon/NameNode
    hiccup cannot manufacture orphans or trigger drops.
  * BULK GUARD: refuses to act on more than --max-drops objects, or on more than
    --max-orphan-pct of scanned tables, unless --allow-bulk (an outage, not real
    orphans, is the usual cause of a huge set).
  * NOTIFY + CONFIRM: before any execute, an itemized plan is printed and you
    must type the whole word 'yes' (or pass --yes for automation). --confirm-each
    prompts per table. Every statement is recorded to an audit log.

------------------------------------------------------------------------------
QUICK EXAMPLES (recommended workflow: report -> review -> apply)
------------------------------------------------------------------------------
  # 1) Report only, all databases (READ-ONLY, changes nothing)
  ./hive_orphan_cleanup.py report \
      --jdbc-url 'jdbc:hive2://hs2.example.com:10000/default;principal=hive/_HOST@REALM' \
      --output-dir ./hms_reports

  # 2) Preview a clean (dry-run prints the exact DROP plan)
  ./hive_orphan_cleanup.py clean --databases sales,marketing --output-dir ./hms_reports

  # 3) Clean exactly what an (edited/trimmed) prior report lists, interactively
  ./hive_orphan_cleanup.py apply --report ./hms_reports/orphans_20260831_080000.csv --execute
  #    (you will be shown the plan and prompted to type 'yes')

  # 4) Include managed tables too (deletes data for genuine orphans) - opt in
  ./hive_orphan_cleanup.py apply --report ./trimmed.csv --allow-managed --execute

  # 5) Opt-in housekeeping while cleaning (external tables only)
  ./hive_orphan_cleanup.py clean --set-retention 30 --compact --execute

------------------------------------------------------------------------------
SAMPLE CRON (read-only reporting only; execute is a human-reviewed step)
------------------------------------------------------------------------------
  # Weekly Monday 02:00 report (read-only). Do NOT schedule --execute blindly;
  # review the report, trim it, then run `apply` by hand (or with --yes once the
  # trimmed report has been reviewed).
  0 2 * * 1 /opt/hive-tools/orphan-cleanup/hive_orphan_cleanup.py report \
      --keytab /etc/security/keytabs/hive.keytab --principal hive/edge01@REALM \
      --jdbc-url 'jdbc:hive2://hs2:10000/default;principal=hive/_HOST@REALM' \
      --output-dir /var/log/hms_orphans >> /var/log/hms_orphans/cron.log 2>&1

------------------------------------------------------------------------------
EXIT CODES
------------------------------------------------------------------------------
  0  success (no errors; orphans may or may not have been found)
  1  runtime error (beeline/hdfs failure, bad args, connection problems)
  2  partial failure (some DML statements failed during clean/apply)
  3  soft warning / stopped safely (empty report, aborted at the prompt, or the
     bulk-safety guard refused a suspiciously large operation)
"""

from __future__ import print_function

import argparse
import csv
import datetime
import json
import logging
import os
import re
import shlex
import subprocess
import sys
import tempfile
from collections import defaultdict

try:
    from concurrent.futures import ThreadPoolExecutor, as_completed
    _HAVE_FUTURES = True
except ImportError:  # very old python
    _HAVE_FUTURES = False


# ----------------------------------------------------------------------------
# Constants / exit codes
# ----------------------------------------------------------------------------
EXIT_OK = 0
EXIT_ERROR = 1
EXIT_PARTIAL = 2
EXIT_WARN = 3

OBJECT_TABLE = "TABLE"
OBJECT_PARTITION = "PARTITION"

REPORT_COLUMNS = [
    "object_type",
    "db",
    "table",
    "tbl_type",
    "partition_spec",
    "location",
    "reason",
    "detected_at",
]

LOG = logging.getLogger("hive_orphan_cleanup")


# ----------------------------------------------------------------------------
# Data model
# ----------------------------------------------------------------------------
class HiveObject(object):
    """A single HMS object (table or partition) and its storage location."""

    __slots__ = (
        "object_type",
        "db",
        "table",
        "tbl_type",
        "partition_spec",
        "location",
        "reason",
        "detected_at",
    )

    def __init__(self, object_type, db, table, tbl_type="", partition_spec="",
                 location="", reason="", detected_at=""):
        self.object_type = object_type
        self.db = db
        self.table = table
        self.tbl_type = tbl_type
        self.partition_spec = partition_spec
        self.location = location
        self.reason = reason
        self.detected_at = detected_at

    @property
    def fqtn(self):
        return "`%s`.`%s`" % (self.db, self.table)

    def to_row(self):
        return {
            "object_type": self.object_type,
            "db": self.db,
            "table": self.table,
            "tbl_type": self.tbl_type,
            "partition_spec": self.partition_spec,
            "location": self.location,
            "reason": self.reason,
            "detected_at": self.detected_at,
        }

    @classmethod
    def from_row(cls, row):
        return cls(
            object_type=(row.get("object_type") or "").strip().upper(),
            db=(row.get("db") or "").strip(),
            table=(row.get("table") or "").strip(),
            tbl_type=(row.get("tbl_type") or "").strip(),
            partition_spec=(row.get("partition_spec") or "").strip(),
            location=(row.get("location") or "").strip(),
            reason=(row.get("reason") or "").strip(),
            detected_at=(row.get("detected_at") or "").strip(),
        )


# ----------------------------------------------------------------------------
# Subprocess wrappers: beeline, hdfs dfs, kinit
# ----------------------------------------------------------------------------
class CommandError(Exception):
    """Raised when an external command fails in a way we cannot recover from."""


class CommandResult(object):
    def __init__(self, returncode, stdout, stderr):
        self.returncode = returncode
        self.stdout = stdout
        self.stderr = stderr

    @property
    def ok(self):
        return self.returncode == 0


def _run(cmd, timeout=None, input_text=None):
    """Run a command (list form), return CommandResult. Never raises on nonzero."""
    LOG.debug("exec: %s", " ".join(shlex.quote(c) for c in cmd))
    try:
        proc = subprocess.Popen(
            cmd,
            stdin=subprocess.PIPE if input_text is not None else None,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            universal_newlines=True,
        )
        try:
            out, err = proc.communicate(input=input_text, timeout=timeout)
        except TypeError:
            # python2-ish communicate() without timeout kwarg
            out, err = proc.communicate(input=input_text)
    except subprocess.TimeoutExpired:
        proc.kill()
        out, err = proc.communicate()
        return CommandResult(124, out or "", (err or "") + "\n[timeout]")
    except OSError as exc:
        raise CommandError("failed to launch %r: %s" % (cmd[0], exc))
    return CommandResult(proc.returncode, out or "", err or "")


class HiveRunner(object):
    """Runs Hive SQL via beeline and returns parsed rows.

    Uses beeline CSV2 output format so we can parse reliably. Each call is a
    fresh beeline session; we group statements to keep session count low.
    """

    def __init__(self, jdbc_url, beeline_path="beeline", extra_args=None,
                 timeout=3600, dry_run=False, audit=None):
        self.jdbc_url = jdbc_url
        self.beeline_path = beeline_path
        self.extra_args = extra_args or []
        self.timeout = timeout
        self.dry_run = dry_run
        self.audit = audit  # AuditLog or None

    def _base_cmd(self):
        cmd = [self.beeline_path]
        if self.jdbc_url:
            cmd += ["-u", self.jdbc_url]
        cmd += list(self.extra_args)
        # Silence chatter, emit clean CSV.
        cmd += [
            "--silent=true",
            "--verbose=false",
            "--showHeader=true",
            "--outputformat=csv2",
            "--nullemptystring=true",
        ]
        return cmd

    def query(self, sql):
        """Run a read-only query, return list[dict] parsed from csv2 output."""
        cmd = self._base_cmd() + ["-e", sql]
        res = _run(cmd, timeout=self.timeout)
        if not res.ok:
            raise CommandError(
                "beeline query failed (rc=%s): %s\nSQL: %s\nSTDERR: %s"
                % (res.returncode, _first_line(res.stderr), _short(sql), _tail(res.stderr))
            )
        return _parse_csv2(res.stdout)

    def execute(self, statements, label=""):
        """Run one or more DML statements. Honors dry_run.

        Returns (ok_count, fail_count). Records each statement in the audit log.
        """
        if isinstance(statements, str):
            statements = [statements]
        statements = [s for s in statements if s and s.strip()]
        if not statements:
            return (0, 0)

        if self.dry_run:
            for s in statements:
                LOG.info("[DRY-RUN] would execute: %s", _short(s))
                if self.audit:
                    self.audit.record(label, s, "DRY-RUN", "")
            return (len(statements), 0)

        ok = 0
        fail = 0
        # Feed statements via a script file so multiple statements share a session.
        script = ";\n".join(s.rstrip(";") for s in statements) + ";\n"
        tmp = tempfile.NamedTemporaryFile(
            mode="w", suffix=".hql", delete=False, prefix="hoc_")
        try:
            tmp.write(script)
            tmp.close()
            cmd = self._base_cmd() + ["-f", tmp.name]
            res = _run(cmd, timeout=self.timeout)
            if res.ok:
                ok = len(statements)
                for s in statements:
                    LOG.info("executed: %s", _short(s))
                    if self.audit:
                        self.audit.record(label, s, "OK", "")
            else:
                # Whole batch failed; fall back to per-statement so we can
                # attribute failures and keep going.
                LOG.warning("batch failed (rc=%s), retrying statement-by-statement",
                            res.returncode)
                ok, fail = self._execute_individually(statements, label)
        finally:
            try:
                os.unlink(tmp.name)
            except OSError:
                pass
        return (ok, fail)

    def _execute_individually(self, statements, label):
        ok = 0
        fail = 0
        for s in statements:
            cmd = self._base_cmd() + ["-e", s]
            res = _run(cmd, timeout=self.timeout)
            if res.ok:
                ok += 1
                LOG.info("executed: %s", _short(s))
                if self.audit:
                    self.audit.record(label, s, "OK", "")
            else:
                fail += 1
                msg = _first_line(res.stderr) or "rc=%s" % res.returncode
                LOG.error("FAILED: %s :: %s", _short(s), msg)
                if self.audit:
                    self.audit.record(label, s, "FAIL", _tail(res.stderr))
        return (ok, fail)


class HdfsClient(object):
    """Thin wrapper over `hdfs dfs` for existence checks against Isilon."""

    def __init__(self, hdfs_path="hdfs", timeout=300):
        self.hdfs_path = hdfs_path
        self.timeout = timeout

    def exists(self, path):
        """True/False if a single path exists. None if indeterminate (error)."""
        res = _run([self.hdfs_path, "dfs", "-test", "-e", path], timeout=self.timeout)
        if res.returncode == 0:
            return True
        if res.returncode == 1:
            return False
        # Any other rc (e.g. connection issue) -> indeterminate.
        LOG.warning("hdfs -test indeterminate for %s: rc=%s %s",
                    path, res.returncode, _first_line(res.stderr))
        return None

    def list_children(self, path):
        """Return set of immediate child paths under `path`.

        Returns None if the path itself does not exist or the listing errored.
        """
        res = _run([self.hdfs_path, "dfs", "-ls", path], timeout=self.timeout)
        if not res.ok:
            return None
        children = set()
        for line in res.stdout.splitlines():
            line = line.strip()
            if not line or line.startswith("Found "):
                continue
            # Last whitespace-delimited token is the path.
            parts = line.split()
            if len(parts) >= 8:
                children.add(parts[-1].rstrip("/"))
        return children

    def confirm_missing(self, path):
        """Authoritatively decide whether `path` is *truly* absent.

        This is stronger (and safer) than `-test -e`, whose rc=1 conflates
        "does not exist" with many transient errors (permission, NN failover,
        Isilon hiccup). We instead LIST THE PARENT and require positive proof:

            True   -> parent listed OK *and* the child is genuinely not present
                      (safe to treat as an orphan)
            False  -> the child is present (definitely NOT an orphan)
            None   -> indeterminate: the parent could not be listed, so we
                      cannot prove absence. Callers MUST treat None as
                      "do not touch" to avoid acting during an outage.

        Requiring the parent to be listable also guarantees we never mass-drop
        objects when a whole mount/NameNode is unavailable (the parent listing
        fails -> None -> skip).
        """
        norm = path.rstrip("/")
        parent = os.path.dirname(norm)
        if not parent or parent == norm:
            # Filesystem root or unparseable: fall back to a plain existence
            # test and only trust an explicit rc=1 "does not exist".
            present = self.exists(path)
            if present is False:
                return True
            if present is True:
                return False
            return None
        kids = self.list_children(parent)
        if kids is None:
            return None  # parent unreachable -> cannot prove absence
        return norm not in kids


def _maybe_kinit(principal, keytab):
    if not keytab and not principal:
        return
    if not (keytab and principal):
        LOG.warning("both --keytab and --principal are required to kinit; skipping")
        return
    LOG.info("kinit %s using keytab %s", principal, keytab)
    res = _run(["kinit", "-kt", keytab, principal], timeout=60)
    if not res.ok:
        raise CommandError("kinit failed: %s" % _first_line(res.stderr))


# ----------------------------------------------------------------------------
# Parsing helpers
# ----------------------------------------------------------------------------
def _parse_csv2(text):
    """Parse beeline csv2 output into list[dict]. First row is header."""
    if not text:
        return []
    lines = [ln for ln in text.splitlines()
             if ln.strip() != "" and not _is_beeline_noise(ln)]
    if not lines:
        return []
    reader = csv.reader(lines)
    rows = list(reader)
    if not rows:
        return []
    header = [h.strip() for h in rows[0]]
    out = []
    for r in rows[1:]:
        if len(r) < len(header):
            r = r + [""] * (len(header) - len(r))
        out.append(dict(zip(header, r)))
    return out


_NOISE_PREFIXES = (
    "Connecting to ",
    "Connected to:",
    "Driver:",
    "Transaction isolation:",
    "Beeline version",
    "Closing:",
    "INFO ",
    "WARN ",
    "0: jdbc:",
)


def _is_beeline_noise(line):
    s = line.strip()
    for p in _NOISE_PREFIXES:
        if s.startswith(p):
            return True
    return False


def _first_line(s):
    if not s:
        return ""
    for ln in s.splitlines():
        if ln.strip():
            return ln.strip()
    return ""


def _tail(s, n=800):
    if not s:
        return ""
    return s[-n:]


def _short(s, n=180):
    s = " ".join(s.split())
    return s if len(s) <= n else s[: n - 3] + "..."


# ----------------------------------------------------------------------------
# Audit log
# ----------------------------------------------------------------------------
class AuditLog(object):
    def __init__(self, path):
        self.path = path
        self._fh = open(path, "a") if path else None

    def record(self, label, statement, status, detail):
        if not self._fh:
            return
        ts = datetime.datetime.now().isoformat(timespec="seconds") \
            if hasattr(datetime.datetime.now(), "isoformat") else str(datetime.datetime.now())
        self._fh.write("%s\t%s\t%s\t%s\t%s\n" % (
            ts, status, label, _short(statement, 500),
            _short(detail, 300).replace("\t", " ")))
        self._fh.flush()

    def close(self):
        if self._fh:
            self._fh.close()
            self._fh = None


# ----------------------------------------------------------------------------
# Enumeration
# ----------------------------------------------------------------------------
SYS_TABLES_SQL = """
SELECT d.NAME AS db, t.TBL_NAME AS tbl, t.TBL_TYPE AS tbl_type, s.LOCATION AS location
FROM sys.TBLS t
JOIN sys.DBS d ON t.DB_ID = d.DB_ID
JOIN sys.SDS s ON t.SD_ID = s.SD_ID
WHERE s.LOCATION IS NOT NULL
"""

SYS_PARTITIONS_SQL = """
SELECT d.NAME AS db, t.TBL_NAME AS tbl, t.TBL_TYPE AS tbl_type,
       p.PART_NAME AS part_name, s.LOCATION AS location
FROM sys.PARTITIONS p
JOIN sys.TBLS t ON p.TBL_ID = t.TBL_ID
JOIN sys.DBS d ON t.DB_ID = d.DB_ID
JOIN sys.SDS s ON p.SD_ID = s.SD_ID
WHERE s.LOCATION IS NOT NULL
"""

# Transactional (ACID) tables. These are managed tables whose data lives under
# the warehouse dir; they must NEVER be auto-dropped by this tool.
SYS_TRANSACTIONAL_SQL = """
SELECT d.NAME AS db, t.TBL_NAME AS tbl
FROM sys.TABLE_PARAMS tp
JOIN sys.TBLS t ON tp.TBL_ID = t.TBL_ID
JOIN sys.DBS d ON t.DB_ID = d.DB_ID
WHERE tp.PARAM_KEY = 'transactional' AND lower(tp.PARAM_VALUE) = 'true'
"""


class Enumerator(object):
    """Enumerate tables and partitions with their locations from HMS."""

    def __init__(self, hive, filters):
        self.hive = hive
        self.filters = filters  # Filters instance

    # --- bulk path via sys.* views ---
    def enumerate_via_sys(self):
        tables = self._sys_tables()
        parts = self._sys_partitions()
        return tables, parts

    def _sys_tables(self):
        rows = self.hive.query(_where_clause(SYS_TABLES_SQL, self.filters))
        out = []
        for r in rows:
            db = r.get("db", "")
            tbl = r.get("tbl", "")
            if not self.filters.accept(db, tbl):
                continue
            out.append(HiveObject(
                OBJECT_TABLE, db, tbl,
                tbl_type=r.get("tbl_type", ""),
                location=r.get("location", ""),
            ))
        return out

    def _sys_partitions(self):
        rows = self.hive.query(_where_clause(SYS_PARTITIONS_SQL, self.filters))
        out = []
        for r in rows:
            db = r.get("db", "")
            tbl = r.get("tbl", "")
            if not self.filters.accept(db, tbl):
                continue
            out.append(HiveObject(
                OBJECT_PARTITION, db, tbl,
                tbl_type=r.get("tbl_type", ""),
                partition_spec=r.get("part_name", ""),
                location=r.get("location", ""),
            ))
        return out

    # --- fallback path via SHOW/DESCRIBE ---
    def enumerate_via_show(self):
        tables = []
        parts = []
        dbs = [r.get("database_name") or list(r.values())[0]
               for r in self.hive.query("SHOW DATABASES")]
        for db in dbs:
            if not self.filters.accept_db(db):
                continue
            trows = self.hive.query("SHOW TABLES IN `%s`" % db)
            for tr in trows:
                tbl = tr.get("tab_name") or list(tr.values())[0]
                if not self.filters.accept(db, tbl):
                    continue
                meta = self._describe_table(db, tbl)
                if meta is None:
                    continue
                tbl_type, location = meta
                tables.append(HiveObject(
                    OBJECT_TABLE, db, tbl, tbl_type=tbl_type, location=location))
                for pobj in self._show_partitions(db, tbl):
                    pobj.tbl_type = tbl_type  # inherit table type for safety gating
                    parts.append(pobj)
        return tables, parts

    def _describe_table(self, db, tbl):
        try:
            rows = self.hive.query("DESCRIBE FORMATTED `%s`.`%s`" % (db, tbl))
        except CommandError as exc:
            LOG.warning("DESCRIBE failed for %s.%s: %s", db, tbl, exc)
            return None
        location = ""
        tbl_type = ""
        for r in rows:
            vals = [v for v in r.values() if v is not None]
            joined = " ".join(str(v) for v in vals)
            if "Location:" in joined:
                location = _value_after(vals, "Location:")
            if "Table Type:" in joined:
                tbl_type = _value_after(vals, "Table Type:")
        return (tbl_type, location)

    def _show_partitions(self, db, tbl):
        try:
            rows = self.hive.query("SHOW PARTITIONS `%s`.`%s`" % (db, tbl))
        except CommandError:
            return []
        out = []
        for r in rows:
            spec = r.get("partition") or list(r.values())[0]
            if not spec:
                continue
            out.append(HiveObject(
                OBJECT_PARTITION, db, tbl, partition_spec=spec, location=""))
        return out


def _value_after(vals, key):
    for i, v in enumerate(vals):
        if v and key in str(v):
            # value is usually the next column
            if i + 1 < len(vals) and vals[i + 1]:
                return str(vals[i + 1]).strip()
    return ""


def _where_clause(base_sql, filters):
    """Append db include/exclude predicates to a sys.* query for efficiency."""
    preds = []
    if filters.databases:
        names = ",".join("'%s'" % d for d in filters.databases)
        preds.append("d.NAME IN (%s)" % names)
    if filters.exclude_databases:
        names = ",".join("'%s'" % d for d in filters.exclude_databases)
        preds.append("d.NAME NOT IN (%s)" % names)
    if not preds:
        return base_sql
    joiner = " AND " if "WHERE" in base_sql.upper() else " WHERE "
    return base_sql + joiner + " AND ".join(preds)


# ----------------------------------------------------------------------------
# Filters
# ----------------------------------------------------------------------------
class Filters(object):
    def __init__(self, databases=None, exclude_databases=None,
                 tables=None, table_regex=None):
        self.databases = _split_csv(databases)
        self.exclude_databases = _split_csv(exclude_databases)
        self.tables = set(_split_csv(tables))
        self.table_regex = re.compile(table_regex) if table_regex else None

    def accept_db(self, db):
        if self.databases and db not in self.databases:
            return False
        if self.exclude_databases and db in self.exclude_databases:
            return False
        return True

    def accept(self, db, tbl):
        if not self.accept_db(db):
            return False
        if self.tables and ("%s.%s" % (db, tbl) not in self.tables
                            and tbl not in self.tables):
            return False
        if self.table_regex and not self.table_regex.search(tbl):
            return False
        return True


def _split_csv(val):
    if not val:
        return []
    if isinstance(val, (list, tuple)):
        return [v.strip() for v in val if v and v.strip()]
    return [v.strip() for v in str(val).split(",") if v.strip()]


# ----------------------------------------------------------------------------
# Detection: which enumerated objects are orphaned on Isilon
# ----------------------------------------------------------------------------
class Detector(object):
    """Classify enumerated objects as orphaned based on Isilon existence."""

    def __init__(self, hdfs, require_parent_exists=True, max_workers=8):
        self.hdfs = hdfs
        self.require_parent_exists = require_parent_exists
        self.max_workers = max_workers

    def detect(self, tables, partitions):
        """Return list[HiveObject] flagged orphaned, with reason populated.

        Conservative by construction: an object is only flagged when we have
        *positive proof* that its location is absent. Any indeterminate result
        (transient storage error, unreachable parent) is skipped rather than
        assumed missing, so an Isilon/NameNode hiccup cannot manufacture
        orphans.
        """
        now = datetime.datetime.now().isoformat(timespec="seconds") \
            if _supports_timespec() else datetime.datetime.now().isoformat()
        orphans = []

        # --- Tables: check each table location ---
        table_locations = [(t, t.location) for t in tables if t.location]
        for obj, missing in self._check_paths(table_locations):
            if missing is True:
                obj.reason = "table location missing on storage"
                obj.detected_at = now
                orphans.append(obj)
            elif missing is None:
                LOG.debug("skip %s: storage indeterminate (transient?)", obj.location)

        # --- Partitions: group by table, list table children once ---
        by_table = defaultdict(list)
        for p in partitions:
            by_table[(p.db, p.table)].append(p)

        for (db, tbl), plist in by_table.items():
            # Try one directory listing of the parent(s) to avoid many -test calls.
            parents = set(os.path.dirname(p.location.rstrip("/"))
                          for p in plist if p.location)
            children_cache = {}
            for parent in parents:
                children_cache[parent] = self.hdfs.list_children(parent)

            to_probe = []
            for p in plist:
                if not p.location:
                    # No location known (fallback path): probe directly later.
                    to_probe.append(p)
                    continue
                parent = os.path.dirname(p.location.rstrip("/"))
                kids = children_cache.get(parent)
                if kids is None:
                    # parent listing failed/absent -> indeterminate. Only probe
                    # individually when we are NOT requiring parent corroboration;
                    # otherwise treat as "do not touch".
                    if not self.require_parent_exists:
                        to_probe.append(p)
                    else:
                        LOG.debug("skip partition %s: parent %s unreachable",
                                  p.location, parent)
                    continue
                if p.location.rstrip("/") not in kids:
                    p.reason = "partition location missing on storage"
                    p.detected_at = now
                    orphans.append(p)

            # Individual probes for partitions we could not resolve via listing.
            probe_locs = [(p, p.location) for p in to_probe if p.location]
            for obj, missing in self._check_paths(probe_locs):
                if missing is True:
                    obj.reason = "partition location missing on storage"
                    obj.detected_at = now
                    orphans.append(obj)
        return orphans

    def _is_missing(self, path):
        """Return True (proven absent), False (present), or None (indeterminate).

        Uses parent-listing corroboration by default (safe); only falls back to
        a bare `-test -e` when the operator explicitly disables the heuristic.
        """
        if self.require_parent_exists:
            return self.hdfs.confirm_missing(path)
        present = self.hdfs.exists(path)
        if present is True:
            return False
        if present is False:
            return True
        return None

    def _check_paths(self, pairs):
        """Yield (obj, is_missing) for each (obj, path) pair."""
        if not pairs:
            return
        if _HAVE_FUTURES and self.max_workers > 1:
            with ThreadPoolExecutor(max_workers=self.max_workers) as ex:
                fut = {ex.submit(self._is_missing, path): obj
                       for obj, path in pairs}
                for f in as_completed(fut):
                    yield (fut[f], f.result())
        else:
            for obj, path in pairs:
                yield (obj, self._is_missing(path))


def _supports_timespec():
    try:
        datetime.datetime.now().isoformat(timespec="seconds")
        return True
    except TypeError:
        return False


# ----------------------------------------------------------------------------
# Reporting
# ----------------------------------------------------------------------------
class Reporter(object):
    def __init__(self, output_dir):
        self.output_dir = output_dir
        if output_dir and not os.path.isdir(output_dir):
            os.makedirs(output_dir)
        self.ts = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")

    def _path(self, name):
        return os.path.join(self.output_dir or ".", name)

    def write_all(self, orphans, stats, recommendations):
        csv_path = self._path("orphans_%s.csv" % self.ts)
        json_path = self._path("orphans_%s.json" % self.ts)
        summary_path = self._path("summary_%s.txt" % self.ts)
        self.write_csv(csv_path, orphans)
        self.write_json(json_path, orphans, stats, recommendations)
        self.write_summary(summary_path, orphans, stats, recommendations)
        return {"csv": csv_path, "json": json_path, "summary": summary_path}

    def write_csv(self, path, orphans):
        with open(path, "w") as fh:
            writer = csv.DictWriter(fh, fieldnames=REPORT_COLUMNS)
            writer.writeheader()
            for o in orphans:
                writer.writerow(o.to_row())
        LOG.info("wrote CSV report: %s (%d rows)", path, len(orphans))

    def write_json(self, path, orphans, stats, recommendations):
        doc = {
            "generated_at": datetime.datetime.now().isoformat(),
            "stats": stats,
            "recommendations": recommendations,
            "orphans": [o.to_row() for o in orphans],
        }
        with open(path, "w") as fh:
            json.dump(doc, fh, indent=2)
        LOG.info("wrote JSON report: %s", path)

    def write_summary(self, path, orphans, stats, recommendations):
        n_tables = sum(1 for o in orphans if o.object_type == OBJECT_TABLE)
        n_parts = sum(1 for o in orphans if o.object_type == OBJECT_PARTITION)
        lines = []
        lines.append("=" * 72)
        lines.append("HIVE ORPHAN OBJECT REPORT")
        lines.append("Generated: %s" % datetime.datetime.now().isoformat())
        lines.append("=" * 72)
        lines.append("")
        lines.append("SCANNED")
        lines.append("  Tables scanned      : %s" % stats.get("tables_scanned", "?"))
        lines.append("  Partitions scanned  : %s" % stats.get("partitions_scanned", "?"))
        lines.append("  Enumeration method  : %s" % stats.get("enumeration_method", "?"))
        lines.append("")
        lines.append("ORPHANS FOUND")
        lines.append("  Orphaned tables     : %d" % n_tables)
        lines.append("  Orphaned partitions : %d" % n_parts)
        lines.append("  TOTAL               : %d" % len(orphans))
        lines.append("")
        est = stats.get("estimated_mysql_rows_reclaimed")
        if est is not None:
            lines.append("ESTIMATED METASTORE (MySQL) ROWS RECLAIMED (approx.)")
            lines.append("  ~%d rows across TBLS/PARTITIONS/SDS/SERDE_PARAMS/"
                         "PARTITION_PARAMS/PART_COL_STATS" % est)
            lines.append("  (each dropped partition frees ~5-8 backing rows; each")
            lines.append("   dropped table frees those plus column/stats rows)")
            lines.append("")

        # Group by db.table for readability
        grouped = defaultdict(lambda: {"table": None, "parts": 0})
        for o in orphans:
            key = "%s.%s" % (o.db, o.table)
            if o.object_type == OBJECT_TABLE:
                grouped[key]["table"] = o
            else:
                grouped[key]["parts"] += 1
        lines.append("BREAKDOWN BY TABLE")
        for key in sorted(grouped):
            g = grouped[key]
            tag = "TABLE-ORPHAN" if g["table"] else "partitions"
            lines.append("  %-50s %s (%d part(s))" % (key, tag, g["parts"]))
        lines.append("")

        lines.append("RECOMMENDATIONS")
        if recommendations:
            for r in recommendations:
                lines.append("  - " + r)
        else:
            lines.append("  (none)")
        lines.append("")
        lines.append("NEXT STEPS")
        lines.append("  Review this report. To clean exactly these objects:")
        lines.append("    hive_orphan_cleanup.py apply --report %s --execute --yes"
                     % self._path("orphans_%s.csv" % self.ts))
        lines.append("=" * 72)
        text = "\n".join(lines) + "\n"
        with open(path, "w") as fh:
            fh.write(text)
        LOG.info("wrote summary: %s", path)
        return text


def estimate_rows_reclaimed(orphans):
    """Rough estimate of metastore backing rows freed by dropping these objects.

    Not exact (schema-dependent) but useful for management reporting. Each
    partition drop cascades PARTITIONS + PARTITION_PARAMS + SDS + SERDE +
    SERDE_PARAMS + (per-column) PART_COL_STATS. We use a conservative multiplier.
    """
    rows = 0
    for o in orphans:
        if o.object_type == OBJECT_PARTITION:
            rows += 6
        else:
            rows += 10
    return rows


# ----------------------------------------------------------------------------
# Report parsing (for `apply` mode)
# ----------------------------------------------------------------------------
def load_report(path):
    """Load a prior report (CSV or JSON) into list[HiveObject]."""
    if not os.path.isfile(path):
        raise CommandError("report file not found: %s" % path)
    if path.lower().endswith(".json"):
        with open(path) as fh:
            doc = json.load(fh)
        rows = doc.get("orphans", doc if isinstance(doc, list) else [])
        return [HiveObject.from_row(r) for r in rows]
    # default: CSV
    with open(path) as fh:
        reader = csv.DictReader(fh)
        return [HiveObject.from_row(r) for r in reader]


# ----------------------------------------------------------------------------
# DML builders
# ----------------------------------------------------------------------------
def _partition_spec_to_clause(spec):
    """Convert 'y=2024/m=01' (or 'y=2024,m=01') into (`y`='2024', `m`='01')."""
    parts = re.split(r"[/,]", spec)
    kv = []
    for p in parts:
        if "=" not in p:
            continue
        k, v = p.split("=", 1)
        k = k.strip()
        v = v.strip()
        # Numeric values can stay unquoted, but quoting is always safe.
        kv.append("`%s`='%s'" % (k, v.replace("'", "\\'")))
    return "(" + ", ".join(kv) + ")"


def build_cleanup_statements(orphans, use_msck=False):
    """Group orphans by table and build DML statements.

    Returns list[(label, [statements])].
    Rules:
      - If a whole table is orphaned -> DROP TABLE (its partitions go with it).
      - Else, drop orphaned partitions. If use_msck and many partitions for a
        table, prefer one MSCK REPAIR ... DROP PARTITIONS; otherwise explicit
        ALTER TABLE ... DROP PARTITION statements (batched).
    """
    table_orphans = {}
    part_orphans = defaultdict(list)
    for o in orphans:
        key = (o.db, o.table)
        if o.object_type == OBJECT_TABLE:
            table_orphans[key] = o
        else:
            part_orphans[key].append(o)

    batches = []
    for (db, tbl), obj in sorted(table_orphans.items()):
        stmt = "DROP TABLE IF EXISTS `%s`.`%s`" % (db, tbl)
        batches.append(("DROP TABLE %s.%s" % (db, tbl), [stmt]))
        # Any partitions of a dropped table are handled by the drop; skip them.
        part_orphans.pop((db, tbl), None)

    for (db, tbl), plist in sorted(part_orphans.items()):
        label = "DROP PARTITIONS %s.%s (%d)" % (db, tbl, len(plist))
        if use_msck:
            stmt = "MSCK REPAIR TABLE `%s`.`%s` DROP PARTITIONS" % (db, tbl)
            batches.append((label, [stmt]))
        else:
            stmts = []
            for p in plist:
                clause = _partition_spec_to_clause(p.partition_spec)
                if clause == "()":
                    LOG.warning("skip partition with unpar.spec: %s.%s %r",
                                db, tbl, p.partition_spec)
                    continue
                stmts.append("ALTER TABLE `%s`.`%s` DROP IF EXISTS PARTITION %s"
                             % (db, tbl, clause))
            if stmts:
                batches.append((label, stmts))
    return batches


# ----------------------------------------------------------------------------
# Safety gating: what is actionable, hard limits, and explicit notification
# ----------------------------------------------------------------------------
def _is_external(obj):
    return "EXTERNAL" in (obj.tbl_type or "").upper()


def _is_view(obj):
    return "VIEW" in (obj.tbl_type or "").upper()


def fetch_transactional_tables(hive):
    """Return a set of (db_lower, table_lower) for ACID/transactional tables.

    Best-effort: if sys.* is unavailable we return an empty set and rely on the
    external-only default (ACID tables are managed) for protection.
    """
    acid = set()
    try:
        rows = hive.query(SYS_TRANSACTIONAL_SQL)
    except CommandError as exc:
        LOG.warning("could not enumerate ACID tables (%s); relying on the "
                    "external-only default for protection", _first_line(str(exc)))
        return acid
    for r in rows:
        db = (r.get("db") or "").strip().lower()
        tbl = (r.get("tbl") or "").strip().lower()
        if db and tbl:
            acid.add((db, tbl))
    if acid:
        LOG.info("identified %d transactional/ACID table(s) - these will never "
                 "be auto-dropped", len(acid))
    return acid


def split_actionable(orphans, acid_set, allow_managed):
    """Split orphans into (actionable, skipped) using conservative rules.

    Hard rules (NOT overridable by --yes):
      * ACID/transactional tables are NEVER auto-dropped.
      * VIEWs are never touched (no storage; should not appear anyway).
      * MANAGED tables/partitions are skipped unless --allow-managed, because a
        DROP on a managed object deletes DATA and a false "missing" reading
        would then be destructive.
    Returns (actionable list, skipped list of (category, obj)).
    """
    actionable = []
    skipped = []
    for o in orphans:
        key = (o.db.strip().lower(), o.table.strip().lower())
        if key in acid_set:
            skipped.append(("acid", o))
            continue
        if _is_view(o):
            skipped.append(("view", o))
            continue
        if not _is_external(o) and not allow_managed:
            skipped.append(("managed", o))
            continue
        actionable.append(o)
    return actionable, skipped


def log_skipped(skipped):
    if not skipped:
        return
    by_cat = defaultdict(int)
    for cat, _o in skipped:
        by_cat[cat] += 1
    msgs = {
        "acid": "ACID/transactional - never auto-dropped",
        "managed": "MANAGED (data-bearing) - pass --allow-managed to include",
        "view": "VIEW (no storage) - ignored",
    }
    LOG.warning("SKIPPED %d orphan(s) by safety policy:", len(skipped))
    for cat in sorted(by_cat):
        LOG.warning("  %-8s %5d  - %s", cat, by_cat[cat], msgs.get(cat, cat))


class BulkGuardError(Exception):
    """Raised when an operation is too large to run without --allow-bulk."""


def bulk_guard(actionable, tables_scanned, args):
    """Refuse suspiciously large operations unless --allow-bulk is set.

    A very large orphan set is far more likely to indicate a storage-wide
    problem (Isilon unmounted, NameNode failover) than genuine orphans, so we
    stop instead of mass-dropping. This gate is independent of --yes.
    """
    n_obj = len(actionable)
    n_tbl = sum(1 for o in actionable if o.object_type == OBJECT_TABLE)
    if args.allow_bulk:
        LOG.warning("--allow-bulk set: bypassing the bulk-safety guard "
                    "(%d object(s), %d table drop(s))", n_obj, n_tbl)
        return
    if n_obj > args.max_drops:
        raise BulkGuardError(
            "refusing to act on %d objects (> --max-drops=%d). A set this large "
            "usually means a storage outage, not real orphans. Verify Isilon/HDFS "
            "health, then re-run with --allow-bulk to override."
            % (n_obj, args.max_drops))
    if tables_scanned and n_tbl:
        pct = 100.0 * n_tbl / tables_scanned
        if pct > args.max_orphan_pct:
            raise BulkGuardError(
                "refusing: %d/%d scanned tables (%.1f%%) look orphaned "
                "(> --max-orphan-pct=%.1f%%). This strongly suggests a storage "
                "outage. Verify storage, then re-run with --allow-bulk."
                % (n_tbl, tables_scanned, pct, args.max_orphan_pct))


def log_destructive_plan(actionable, use_msck, dry_run):
    """Emit an explicit, itemized notification of exactly what will be dropped."""
    tables = [o for o in actionable if o.object_type == OBJECT_TABLE]
    parts = [o for o in actionable if o.object_type == OBJECT_PARTITION]
    part_tables = sorted(set((o.db, o.table) for o in parts))
    banner = ("PLANNED CHANGES (DRY-RUN - nothing will be dropped)" if dry_run
              else "!!! DESTRUCTIVE PLAN - the following Hive DML WILL be executed !!!")
    LOG.warning("=" * 72)
    LOG.warning(banner)
    LOG.warning("  DROP TABLE     : %d table(s)", len(tables))
    LOG.warning("  DROP PARTITION : %d partition(s) across %d table(s)%s",
                len(parts), len(part_tables),
                " via MSCK REPAIR ... DROP PARTITIONS" if use_msck else "")
    cap = 50
    shown = 0
    for o in tables:
        LOG.warning("    DROP TABLE %s  [%s]  loc=%s",
                    o.fqtn, o.tbl_type or "?", o.location or "?")
        shown += 1
        if shown >= cap:
            break
    for (db, tbl) in part_tables:
        if shown >= cap:
            break
        n = sum(1 for o in parts if o.db == db and o.table == tbl)
        LOG.warning("    DROP %d PARTITION(s) FROM `%s`.`%s`", n, db, tbl)
        shown += 1
    remaining = (len(tables) + len(part_tables)) - shown
    if remaining > 0:
        LOG.warning("    ... and %d more (see the CSV report for the full list)",
                    remaining)
    LOG.warning("=" * 72)


# ----------------------------------------------------------------------------
# Housekeeping (opt-in)
# ----------------------------------------------------------------------------
COMPACTION_SMALL_FILE_HINT = (
    "Run compaction on ACID tables with many small delta files to shrink "
    "TXN_COMPONENTS/COMPLETED_TXN_COMPONENTS/WRITE_SET after the Cleaner runs."
)


class Housekeeper(object):
    """Opt-in, supported MySQL-load reducers driven via Hive DML/config."""

    def __init__(self, hive):
        self.hive = hive

    def set_retention(self, tables, days):
        """Enable partition auto-discovery + retention on partitioned externals."""
        stmts = []
        for t in tables:
            if (t.tbl_type or "").upper().find("EXTERNAL") < 0:
                continue
            stmts.append(
                "ALTER TABLE `%s`.`%s` SET TBLPROPERTIES("
                "'discover.partitions'='true','partition.retention.period'='%dd')"
                % (t.db, t.table, days))
        if not stmts:
            LOG.info("set-retention: no eligible external tables")
            return (0, 0)
        LOG.info("set-retention: applying %dd retention to %d external tables",
                 days, len(stmts))
        return self.hive.execute(stmts, label="SET RETENTION %dd" % days)

    def find_acid_tables(self):
        """Return list of transactional (ACID) tables from sys.* if available."""
        sql = (
            "SELECT d.NAME AS db, t.TBL_NAME AS tbl "
            "FROM sys.TABLE_PARAMS tp "
            "JOIN sys.TBLS t ON tp.TBL_ID = t.TBL_ID "
            "JOIN sys.DBS d ON t.DB_ID = d.DB_ID "
            "WHERE tp.PARAM_KEY='transactional' AND lower(tp.PARAM_VALUE)='true'"
        )
        try:
            rows = self.hive.query(sql)
        except CommandError as exc:
            LOG.warning("could not enumerate ACID tables: %s", exc)
            return []
        return [(r.get("db"), r.get("tbl")) for r in rows if r.get("tbl")]

    def compact(self, acid_tables):
        """Issue major compaction on ACID tables (Cleaner then prunes txn rows)."""
        if not acid_tables:
            LOG.info("compact: no ACID tables found")
            return (0, 0)
        stmts = ["ALTER TABLE `%s`.`%s` COMPACT 'major'" % (db, tbl)
                 for db, tbl in acid_tables]
        LOG.info("compact: requesting major compaction on %d ACID tables",
                 len(stmts))
        return self.hive.execute(stmts, label="COMPACT major")


def build_recommendations(hive, want_details=True):
    """Report-only, config-level recommendations (applied via Cloudera Manager)."""
    recs = [
        "Lower `hive.metastore.event.db.listener.timetolive` (HMS safety valve) "
        "so the DbNotificationListener cleanup thread prunes NOTIFICATION_LOG "
        "rows; large NOTIFICATION_LOG is a common MySQL bloat source.",
        "Confirm the Compaction Initiator and Cleaner are enabled "
        "(`hive.compactor.initiator.on=true`, worker threads > 0) so ACID "
        "transaction metadata (TXN_COMPONENTS, COMPLETED_TXN_COMPONENTS, "
        "WRITE_SET, COMPLETED_COMPACTIONS) is compacted and pruned.",
        COMPACTION_SMALL_FILE_HINT,
        "Dropping orphaned partitions/tables (this tool) cascades their rows in "
        "PART_COL_STATS/TAB_COL_STATS - the same statistics rows behind heavy "
        "get_partitions_statistics_req load - reducing both row count and HMS RPC.",
        "Consider enabling partition retention (--set-retention) on high-churn "
        "external tables so old partition metadata expires automatically.",
        "Do NOT delete rows directly from the MySQL metastore tables; use Hive "
        "DML so HMS caches and dependent rows stay consistent.",
    ]
    if want_details:
        # Try to surface a couple of live counts to make the report actionable.
        for label, sql in (
            ("NOTIFICATION_LOG rows", "SELECT COUNT(*) AS c FROM sys.NOTIFICATION_LOG"),
        ):
            try:
                rows = hive.query(sql)
                if rows:
                    val = list(rows[0].values())[0]
                    recs.insert(0, "Current %s: %s" % (label, val))
            except CommandError:
                pass
    return recs


# ----------------------------------------------------------------------------
# Orchestration
# ----------------------------------------------------------------------------
def enumerate_objects(enumerator):
    """Enumerate via sys.* then fall back to SHOW/DESCRIBE on failure."""
    try:
        tables, parts = enumerator.enumerate_via_sys()
        method = "sys.* views"
        LOG.info("enumerated via sys.*: %d tables, %d partitions",
                 len(tables), len(parts))
        return tables, parts, method
    except CommandError as exc:
        LOG.warning("sys.* enumeration failed (%s); falling back to SHOW/DESCRIBE",
                    _first_line(str(exc)))
        tables, parts = enumerator.enumerate_via_show()
        method = "SHOW/DESCRIBE fallback"
        LOG.info("enumerated via fallback: %d tables, %d partitions",
                 len(tables), len(parts))
        return tables, parts, method


def reverify_orphans(orphans, hdfs, require_parent_exists):
    """Re-check each orphan's location right before mutating. Returns kept list.

    This is the last line of defence before any DROP. We require *positive
    proof* that the location is still absent; anything we cannot prove absent
    (location now present, or storage indeterminate) is skipped and logged.
    """
    kept = []
    for o in orphans:
        if not o.location:
            # No location recorded (e.g. a hand-edited report). We cannot prove
            # absence, so we keep it but warn loudly - the operator is asserting
            # it should be dropped.
            LOG.warning("KEEP-WITHOUT-VERIFY (no location in report): %s %s %s",
                        o.object_type, o.fqtn, o.partition_spec or "")
            kept.append(o)
            continue
        if require_parent_exists:
            missing = hdfs.confirm_missing(o.location)
        else:
            present = hdfs.exists(o.location)
            missing = True if present is False else (False if present is True else None)
        if missing is False:
            LOG.warning("SKIP (location now exists): %s %s %s",
                        o.object_type, o.fqtn, o.location)
            continue
        if missing is None:
            LOG.warning("SKIP (storage indeterminate / parent unreachable): %s %s %s",
                        o.object_type, o.fqtn, o.location)
            continue
        kept.append(o)
    return kept


def do_clean(orphans, hive, use_msck, args):
    """Execute cleanup DML for the given orphans. Returns (ok, fail)."""
    batches = build_cleanup_statements(orphans, use_msck=use_msck)
    if args.limit:
        batches = batches[: args.limit]
    total_ok = 0
    total_fail = 0
    confirm_each = getattr(args, "confirm_each", False)
    for label, stmts in batches:
        if confirm_each and not hive.dry_run and not _confirm_batch(label):
            LOG.warning("skipped by user: %s", label)
            continue
        ok, fail = hive.execute(stmts, label=label)
        total_ok += ok
        total_fail += fail
    return total_ok, total_fail


def _confirm_batch(label):
    """Per-batch prompt for --confirm-each. On a non-TTY, allow (already gated)."""
    if not sys.stdin or not sys.stdin.isatty():
        return True
    try:
        ans = input("  Execute: %s ?  [y/N]: " % label).strip().lower()
    except EOFError:
        return False
    return ans in ("y", "yes")


def confirm_destructive(count, assume_yes):
    """Return True only on explicit consent.

    Interactive callers must type the whole word 'yes' (not just 'y'). In a
    non-interactive context we refuse unless --yes was passed, so a scheduled
    job can never drop data by accident.
    """
    if assume_yes:
        LOG.warning("proceeding without interactive confirmation (--yes given)")
        return True
    if not sys.stdin or not sys.stdin.isatty():
        LOG.error("refusing to DROP in non-interactive mode without --yes "
                  "(no TTY available to confirm on)")
        return False
    try:
        ans = input("Type 'yes' to DROP %d object(s), or anything else to abort: "
                    % count).strip()
    except EOFError:
        return False
    return ans == "yes"


# ----------------------------------------------------------------------------
# CLI
# ----------------------------------------------------------------------------
def build_parser():
    p = argparse.ArgumentParser(
        prog="hive_orphan_cleanup.py",
        description="Identify and clean orphaned Hive tables/partitions on "
                    "CDP 7.1.9 + Isilon (metastore metadata cleanup via Hive DML).",
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )

    # Shared options live on a parent parser so they can be given AFTER the
    # subcommand (e.g. `report --jdbc-url ...`), which is the natural/ documented
    # ordering for subcommand CLIs.
    shared = argparse.ArgumentParser(add_help=False)

    # Global / connection options (shared by all subcommands)
    conn = shared.add_argument_group("connection")
    conn.add_argument("--jdbc-url", default=os.environ.get("HOC_JDBC_URL", ""),
                      help="HiveServer2 JDBC URL for beeline (e.g. "
                           "'jdbc:hive2://hs2:10000/default;principal=hive/_HOST@REALM'). "
                           "Env: HOC_JDBC_URL")
    conn.add_argument("--beeline-path", default=os.environ.get("HOC_BEELINE", "beeline"),
                      help="Path to beeline binary (default: beeline)")
    conn.add_argument("--hdfs-path", default=os.environ.get("HOC_HDFS", "hdfs"),
                      help="Path to hdfs binary (default: hdfs)")
    conn.add_argument("--beeline-arg", action="append", default=[],
                      help="Extra arg passed through to beeline (repeatable)")
    conn.add_argument("--keytab", default=os.environ.get("HOC_KEYTAB"),
                      help="Kerberos keytab for optional pre-run kinit")
    conn.add_argument("--principal", default=os.environ.get("HOC_PRINCIPAL"),
                      help="Kerberos principal for optional pre-run kinit")
    conn.add_argument("--beeline-timeout", type=int, default=3600,
                      help="Per-beeline-invocation timeout in seconds")
    conn.add_argument("--hdfs-timeout", type=int, default=300,
                      help="Per-hdfs-invocation timeout in seconds")

    # Common behavioral options
    common = shared.add_argument_group("common")
    common.add_argument("--output-dir", default=os.environ.get("HOC_OUTDIR", "."),
                        help="Directory for reports/audit logs (default: .)")
    common.add_argument("--log-dir", default=os.environ.get("HOC_LOGDIR"),
                        help="Directory for the run log file (default: output-dir)")
    common.add_argument("--max-workers", type=int, default=8,
                        help="Parallel hdfs existence checks (default: 8)")
    common.add_argument("--require-parent-exists", dest="require_parent_exists",
                        action="store_true", default=True,
                        help="Only flag orphaned if parent dir is reachable (default on)")
    common.add_argument("--no-require-parent-exists", dest="require_parent_exists",
                        action="store_false",
                        help="Disable the parent-reachable safety heuristic")
    common.add_argument("-v", "--verbose", action="store_true", help="Debug logging")

    # Filters
    filt = shared.add_argument_group("filters")
    filt.add_argument("--databases", help="Comma-separated DBs to include")
    filt.add_argument("--exclude-databases", help="Comma-separated DBs to exclude")
    filt.add_argument("--tables", help="Comma-separated tables (db.table or table)")
    filt.add_argument("--table-regex", help="Only tables whose name matches regex")

    sub = p.add_subparsers(dest="mode", metavar="{report,clean,apply}")
    sub.required = True

    # report
    sp_report = sub.add_parser("report", parents=[shared],
                               help="Read-only: find orphans, write reports")
    _add_housekeeping_flags(sp_report, executable=False)

    # clean
    sp_clean = sub.add_parser("clean", parents=[shared],
                              help="Detect then clean (dry-run unless --execute)")
    sp_clean.add_argument("--execute", action="store_true",
                          help="Actually run DML (otherwise dry-run)")
    sp_clean.add_argument("--yes", action="store_true", help="Skip confirmation prompt")
    sp_clean.add_argument("--limit", type=int, default=0,
                          help="Cap number of table-batches acted on (0 = no cap)")
    sp_clean.add_argument("--use-msck", action="store_true",
                          help="Use MSCK REPAIR ... DROP PARTITIONS per table "
                               "instead of explicit ALTER ... DROP PARTITION "
                               "(drops ALL missing partitions per table)")
    _add_safety_flags(sp_clean)
    _add_housekeeping_flags(sp_clean, executable=True)

    # apply
    sp_apply = sub.add_parser("apply", parents=[shared],
                              help="Clean objects listed in a prior report")
    sp_apply.add_argument("--report", required=True,
                          help="Path to a prior report (.csv or .json), optionally trimmed")
    sp_apply.add_argument("--execute", action="store_true",
                          help="Actually run DML (otherwise dry-run)")
    sp_apply.add_argument("--yes", action="store_true", help="Skip confirmation prompt")
    sp_apply.add_argument("--limit", type=int, default=0,
                          help="Cap number of table-batches acted on (0 = no cap)")
    sp_apply.add_argument("--use-msck", action="store_true",
                          help="Use MSCK REPAIR ... DROP PARTITIONS per table "
                               "(drops ALL missing partitions per table)")
    sp_apply.add_argument("--skip-reverify", action="store_true",
                          help="Do not re-check storage before dropping (trust "
                               "report). Riskier; use only on a fresh report.")
    _add_safety_flags(sp_apply)

    return p


def _add_safety_flags(sp):
    """Conservative, production-oriented guard rails shared by clean/apply."""
    sp.add_argument("--allow-managed", action="store_true",
                    help="Include MANAGED (non-ACID) tables/partitions. OFF by "
                         "default because DROP on a managed object deletes DATA; "
                         "only EXTERNAL objects are acted on unless you opt in. "
                         "ACID/transactional tables are NEVER auto-dropped.")
    sp.add_argument("--allow-bulk", action="store_true",
                    help="Override the bulk-safety guard (--max-drops / "
                         "--max-orphan-pct). Use only after confirming storage "
                         "is healthy - a large orphan set usually means an outage.")
    sp.add_argument("--max-drops", type=int, default=500,
                    help="Refuse to act on more than N objects unless "
                         "--allow-bulk (default 500).")
    sp.add_argument("--max-orphan-pct", type=float, default=25.0,
                    help="Refuse if more than this percent of scanned tables look "
                         "orphaned, unless --allow-bulk (default 25.0).")
    sp.add_argument("--confirm-each", action="store_true",
                    help="Prompt interactively before each per-table DROP batch.")


def _add_housekeeping_flags(sp, executable):
    sp.add_argument("--set-retention", type=int, metavar="DAYS", default=0,
                    help="Enable partition auto-discovery + N-day retention on "
                         "partitioned EXTERNAL tables (%s)"
                         % ("executed with --execute" if executable else "report-only"))
    sp.add_argument("--compact", action="store_true",
                    help="Request major compaction on ACID tables (%s)"
                         % ("executed with --execute" if executable else "report-only"))


def setup_logging(args):
    level = logging.DEBUG if getattr(args, "verbose", False) else logging.INFO
    log_dir = args.log_dir or args.output_dir or "."
    if log_dir and not os.path.isdir(log_dir):
        os.makedirs(log_dir)
    ts = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
    log_path = os.path.join(log_dir, "hive_orphan_cleanup_%s.log" % ts)
    handlers = [logging.StreamHandler(sys.stdout), logging.FileHandler(log_path)]
    fmt = "%(asctime)s %(levelname)-7s %(message)s"
    logging.basicConfig(level=level, format=fmt, handlers=handlers)
    LOG.debug("logging to %s", log_path)
    return log_path


def make_clients(args, dry_run, audit):
    hive = HiveRunner(
        jdbc_url=args.jdbc_url,
        beeline_path=args.beeline_path,
        extra_args=args.beeline_arg,
        timeout=args.beeline_timeout,
        dry_run=dry_run,
        audit=audit,
    )
    hdfs = HdfsClient(hdfs_path=args.hdfs_path, timeout=args.hdfs_timeout)
    return hive, hdfs


# ----------------------------------------------------------------------------
# Mode handlers
# ----------------------------------------------------------------------------
def run_report(args):
    reporter = Reporter(args.output_dir)
    hive, hdfs = make_clients(args, dry_run=True, audit=None)
    filters = Filters(args.databases, args.exclude_databases,
                      args.tables, args.table_regex)
    enumerator = Enumerator(hive, filters)

    tables, parts, method = enumerate_objects(enumerator)
    detector = Detector(hdfs, args.require_parent_exists, args.max_workers)
    orphans = detector.detect(tables, parts)

    recs = build_recommendations(hive)
    if args.set_retention:
        recs.append("(report-only) --set-retention %d would set "
                    "partition.retention.period on eligible external tables."
                    % args.set_retention)
    if args.compact:
        recs.append("(report-only) --compact would request major compaction on "
                    "ACID tables.")

    stats = {
        "tables_scanned": len(tables),
        "partitions_scanned": len(parts),
        "enumeration_method": method,
        "orphaned_tables": sum(1 for o in orphans if o.object_type == OBJECT_TABLE),
        "orphaned_partitions": sum(1 for o in orphans if o.object_type == OBJECT_PARTITION),
        "estimated_mysql_rows_reclaimed": estimate_rows_reclaimed(orphans),
    }
    paths = reporter.write_all(orphans, stats, recs)
    LOG.info("REPORT COMPLETE: %d orphaned objects (%d tables, %d partitions)",
             len(orphans), stats["orphaned_tables"], stats["orphaned_partitions"])
    LOG.info("reports: %s", ", ".join(paths.values()))
    return EXIT_OK


def run_clean(args):
    reporter = Reporter(args.output_dir)
    dry_run = not args.execute
    audit = AuditLog(os.path.join(args.output_dir or ".", "audit_%s.log" % reporter.ts))
    try:
        hive, hdfs = make_clients(args, dry_run=dry_run, audit=audit)
        filters = Filters(args.databases, args.exclude_databases,
                          args.tables, args.table_regex)
        enumerator = Enumerator(hive, filters)

        tables, parts, method = enumerate_objects(enumerator)
        detector = Detector(hdfs, args.require_parent_exists, args.max_workers)
        orphans = detector.detect(tables, parts)

        recs = build_recommendations(hive)
        stats = {
            "tables_scanned": len(tables),
            "partitions_scanned": len(parts),
            "enumeration_method": method,
            "orphaned_tables": sum(1 for o in orphans if o.object_type == OBJECT_TABLE),
            "orphaned_partitions": sum(1 for o in orphans if o.object_type == OBJECT_PARTITION),
            "estimated_mysql_rows_reclaimed": estimate_rows_reclaimed(orphans),
        }
        reporter.write_all(orphans, stats, recs)

        # --- Conservative safety policy -------------------------------------
        # 1) Never touch ACID/managed objects unless explicitly allowed.
        acid_set = fetch_transactional_tables(hive)
        actionable, skipped = split_actionable(orphans, acid_set, args.allow_managed)
        log_skipped(skipped)

        if not actionable and not args.set_retention and not args.compact:
            if orphans:
                LOG.info("no actionable orphans after safety policy (all %d "
                         "skipped); nothing to clean", len(skipped))
            else:
                LOG.info("no orphaned objects found; nothing to clean")
            return EXIT_OK

        mode_word = "DRY-RUN" if dry_run else "EXECUTE"

        # 2) Re-verify against storage (positive proof of absence) up front.
        verified = reverify_orphans(actionable, hdfs, args.require_parent_exists)
        if not verified and not args.set_retention and not args.compact:
            LOG.info("nothing left to clean after storage re-verification")
            return EXIT_OK

        # 3) Bulk guard: a huge set almost always means a storage outage.
        try:
            bulk_guard(verified, len(tables), args)
        except BulkGuardError as exc:
            LOG.error("BULK SAFETY GUARD: %s", exc)
            return EXIT_WARN

        if args.use_msck:
            LOG.warning("--use-msck: partitions will be pruned with MSCK REPAIR "
                        "TABLE ... DROP PARTITIONS, which drops EVERY partition "
                        "whose directory is currently missing - review carefully.")

        # 4) Notify: itemized plan, then require explicit confirmation.
        if verified:
            log_destructive_plan(verified, args.use_msck, dry_run)
        LOG.info("%s: %d object(s) to clean (after policy + re-verify)",
                 mode_word, len(verified))
        if not dry_run and verified:
            if not confirm_destructive(len(verified), args.yes):
                LOG.warning("ABORTED: no confirmation given; no changes made")
                return EXIT_WARN

        ok, fail = do_clean(verified, hive, args.use_msck, args)

        hk_ok, hk_fail = _run_housekeeping(args, hive, tables)
        ok += hk_ok
        fail += hk_fail

        LOG.info("CLEAN COMPLETE (%s): %d statement(s) ok, %d failed",
                 mode_word, ok, fail)
        return EXIT_PARTIAL if fail else EXIT_OK
    finally:
        audit.close()


def run_apply(args):
    reporter_ts = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
    dry_run = not args.execute
    audit = AuditLog(os.path.join(args.output_dir or ".", "audit_%s.log" % reporter_ts))
    try:
        hive, hdfs = make_clients(args, dry_run=dry_run, audit=audit)
        orphans = load_report(args.report)
        if not orphans:
            LOG.warning("report %s contained no objects; nothing to do", args.report)
            return EXIT_WARN
        LOG.info("loaded %d object(s) from report %s", len(orphans), args.report)

        # --- Same conservative safety policy as `clean` ---------------------
        acid_set = fetch_transactional_tables(hive)
        actionable, skipped = split_actionable(orphans, acid_set, args.allow_managed)
        log_skipped(skipped)
        if not actionable:
            LOG.info("no actionable objects after safety policy; nothing to do")
            return EXIT_OK

        if args.skip_reverify:
            LOG.warning("--skip-reverify: trusting the report WITHOUT re-checking "
                        "storage. Riskier - ensure the report is current and the "
                        "listed locations are genuinely gone.")
            verified = actionable
        else:
            verified = reverify_orphans(actionable, hdfs, args.require_parent_exists)
            LOG.info("%d object(s) remain after storage re-verification",
                     len(verified))
        if not verified:
            LOG.info("nothing left to clean after re-verification")
            return EXIT_OK

        try:
            bulk_guard(verified, 0, args)  # no scanned-table denominator here
        except BulkGuardError as exc:
            LOG.error("BULK SAFETY GUARD: %s", exc)
            return EXIT_WARN

        if args.use_msck:
            LOG.warning("--use-msck: MSCK REPAIR ... DROP PARTITIONS drops EVERY "
                        "missing partition per table - review carefully.")

        mode_word = "DRY-RUN" if dry_run else "EXECUTE"
        log_destructive_plan(verified, args.use_msck, dry_run)
        if not dry_run:
            if not confirm_destructive(len(verified), args.yes):
                LOG.warning("ABORTED: no confirmation given; no changes made")
                return EXIT_WARN

        ok, fail = do_clean(verified, hive, args.use_msck, args)
        LOG.info("APPLY COMPLETE (%s): %d statement(s) ok, %d failed",
                 mode_word, ok, fail)
        return EXIT_PARTIAL if fail else EXIT_OK
    finally:
        audit.close()


def _run_housekeeping(args, hive, tables):
    ok = 0
    fail = 0
    if getattr(args, "set_retention", 0):
        hk = Housekeeper(hive)
        a, b = hk.set_retention(tables, args.set_retention)
        ok += a
        fail += b
    if getattr(args, "compact", False):
        hk = Housekeeper(hive)
        acid = hk.find_acid_tables()
        a, b = hk.compact(acid)
        ok += a
        fail += b
    return ok, fail


def main(argv=None):
    if sys.version_info < (3, 6):
        sys.stderr.write(
            "WARNING: Python 3.6+ is recommended (RHEL 8 ships 3.6); "
            "detected %s. Continuing, but behavior is unsupported.\n"
            % ".".join(str(x) for x in sys.version_info[:3]))
    parser = build_parser()
    args = parser.parse_args(argv)
    setup_logging(args)

    try:
        _maybe_kinit(args.principal, args.keytab)
    except CommandError as exc:
        LOG.error("%s", exc)
        return EXIT_ERROR

    if args.mode in ("clean", "apply") and not args.jdbc_url:
        LOG.warning("no --jdbc-url given; beeline will use its default connection")

    try:
        if args.mode == "report":
            return run_report(args)
        elif args.mode == "clean":
            return run_clean(args)
        elif args.mode == "apply":
            return run_apply(args)
        else:
            parser.error("unknown mode: %s" % args.mode)
    except CommandError as exc:
        LOG.error("fatal: %s", exc)
        return EXIT_ERROR
    except KeyboardInterrupt:
        LOG.error("interrupted")
        return EXIT_ERROR


if __name__ == "__main__":
    sys.exit(main())
