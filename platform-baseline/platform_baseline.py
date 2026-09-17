#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
platform_baseline.py
====================

A quick, read-only *performance baseline & best-practices* reporter for a
CDP 7.1.9 (Hive 3) data platform. It is the "start simple" member of the
hive-tools family: point it at what you already have (a HiveServer2 JDBC URL,
Spark event logs, and/or HiveServer2 query logs) and it produces a consolidated
report of how the platform is configured and where the easy optimization wins
are - with a bias toward things that also relieve Hive Metastore (HMS) pressure.

It answers questions the core operations team asks first:

  * What STORAGE FORMATS and COMPRESSION codecs are actually in use, and how
    many tables use each? (text/CSV tables and missing compression are cheap,
    high-impact wins.)
  * How are tables TYPED (managed / external / ACID / view) and PARTITIONED?
    Which tables have so many partitions they drive get_partitions load on HMS?
  * Which tables have a SMALL-FILE problem (lots of tiny files)?
  * Do tables have STATISTICS (needed for good query plans / CBO)?
  * What key SPARK configs are set across applications (executor sizing,
    dynamic allocation, AQE, shuffle partitions, serializer, output codec)?
  * What is the HIVE query mix (operation types, common SET overrides, hottest
    tables) as seen in the query logs?

It is fully standalone: Python 3.6+, standard library only, and strictly
READ-ONLY. It never issues DDL/DML and never mutates the cluster.

------------------------------------------------------------------------------
INPUTS  ->  ANALYZERS  (each is optional; you get a section per input provided)
------------------------------------------------------------------------------
  --jdbc-url ...                 -> TABLES : storage/format/compression, table
                                             types, partitioning, stats, small
                                             files   (reads HMS via beeline sys.*)
  --spark-logs DIR|glob ...      -> SPARK  : per-app config counts + anti-patterns
                                             (reads Spark event-log JSON; .gz ok)
  --hive-logs  DIR|glob ...      -> HIVE   : query op mix, SET overrides, hot
                                             tables (reads HiveServer2 logs)

If you pass none of the above there is nothing to analyze (exit 3).

------------------------------------------------------------------------------
PROCESSING (all read-only; cheap gates before any parse/JSON)
------------------------------------------------------------------------------
  TABLES : beeline sys.* --> join TBLS/SDS/SERDES/TABLE_PARAMS/PARTITIONS
                          --> classify format+codec, bucket partitions, flag
                              small files / missing stats / non-columnar
  SPARK  : discover event logs --> substring-gate the 3-4 events we need
                          --> parse only those JSON lines (stop early)
                          --> tally key config value distributions + flags
  HIVE   : discover HS2 logs   --> regex "Executing command(queryId=..): SQL"
                          --> classify op, capture SET keys + referenced tables
  REPORT : one executive summary (TXT) + machine-readable JSON + focused CSVs

------------------------------------------------------------------------------
QUICK EXAMPLES
------------------------------------------------------------------------------
  # Storage/best-practice baseline from HMS (read-only)
  ./platform_baseline.py report \
      --jdbc-url 'jdbc:hive2://hs2:10000/default;principal=hive/_HOST@REALM' \
      --output-dir ./baseline_out

  # Add Spark app config baseline (event logs copied from HDFS or a local dir)
  ./platform_baseline.py report --spark-logs /var/log/spark2/apps \
      --output-dir ./baseline_out

  # Everything at once, limiting per-database scope and listing more rows
  ./platform_baseline.py report \
      --jdbc-url 'jdbc:hive2://hs2:10000/default' \
      --spark-logs '/data/spark-history/*' --hive-logs /var/log/hive \
      --databases sales,marketing --top 30 --output-dir ./baseline_out

------------------------------------------------------------------------------
EXIT CODES
------------------------------------------------------------------------------
  0  success (a report was written)
  1  runtime error (bad args, beeline failure on a requested TABLES run)
  3  soft warning (no inputs given, or nothing matched)
"""

from __future__ import print_function

import argparse
import csv
import datetime
import glob
import gzip
import io
import json
import logging
import os
import re
import shlex
import subprocess
import sys
from collections import Counter, OrderedDict, defaultdict

LOG = logging.getLogger("platform_baseline")

EXIT_OK = 0
EXIT_ERROR = 1
EXIT_WARN = 3

MB = 1024 * 1024


# ============================================================================
# Generic helpers
# ============================================================================
class CommandError(Exception):
    pass


class CommandResult(object):
    def __init__(self, returncode, stdout, stderr):
        self.returncode = returncode
        self.stdout = stdout
        self.stderr = stderr

    @property
    def ok(self):
        return self.returncode == 0


def _run(cmd, timeout=None):
    LOG.debug("exec: %s", " ".join(shlex.quote(c) for c in cmd))
    try:
        proc = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE,
                                universal_newlines=True)
        try:
            out, err = proc.communicate(timeout=timeout)
        except TypeError:
            out, err = proc.communicate()
    except subprocess.TimeoutExpired:
        proc.kill()
        out, err = proc.communicate()
        return CommandResult(124, out or "", (err or "") + "\n[timeout]")
    except OSError as exc:
        raise CommandError("failed to launch %r: %s" % (cmd[0], exc))
    return CommandResult(proc.returncode, out or "", err or "")


def _first_line(s):
    for ln in (s or "").splitlines():
        if ln.strip():
            return ln.strip()
    return ""


def _split_csv(val):
    if not val:
        return []
    if isinstance(val, (list, tuple)):
        return [v.strip() for v in val if v and str(v).strip()]
    return [v.strip() for v in str(val).split(",") if v.strip()]


def pct(part, whole):
    return (100.0 * part / whole) if whole else 0.0


def human_size(n):
    try:
        n = float(n)
    except (TypeError, ValueError):
        return "?"
    for unit in ("B", "KB", "MB", "GB", "TB", "PB"):
        if abs(n) < 1024.0:
            return "%.1f %s" % (n, unit)
        n /= 1024.0
    return "%.1f EB" % n


def _bar(count, total, width=22):
    n = int(round(width * count / total)) if total else 0
    return "#" * n + "." * (width - n)


def open_text(path):
    """Open a plain or .gz text file for reading (utf-8, lenient)."""
    if path.endswith(".gz"):
        return io.TextIOWrapper(gzip.open(path, "rb"), encoding="utf-8", errors="replace")
    return io.open(path, "r", encoding="utf-8", errors="replace")


def discover_files(paths):
    """Expand dirs (recursively), globs, and plain files into a de-duped list."""
    out = []
    for p in paths or []:
        if os.path.isdir(p):
            for root, _dirs, files in os.walk(p):
                for f in files:
                    out.append(os.path.join(root, f))
        elif any(ch in p for ch in "*?[]"):
            out.extend(glob.glob(p, recursive=True))
        elif os.path.isfile(p):
            out.append(p)
        else:
            LOG.warning("input path not found: %s", p)
    seen, res = set(), []
    for f in out:
        if f not in seen and os.path.isfile(f):
            seen.add(f)
            res.append(f)
    return res


# --- beeline (read-only) ----------------------------------------------------
_NOISE_PREFIXES = ("Connecting to ", "Connected to:", "Driver:",
                   "Transaction isolation:", "Beeline version", "Closing:",
                   "INFO ", "WARN ", "0: jdbc:")


def _is_noise(line):
    s = line.strip()
    return any(s.startswith(p) for p in _NOISE_PREFIXES)


def _parse_csv2(text):
    lines = [ln for ln in (text or "").splitlines()
             if ln.strip() and not _is_noise(ln)]
    if not lines:
        return []
    rows = list(csv.reader(lines))
    if not rows:
        return []
    header = [h.strip() for h in rows[0]]
    out = []
    for r in rows[1:]:
        if len(r) < len(header):
            r = r + [""] * (len(header) - len(r))
        out.append(dict(zip(header, r)))
    return out


class HiveRunner(object):
    """Minimal read-only beeline wrapper returning list[dict] from csv2 output."""

    def __init__(self, jdbc_url, beeline_path="beeline", extra_args=None, timeout=3600):
        self.jdbc_url = jdbc_url
        self.beeline_path = beeline_path
        self.extra_args = extra_args or []
        self.timeout = timeout

    def _base_cmd(self):
        cmd = [self.beeline_path]
        if self.jdbc_url:
            cmd += ["-u", self.jdbc_url]
        cmd += list(self.extra_args)
        cmd += ["--silent=true", "--verbose=false", "--showHeader=true",
                "--outputformat=csv2", "--nullemptystring=true"]
        return cmd

    def query(self, sql):
        res = _run(self._base_cmd() + ["-e", sql], timeout=self.timeout)
        if not res.ok:
            raise CommandError("beeline query failed (rc=%s): %s"
                               % (res.returncode, _first_line(res.stderr)))
        return _parse_csv2(res.stdout)


def maybe_kinit(principal, keytab):
    if not (keytab and principal):
        return
    LOG.info("kinit %s using keytab %s", principal, keytab)
    res = _run(["kinit", "-kt", keytab, principal], timeout=60)
    if not res.ok:
        raise CommandError("kinit failed: %s" % _first_line(res.stderr))


class Filters(object):
    def __init__(self, databases=None, exclude_databases=None, tables=None,
                 table_regex=None):
        self.databases = _split_csv(databases)
        self.exclude_databases = _split_csv(exclude_databases)
        self.tables = set(_split_csv(tables))
        self.table_regex = re.compile(table_regex) if table_regex else None

    def accept(self, db, tbl):
        if self.databases and db not in self.databases:
            return False
        if self.exclude_databases and db in self.exclude_databases:
            return False
        if self.tables and ("%s.%s" % (db, tbl) not in self.tables
                            and tbl not in self.tables):
            return False
        if self.table_regex and not self.table_regex.search(tbl or ""):
            return False
        return True

    def db_predicates(self, alias="d"):
        preds = []
        if self.databases:
            preds.append("%s.NAME IN (%s)"
                         % (alias, ",".join("'%s'" % d for d in self.databases)))
        if self.exclude_databases:
            preds.append("%s.NAME NOT IN (%s)"
                         % (alias, ",".join("'%s'" % d for d in self.exclude_databases)))
        return preds


# ============================================================================
# Analyzer 1: TABLES  (storage / format / compression / partitions / stats)
# ============================================================================
Q_TABLES = """
SELECT t.TBL_ID AS tbl_id, d.NAME AS db, t.TBL_NAME AS tbl, t.TBL_TYPE AS tbl_type,
       s.INPUT_FORMAT AS input_format, s.OUTPUT_FORMAT AS output_format,
       s.IS_COMPRESSED AS is_compressed, ser.SLIB AS serde_lib
FROM sys.TBLS t
JOIN sys.DBS d ON t.DB_ID = d.DB_ID
LEFT JOIN sys.SDS s ON t.SD_ID = s.SD_ID
LEFT JOIN sys.SERDES ser ON s.SERDE_ID = ser.SERDE_ID
"""

Q_TABLES_NOSERDE = """
SELECT t.TBL_ID AS tbl_id, d.NAME AS db, t.TBL_NAME AS tbl, t.TBL_TYPE AS tbl_type,
       s.INPUT_FORMAT AS input_format, s.OUTPUT_FORMAT AS output_format,
       s.IS_COMPRESSED AS is_compressed, '' AS serde_lib
FROM sys.TBLS t
JOIN sys.DBS d ON t.DB_ID = d.DB_ID
LEFT JOIN sys.SDS s ON t.SD_ID = s.SD_ID
"""

TABLE_PARAM_KEYS = ("transient_lastDdlTime", "numRows", "totalSize", "numFiles",
                    "rawDataSize", "COLUMN_STATS_ACCURATE", "transactional",
                    "transactional_properties", "orc.compress",
                    "parquet.compression", "EXTERNAL")

Q_PARAMS_BASE = """
SELECT tp.TBL_ID AS tbl_id, tp.PARAM_KEY AS k, tp.PARAM_VALUE AS v
FROM sys.TABLE_PARAMS tp
JOIN sys.TBLS t ON tp.TBL_ID = t.TBL_ID
JOIN sys.DBS d ON t.DB_ID = d.DB_ID
WHERE tp.PARAM_KEY IN (%s)
""" % ",".join("'%s'" % k for k in TABLE_PARAM_KEYS)

PART_BUCKETS = (("0 (unpartitioned)", 0, 0), ("1-10", 1, 10),
                ("11-100", 11, 100), ("101-1,000", 101, 1000),
                ("1,001-10,000", 1001, 10000), ("10,001-100,000", 10001, 100000),
                ("100,000+", 100001, None))

TABLE_DETAIL_COLUMNS = ["db", "table", "type", "format", "compression",
                        "partitions", "num_files", "total_size", "num_rows",
                        "basic_stats", "col_stats", "avg_file_size"]

_COLUMNAR = ("ORC", "PARQUET")


def classify_format(input_format, serde_lib, output_format, tbl_type):
    if "VIEW" in (tbl_type or "").upper():
        return "VIEW"
    s = " ".join(x or "" for x in (input_format, serde_lib, output_format)).lower()
    if not s.strip():
        return "UNKNOWN"
    if "orc" in s:
        return "ORC"
    if "parquet" in s:
        return "PARQUET"
    if "avro" in s:
        return "AVRO"
    if "rcfile" in s or "columnarserde" in s:
        return "RCFILE"
    if "sequencefile" in s:
        return "SEQUENCEFILE"
    if "hbase" in s:
        return "HBASE"
    if "kafka" in s:
        return "KAFKA"
    if "jdbc" in s or "druid" in s:
        return "JDBC/EXT"
    if "opencsv" in s:
        return "CSV"
    if "json" in s:
        return "JSON"
    if "regexserde" in s:
        return "TEXT(regex)"
    if "text" in s or "lazysimple" in s:
        return "TEXT"
    return "OTHER"


def classify_type(tbl_type, params):
    tt = (tbl_type or "").upper()
    if "VIEW" in tt:
        return "VIEW"
    if (params.get("transactional", "") or "").lower() == "true":
        return "ACID"
    if "EXTERNAL" in tt or (params.get("EXTERNAL", "") or "").upper() == "TRUE":
        return "EXTERNAL"
    if "MANAGED" in tt:
        return "MANAGED"
    return tt or "UNKNOWN"


def detect_compression(fmt, params, is_compressed):
    if fmt == "ORC":
        c = params.get("orc.compress")
        return c.upper() if c else "unset (ORC default ZLIB)"
    if fmt == "PARQUET":
        c = params.get("parquet.compression")
        return c.upper() if c else "unset (writer default)"
    if fmt == "VIEW":
        return "n/a (view)"
    if (is_compressed or "").lower() == "true":
        return "compressed (unspecified)"
    return "none / unknown"


def _part_bucket(n):
    for label, lo, hi in PART_BUCKETS:
        if n >= lo and (hi is None or n <= hi):
            return label
    return "?"


def _int(params, key):
    try:
        return int(float(params[key]))
    except (KeyError, ValueError, TypeError):
        return None


def analyze_tables(hive, filters, small_file_mb=128, high_partition=10000, top_n=20):
    """Return the TABLES baseline dict. Raises CommandError if HMS is unreadable."""
    preds = filters.db_predicates("d")
    tbl_sql = Q_TABLES + (" WHERE " + " AND ".join(preds) if preds else "")
    serde_used = True
    try:
        rows = hive.query(tbl_sql)
    except CommandError as exc:
        LOG.warning("sys.SERDES join failed (%s); retrying without serde detail",
                    _first_line(str(exc)))
        serde_used = False
        nos = Q_TABLES_NOSERDE + (" WHERE " + " AND ".join(preds) if preds else "")
        rows = hive.query(nos)

    tables = {}          # tbl_id -> record
    for r in rows:
        db = (r.get("db") or "").strip()
        tbl = (r.get("tbl") or "").strip()
        if not tbl or not filters.accept(db, tbl):
            continue
        tables[r.get("tbl_id")] = {
            "db": db, "table": tbl, "tbl_type": (r.get("tbl_type") or "").strip(),
            "input_format": r.get("input_format") or "",
            "output_format": r.get("output_format") or "",
            "serde_lib": r.get("serde_lib") or "",
            "is_compressed": r.get("is_compressed") or "",
            "params": {}, "partitions": 0,
        }

    # table params (bucketed key set)
    params_sql = Q_PARAMS_BASE + ("".join(" AND " + p for p in preds) if preds else "")
    try:
        for r in hive.query(params_sql):
            rec = tables.get(r.get("tbl_id"))
            if rec is not None:
                rec["params"][r.get("k")] = r.get("v")
    except CommandError as exc:
        LOG.warning("TABLE_PARAMS query failed (%s); stats/compression detail limited",
                    _first_line(str(exc)))

    # partition counts
    part_sql = ("SELECT t.TBL_ID AS tbl_id, COUNT(*) AS n FROM sys.PARTITIONS p "
                "JOIN sys.TBLS t ON p.TBL_ID = t.TBL_ID "
                "JOIN sys.DBS d ON t.DB_ID = d.DB_ID"
                + (" WHERE " + " AND ".join(preds) if preds else "")
                + " GROUP BY t.TBL_ID")
    try:
        for r in hive.query(part_sql):
            rec = tables.get(r.get("tbl_id"))
            if rec is not None:
                rec["partitions"] = _int({"n": r.get("n")}, "n") or 0
    except CommandError as exc:
        LOG.warning("PARTITIONS count query failed (%s); partition detail omitted",
                    _first_line(str(exc)))

    # ---- derive per-table facts + aggregate ----
    fmt_counts = Counter()
    comp_counts = Counter()
    type_counts = Counter()
    part_bucket_counts = OrderedDict((b[0], 0) for b in PART_BUCKETS)
    detail = []
    small_files = []
    high_part = []
    non_columnar = []
    managed_nonacid = []
    missing_basic = 0
    missing_col = 0
    basic_present = 0
    col_present = 0
    partitioned = 0

    for rec in tables.values():
        p = rec["params"]
        fmt = classify_format(rec["input_format"], rec["serde_lib"],
                              rec["output_format"], rec["tbl_type"])
        ttype = classify_type(rec["tbl_type"], p)
        comp = detect_compression(fmt, p, rec["is_compressed"])
        nfiles = _int(p, "numFiles")
        tsize = _int(p, "totalSize")
        nrows = _int(p, "numRows")
        avg = (tsize / nfiles) if (nfiles and tsize is not None and nfiles > 0) else None
        has_basic = "numRows" in p
        csa = p.get("COLUMN_STATS_ACCURATE", "") or ""
        has_col = "COLUMN_STATS" in csa
        fqn = "%s.%s" % (rec["db"], rec["table"])

        fmt_counts[fmt] += 1
        comp_counts[comp if comp.startswith(("ZLIB", "SNAPPY", "GZIP", "ZSTD",
                                             "LZ4", "LZO", "NONE", "none", "unset",
                                             "compressed", "n/a")) else comp] += 1
        type_counts[ttype] += 1
        part_bucket_counts[_part_bucket(rec["partitions"])] += 1
        if rec["partitions"] > 0:
            partitioned += 1

        if fmt not in _COLUMNAR and fmt not in ("VIEW", "HBASE", "KAFKA", "JDBC/EXT"):
            non_columnar.append((fqn, fmt))
        if ttype == "MANAGED":
            managed_nonacid.append(fqn)
        if has_basic:
            basic_present += 1
        elif fmt != "VIEW":
            missing_basic += 1
        if has_col:
            col_present += 1
        elif fmt != "VIEW":
            missing_col += 1
        if rec["partitions"] >= high_partition:
            high_part.append((fqn, rec["partitions"]))
        if (nfiles is not None and nfiles >= 8 and avg is not None
                and avg < small_file_mb * MB):
            small_files.append({"table": fqn, "num_files": nfiles,
                                "total_size": tsize or 0, "avg_file_size": int(avg)})

        detail.append({
            "db": rec["db"], "table": rec["table"], "type": ttype, "format": fmt,
            "compression": comp, "partitions": rec["partitions"],
            "num_files": nfiles if nfiles is not None else "",
            "total_size": tsize if tsize is not None else "",
            "num_rows": nrows if nrows is not None else "",
            "basic_stats": "yes" if has_basic else "no",
            "col_stats": "yes" if has_col else "no",
            "avg_file_size": int(avg) if avg is not None else "",
        })

    small_files.sort(key=lambda x: x["num_files"], reverse=True)
    high_part.sort(key=lambda x: x[1], reverse=True)
    top_part = sorted(((d["db"] + "." + d["table"], d["partitions"]) for d in detail),
                      key=lambda x: x[1], reverse=True)[:top_n]
    detail.sort(key=lambda d: (d["db"], d["table"]))

    n = len(tables)
    n_base = sum(1 for d in detail if d["format"] != "VIEW")
    recs = _table_recommendations(non_columnar, comp_counts, managed_nonacid,
                                  high_part, missing_basic, missing_col,
                                  small_files, n_base, high_partition, small_file_mb)

    return {
        "n_tables": n,
        "n_base_tables": n_base,
        "serde_detail": serde_used,
        "format_counts": _counter_rows(fmt_counts, n),
        "compression_counts": _counter_rows(comp_counts, n),
        "type_counts": _counter_rows(type_counts, n),
        "partitioning": {
            "partitioned": partitioned, "unpartitioned": n - partitioned,
            "buckets": list(part_bucket_counts.items()),
            "top": top_part,
        },
        "stats": {"basic_present": basic_present, "basic_missing": missing_basic,
                  "col_present": col_present, "col_missing": missing_col},
        "small_files": {"threshold_mb": small_file_mb, "count": len(small_files),
                        "top": small_files[:top_n]},
        "flags": {
            "non_columnar": {"count": len(non_columnar),
                             "samples": [x[0] for x in non_columnar[:top_n]]},
            "managed_nonacid": {"count": len(managed_nonacid),
                                "samples": managed_nonacid[:top_n]},
            "high_partition": {"count": len(high_part), "threshold": high_partition,
                               "samples": ["%s (%d)" % (f, c) for f, c in high_part[:top_n]]},
        },
        "recommendations": recs,
        "detail": detail,
    }


def _counter_rows(counter, total):
    return [(k, c, round(pct(c, total), 1))
            for k, c in counter.most_common()]


def _table_recommendations(non_columnar, comp_counts, managed_nonacid, high_part,
                           missing_basic, missing_col, small_files, n_base,
                           high_partition, small_file_mb):
    recs = []
    if non_columnar:
        recs.append("%d of %d base tables are NOT columnar (text/CSV/JSON/etc.). "
                    "Converting hot ones to ORC or Parquet cuts scan time, storage, "
                    "and split/file counts. e.g. %s"
                    % (len(non_columnar), n_base,
                       ", ".join(x[0] for x in non_columnar[:5])))
    uncompressed = sum(c for k, c in comp_counts.items()
                       if k.startswith(("none", "NONE")))
    if uncompressed:
        recs.append("%d tables report no/unknown compression. Set orc.compress "
                    "(SNAPPY/ZLIB) or parquet.compression (SNAPPY/ZSTD) to shrink "
                    "storage and I/O." % uncompressed)
    if high_part:
        recs.append("%d table(s) have >= %s partitions - a top driver of HMS "
                    "get_partitions load. Review partition granularity and enable "
                    "partition retention on high-churn externals. e.g. %s"
                    % (len(high_part), "{:,}".format(high_partition),
                       ", ".join("%s (%d)" % (f, c) for f, c in high_part[:5])))
    if small_files["count"] if isinstance(small_files, dict) else small_files:
        sf = small_files if isinstance(small_files, list) else small_files
        cnt = len(sf)
        recs.append("%d table(s) show a small-file pattern (many files averaging "
                    "< %d MB). Compact/insert-overwrite or tune writer file sizes "
                    "to reduce NameNode/HMS object counts." % (cnt, small_file_mb))
    if missing_basic:
        recs.append("%d table(s) lack basic statistics. Run ANALYZE TABLE ... "
                    "COMPUTE STATISTICS so the optimizer sizes joins correctly."
                    % missing_basic)
    if missing_col:
        recs.append("%d table(s) lack column statistics. ANALYZE ... FOR COLUMNS "
                    "enables CBO and better plans (also reduces ret/replan load)."
                    % missing_col)
    if managed_nonacid:
        recs.append("%d MANAGED non-ACID table(s) found. In Hive 3, managed tables "
                    "are expected to be transactional; verify these are intentional "
                    "(otherwise EXTERNAL or ACID is usually correct)."
                    % len(managed_nonacid))
    if not recs:
        recs.append("No obvious table-level anti-patterns detected in this scope.")
    return recs


# ============================================================================
# Analyzer 2: SPARK  (per-application config from event logs)
# ============================================================================
SPARK_KEY_GROUPS = OrderedDict([
    ("Resources", ["spark.executor.memory", "spark.executor.cores",
                   "spark.executor.instances", "spark.executor.memoryOverhead",
                   "spark.driver.memory", "spark.driver.cores"]),
    ("Elasticity", ["spark.dynamicAllocation.enabled",
                    "spark.dynamicAllocation.maxExecutors",
                    "spark.shuffle.service.enabled"]),
    ("SQL / tuning", ["spark.sql.shuffle.partitions", "spark.default.parallelism",
                      "spark.sql.adaptive.enabled",
                      "spark.sql.adaptive.coalescePartitions.enabled",
                      "spark.sql.cbo.enabled",
                      "spark.sql.autoBroadcastJoinThreshold"]),
    ("Serialization", ["spark.serializer"]),
    ("File format / compression", ["spark.sql.sources.default",
                                   "spark.sql.parquet.compression.codec",
                                   "spark.sql.orc.compression.codec"]),
    ("Runtime", ["spark.master", "spark.submit.deployMode"]),
])
SPARK_KEYS = [k for group in SPARK_KEY_GROUPS.values() for k in group]

SPARK_APP_COLUMNS = ["app_id", "app_name", "user", "version", "start",
                     "executor_memory", "executor_cores", "dynamic_allocation",
                     "shuffle_partitions", "adaptive", "serializer",
                     "parquet_codec"]


def _spark_flags(props):
    """Return list of anti-pattern ids this app trips."""
    tripped = []
    if props.get("spark.dynamicAllocation.enabled", "false").lower() != "true":
        tripped.append("dynamic_allocation_off")
    if props.get("spark.sql.adaptive.enabled", "false").lower() != "true":
        tripped.append("aqe_off")
    if "kryo" not in props.get("spark.serializer", "").lower():
        tripped.append("java_serializer")
    if "spark.sql.shuffle.partitions" not in props:
        tripped.append("default_shuffle_partitions")
    if props.get("spark.sql.cbo.enabled", "false").lower() != "true":
        tripped.append("cbo_off")
    return tripped


SPARK_FLAG_DESC = {
    "dynamic_allocation_off": "dynamic allocation disabled (fixed executors waste/queue resources)",
    "aqe_off": "Adaptive Query Execution off (misses skew/coalesce optimizations)",
    "java_serializer": "not using KryoSerializer (slower, larger shuffle)",
    "default_shuffle_partitions": "spark.sql.shuffle.partitions unset (defaults to 200)",
    "cbo_off": "cost-based optimizer disabled",
}


def _parse_spark_eventlog(path):
    """Return an app dict from one event log, or None if it isn't a Spark log."""
    props, app = {}, {}
    version = [None]
    end = [None]
    seen_env = seen_start = False
    try:
        with open_text(path) as fh:
            for line in fh:
                if "SparkListener" not in line:
                    continue
                try:
                    if not seen_env and "SparkListenerEnvironmentUpdate" in line:
                        ev = json.loads(line)
                        for kv in ev.get("Spark Properties", []) or []:
                            if isinstance(kv, list) and len(kv) == 2:
                                props[kv[0]] = kv[1]
                        seen_env = True
                    elif not seen_start and "SparkListenerApplicationStart" in line:
                        ev = json.loads(line)
                        app = {"app_id": ev.get("App ID") or "",
                               "app_name": ev.get("App Name") or "",
                               "user": ev.get("Spark User") or "",
                               "start": ev.get("Timestamp")}
                        seen_start = True
                    elif version[0] is None and "SparkListenerLogStart" in line:
                        version[0] = json.loads(line).get("Spark Version")
                    elif end[0] is None and "SparkListenerApplicationEnd" in line:
                        end[0] = json.loads(line).get("Timestamp")
                except (ValueError, TypeError):
                    continue
                if seen_env and seen_start and version[0] is not None:
                    break
    except (IOError, OSError) as exc:
        LOG.debug("could not read %s: %s", path, exc)
        return None
    if not (seen_env or seen_start):
        return None
    app.setdefault("app_id", os.path.basename(path))
    app["version"] = version[0] or ""
    app["props"] = props
    if app.get("start") and end[0]:
        try:
            app["duration_s"] = int((int(end[0]) - int(app["start"])) / 1000)
        except (ValueError, TypeError):
            app["duration_s"] = ""
    else:
        app["duration_s"] = ""
    return app


def analyze_spark(paths, top_n=20):
    files = discover_files(paths)
    LOG.info("spark: scanning %d candidate event-log file(s)", len(files))
    apps = []
    parse_skips = 0
    for f in files:
        a = _parse_spark_eventlog(f)
        if a is None:
            parse_skips += 1
            continue
        apps.append(a)

    value_counts = OrderedDict((k, Counter()) for k in SPARK_KEYS)
    flag_counts = defaultdict(list)
    users = Counter()
    versions = Counter()
    app_rows = []
    for a in apps:
        p = a["props"]
        for k in SPARK_KEYS:
            value_counts[k][p.get(k, "(unset)")] += 1
        for fid in _spark_flags(p):
            flag_counts[fid].append(a.get("app_id", "?"))
        if a.get("user"):
            users[a["user"]] += 1
        if a.get("version"):
            versions[a["version"]] += 1
        app_rows.append({
            "app_id": a.get("app_id", ""), "app_name": a.get("app_name", ""),
            "user": a.get("user", ""), "version": a.get("version", ""),
            "start": _ms_to_iso(a.get("start")),
            "executor_memory": p.get("spark.executor.memory", ""),
            "executor_cores": p.get("spark.executor.cores", ""),
            "dynamic_allocation": p.get("spark.dynamicAllocation.enabled", ""),
            "shuffle_partitions": p.get("spark.sql.shuffle.partitions", ""),
            "adaptive": p.get("spark.sql.adaptive.enabled", ""),
            "serializer": p.get("spark.serializer", "").split(".")[-1],
            "parquet_codec": p.get("spark.sql.parquet.compression.codec", ""),
        })

    config_counts = OrderedDict()
    for group, keys in SPARK_KEY_GROUPS.items():
        config_counts[group] = [(k, value_counts[k].most_common(8)) for k in keys]

    flags = OrderedDict()
    for fid in ("dynamic_allocation_off", "aqe_off", "java_serializer",
                "default_shuffle_partitions", "cbo_off"):
        ids = flag_counts.get(fid, [])
        flags[fid] = {"count": len(ids), "desc": SPARK_FLAG_DESC[fid],
                      "samples": ids[:top_n]}

    return {
        "n_files_scanned": len(files),
        "n_apps": len(apps),
        "n_skipped": parse_skips,
        "versions": versions.most_common(),
        "users": users.most_common(top_n),
        "config_counts": config_counts,
        "flags": flags,
        "apps": app_rows,
    }


def _ms_to_iso(ms):
    try:
        return datetime.datetime.fromtimestamp(int(ms) / 1000.0).isoformat(
            timespec="seconds")
    except (TypeError, ValueError, OverflowError, OSError):
        return ""


# ============================================================================
# Analyzer 3: HIVE query logs  (op mix, SET overrides, hot tables) best-effort
# ============================================================================
_EXEC_RE = re.compile(r"Executing command\(queryId=([^)]*)\):\s*(.+)")
_TIME_RE = re.compile(r"Time taken:\s*([0-9]+(?:\.[0-9]+)?)\s*seconds")
_TABLE_RE = re.compile(
    r"\b(?:FROM|JOIN|INTO|OVERWRITE\s+TABLE|INTO\s+TABLE|UPDATE|TABLE)\s+"
    r"`?([A-Za-z0-9_]+`?\.`?[A-Za-z0-9_]+|[A-Za-z0-9_]+)`?", re.I)

_OP_SIMPLE = {"SELECT", "INSERT", "CREATE", "DROP", "ALTER", "SET", "MSCK",
              "ANALYZE", "TRUNCATE", "DELETE", "UPDATE", "MERGE", "EXPORT",
              "IMPORT", "SHOW", "DESCRIBE", "USE", "LOAD", "GRANT", "REVOKE"}


def sql_operation(sql):
    s = sql.strip().lstrip("(").upper()
    m = re.match(r"([A-Z]+)", s)
    if not m:
        return "OTHER"
    w = m.group(1)
    if w == "WITH":
        return "SELECT"
    return w if w in _OP_SIMPLE else "OTHER"


def _clean_table(tok):
    return tok.replace("`", "").strip().lower()


def analyze_hive_logs(paths, top_n=20):
    files = discover_files(paths)
    LOG.info("hive: scanning %d log file(s)", len(files))
    ops = Counter()
    set_keys = Counter()
    tables = Counter()
    engines = Counter()
    query_ids = set()
    n_queries = 0
    durations = []
    matched_files = 0

    for path in files:
        got = False
        try:
            with open_text(path) as fh:
                for line in fh:
                    if "Time taken:" in line:
                        m = _TIME_RE.search(line)
                        if m:
                            try:
                                durations.append(float(m.group(1)))
                            except ValueError:
                                pass
                    if "Executing command(queryId=" not in line:
                        continue
                    m = _EXEC_RE.search(line)
                    if not m:
                        continue
                    got = True
                    n_queries += 1
                    qid, sql = m.group(1), m.group(2).strip()
                    if qid:
                        query_ids.add(qid)
                    op = sql_operation(sql)
                    ops[op] += 1
                    if op == "SET":
                        body = sql.strip()[3:].strip().rstrip(";")
                        if "=" in body:
                            key = body.split("=", 1)[0].strip()
                            if key:
                                set_keys[key] += 1
                                if key == "hive.execution.engine":
                                    engines[body.split("=", 1)[1].strip()] += 1
                    else:
                        for t in _TABLE_RE.findall(sql):
                            ct = _clean_table(t)
                            if ct and not ct.isdigit():
                                tables[ct] += 1
        except (IOError, OSError) as exc:
            LOG.debug("could not read %s: %s", path, exc)
            continue
        if got:
            matched_files += 1

    dur = None
    if durations:
        dur = {"count": len(durations),
               "avg_s": round(sum(durations) / len(durations), 2),
               "max_s": round(max(durations), 2)}

    return {
        "n_files": len(files),
        "n_files_matched": matched_files,
        "n_queries": n_queries,
        "distinct_query_ids": len(query_ids),
        "ops": ops.most_common(),
        "set_keys": set_keys.most_common(top_n),
        "tables": tables.most_common(top_n),
        "engines": engines.most_common(),
        "durations": dur,
    }


# ============================================================================
# Reporting
# ============================================================================
class Reporter(object):
    def __init__(self, output_dir):
        self.output_dir = output_dir or "."
        if not os.path.isdir(self.output_dir):
            os.makedirs(self.output_dir)
        self.ts = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")

    def _p(self, name):
        return os.path.join(self.output_dir, name)

    def write_all(self, results, top_n=20):
        outputs = OrderedDict()
        summary_path = self._p("baseline_summary_%s.txt" % self.ts)
        json_path = self._p("baseline_%s.json" % self.ts)
        with open(json_path, "w") as fh:
            json.dump({"generated_at": datetime.datetime.now().isoformat(),
                       "results": results}, fh, indent=2, default=str)
        outputs["json"] = json_path

        if "tables" in results:
            outputs.update(self._write_table_csvs(results["tables"]))
        if "spark" in results:
            outputs.update(self._write_spark_csvs(results["spark"]))
        if "hive" in results:
            outputs.update(self._write_hive_csvs(results["hive"]))

        text = self._render_summary(results, top_n)
        with open(summary_path, "w") as fh:
            fh.write(text)
        outputs["summary"] = summary_path
        return outputs, text

    # ---- CSVs ----
    def _dump_csv(self, name, columns, rows):
        path = self._p(name)
        with open(path, "w") as fh:
            w = csv.DictWriter(fh, fieldnames=columns)
            w.writeheader()
            for r in rows:
                w.writerow({c: r.get(c, "") for c in columns})
        return path

    def _write_table_csvs(self, t):
        out = {}
        out["tables_detail_csv"] = self._dump_csv(
            "baseline_tables_detail_%s.csv" % self.ts, TABLE_DETAIL_COLUMNS, t["detail"])
        out["small_files_csv"] = self._dump_csv(
            "baseline_small_files_%s.csv" % self.ts,
            ["table", "num_files", "total_size", "avg_file_size"], t["small_files"]["top"])
        # format/compression/type counts as one tall CSV
        rows = ([{"dimension": "format", "value": k, "count": c, "pct": p}
                 for k, c, p in t["format_counts"]]
                + [{"dimension": "compression", "value": k, "count": c, "pct": p}
                   for k, c, p in t["compression_counts"]]
                + [{"dimension": "type", "value": k, "count": c, "pct": p}
                   for k, c, p in t["type_counts"]])
        out["config_counts_csv"] = self._dump_csv(
            "baseline_table_config_counts_%s.csv" % self.ts,
            ["dimension", "value", "count", "pct"], rows)
        return out

    def _write_spark_csvs(self, s):
        out = {}
        out["spark_apps_csv"] = self._dump_csv(
            "baseline_spark_apps_%s.csv" % self.ts, SPARK_APP_COLUMNS, s["apps"])
        flat = []
        for group, keys in s["config_counts"].items():
            for key, valuelist in keys:
                for value, count in valuelist:
                    flat.append({"group": group, "key": key, "value": value,
                                 "app_count": count})
        out["spark_config_csv"] = self._dump_csv(
            "baseline_spark_config_counts_%s.csv" % self.ts,
            ["group", "key", "value", "app_count"], flat)
        return out

    def _write_hive_csvs(self, h):
        out = {}
        out["hive_ops_csv"] = self._dump_csv(
            "baseline_hive_ops_%s.csv" % self.ts, ["operation", "count"],
            [{"operation": k, "count": c} for k, c in h["ops"]])
        out["hive_set_csv"] = self._dump_csv(
            "baseline_hive_set_params_%s.csv" % self.ts, ["set_key", "count"],
            [{"set_key": k, "count": c} for k, c in h["set_keys"]])
        out["hive_tables_csv"] = self._dump_csv(
            "baseline_hive_hot_tables_%s.csv" % self.ts, ["table", "references"],
            [{"table": k, "references": c} for k, c in h["tables"]])
        return out

    # ---- human summary ----
    def _render_summary(self, results, top_n):
        L = []
        L.append("=" * 74)
        L.append("PLATFORM PERFORMANCE BASELINE")
        L.append("Generated: %s" % datetime.datetime.now().isoformat())
        L.append("Sections : %s" % ", ".join(k.upper() for k in results) or "(none)")
        L.append("=" * 74)
        L.append("")
        if "tables" in results:
            self._section_tables(L, results["tables"], top_n)
        if "spark" in results:
            self._section_spark(L, results["spark"], top_n)
        if "hive" in results:
            self._section_hive(L, results["hive"], top_n)
        L.append("=" * 74)
        L.append("Full detail: baseline_%s.json + the baseline_*_%s.csv files"
                 % (self.ts, self.ts))
        L.append("This tool is READ-ONLY; all recommendations are advisory.")
        L.append("=" * 74)
        return "\n".join(L) + "\n"

    def _dist(self, L, title, rows, total, indent="  "):
        L.append("%s%s" % (indent, title))
        if not rows:
            L.append("%s  (none)" % indent)
            return
        width = max(len(str(k)) for k, _c, _p in rows)
        width = min(max(width, 8), 34)
        for k, c, p in rows:
            L.append("%s  %-*s %6d  %5.1f%%  %s"
                     % (indent, width, str(k)[:width], c, p, _bar(c, total)))

    def _section_tables(self, L, t, top_n):
        L.append("-" * 74)
        L.append("TABLES  (%d total; %d base tables, %d views)  [source: HMS sys.*]"
                 % (t["n_tables"], t["n_base_tables"], t["n_tables"] - t["n_base_tables"]))
        L.append("-" * 74)
        if not t["serde_detail"]:
            L.append("  NOTE: sys.SERDES was unavailable; text/CSV/JSON not distinguished.")
        self._dist(L, "Storage format:", t["format_counts"], t["n_tables"])
        L.append("")
        self._dist(L, "Compression codec:", t["compression_counts"], t["n_tables"])
        L.append("")
        self._dist(L, "Table type:", t["type_counts"], t["n_tables"])
        L.append("")
        pinfo = t["partitioning"]
        L.append("  Partitioning: %d partitioned, %d unpartitioned"
                 % (pinfo["partitioned"], pinfo["unpartitioned"]))
        for label, count in pinfo["buckets"]:
            if count:
                L.append("    %-18s %6d" % (label, count))
        if pinfo["top"]:
            L.append("  Most-partitioned tables (get_partitions pressure):")
            for fqn, c in pinfo["top"][:min(top_n, 10)]:
                if c:
                    L.append("    %-52s %8d partitions" % (fqn, c))
        L.append("")
        st = t["stats"]
        L.append("  Statistics coverage (base tables):")
        L.append("    basic stats : %d present / %d missing" % (st["basic_present"], st["basic_missing"]))
        L.append("    column stats: %d present / %d missing" % (st["col_present"], st["col_missing"]))
        sf = t["small_files"]
        L.append("")
        L.append("  Small-file offenders (>=8 files avg < %d MB): %d table(s)"
                 % (sf["threshold_mb"], sf["count"]))
        for row in sf["top"][:min(top_n, 10)]:
            L.append("    %-52s %6d files  avg %s"
                     % (row["table"], row["num_files"], human_size(row["avg_file_size"])))
        L.append("")
        L.append("  RECOMMENDATIONS:")
        for r in t["recommendations"]:
            L.extend(_wrap("- " + r, 70, "    "))
        L.append("")

    def _section_spark(self, L, s, top_n):
        L.append("-" * 74)
        L.append("SPARK  (%d applications from %d event-log file(s); %d skipped)"
                 % (s["n_apps"], s["n_files_scanned"], s["n_skipped"]))
        L.append("-" * 74)
        if not s["n_apps"]:
            L.append("  No Spark event logs parsed. Point --spark-logs at the event-log dir")
            L.append("  (e.g. spark.eventLog.dir / Spark History server storage).")
            L.append("")
            return
        if s["versions"]:
            L.append("  Spark versions: " + ", ".join("%s (%d)" % (v, c)
                     for v, c in s["versions"]))
        if s["users"]:
            L.append("  Top users: " + ", ".join("%s (%d)" % (u, c)
                     for u, c in s["users"][:min(top_n, 8)]))
        L.append("")
        for group, keys in s["config_counts"].items():
            L.append("  %s:" % group)
            for key, valuelist in keys:
                shown = ", ".join("%s=%d" % (val, cnt) for val, cnt in valuelist[:5])
                L.append("    %-42s %s" % (key, shown or "(none)"))
        L.append("")
        L.append("  Anti-pattern prevalence (apps affected of %d):" % s["n_apps"])
        for fid, info in s["flags"].items():
            L.append("    %-28s %5d  (%4.1f%%)  %s"
                     % (fid, info["count"], pct(info["count"], s["n_apps"]), info["desc"]))
        L.append("")

    def _section_hive(self, L, h, top_n):
        L.append("-" * 74)
        L.append("HIVE QUERY LOGS  (%d queries across %d/%d file(s))  [best-effort]"
                 % (h["n_queries"], h["n_files_matched"], h["n_files"]))
        L.append("-" * 74)
        if not h["n_queries"]:
            L.append("  No 'Executing command(queryId=...)' lines found. Point --hive-logs")
            L.append("  at HiveServer2 logs (hadoop-cmf-*-HIVESERVER2-*.log[.gz]).")
            L.append("")
            return
        L.append("  Distinct queryIds: %d" % h["distinct_query_ids"])
        if h["durations"]:
            d = h["durations"]
            L.append("  Durations seen: %d (avg %.1fs, max %.1fs)"
                     % (d["count"], d["avg_s"], d["max_s"]))
        L.append("  Operation mix:")
        total_ops = sum(c for _k, c in h["ops"]) or 1
        for op, c in h["ops"]:
            L.append("    %-12s %6d  %5.1f%%  %s" % (op, c, pct(c, total_ops), _bar(c, total_ops)))
        if h["engines"]:
            L.append("  Execution engine (from SET hive.execution.engine): "
                     + ", ".join("%s (%d)" % (e, c) for e, c in h["engines"]))
        if h["set_keys"]:
            L.append("  Most common SET overrides:")
            for k, c in h["set_keys"][:min(top_n, 12)]:
                L.append("    %-44s %6d" % (k, c))
        if h["tables"]:
            L.append("  Hottest tables (by references in query text):")
            for tbl, c in h["tables"][:min(top_n, 12)]:
                L.append("    %-52s %6d" % (tbl, c))
        L.append("")


def _wrap(text, width, indent):
    words = text.split()
    lines, cur = [], indent
    for w in words:
        if len(cur) + len(w) + 1 > width and cur.strip():
            lines.append(cur)
            cur = indent + "  " + w
        else:
            cur += (" " if cur.strip() else "") + w
    if cur.strip():
        lines.append(cur)
    return lines


# ============================================================================
# CLI
# ============================================================================
def build_parser():
    p = argparse.ArgumentParser(
        prog="platform_baseline.py",
        description="Read-only performance baseline & best-practices reporter for "
                    "Hive tables, Spark apps, and Hive query logs (CDP 7.1.9).",
        formatter_class=argparse.RawDescriptionHelpFormatter)
    sub = p.add_subparsers(dest="mode", metavar="{report}")
    sub.required = True

    r = sub.add_parser("report", help="Analyze available inputs and write a report",
                       formatter_class=argparse.RawDescriptionHelpFormatter)

    src = r.add_argument_group("inputs (provide one or more)")
    src.add_argument("--jdbc-url", default=os.environ.get("PB_JDBC_URL", ""),
                     help="HiveServer2 JDBC URL -> enables the TABLES analysis "
                          "(reads HMS via beeline sys.*). Env: PB_JDBC_URL")
    src.add_argument("--spark-logs", nargs="+", default=[], metavar="PATH",
                     help="Spark event-log dirs/globs/files -> enables SPARK analysis")
    src.add_argument("--hive-logs", nargs="+", default=[], metavar="PATH",
                     help="HiveServer2 log dirs/globs/files -> enables HIVE analysis")

    conn = r.add_argument_group("beeline (for --jdbc-url)")
    conn.add_argument("--beeline-path", default=os.environ.get("PB_BEELINE", "beeline"))
    conn.add_argument("--beeline-arg", action="append", default=[],
                      help="Extra arg passed through to beeline (repeatable)")
    conn.add_argument("--beeline-timeout", type=int, default=3600)
    conn.add_argument("--keytab", default=os.environ.get("PB_KEYTAB"))
    conn.add_argument("--principal", default=os.environ.get("PB_PRINCIPAL"))

    filt = r.add_argument_group("table filters")
    filt.add_argument("--databases", help="Comma-separated DBs to include")
    filt.add_argument("--exclude-databases", help="Comma-separated DBs to exclude")
    filt.add_argument("--tables", help="Comma-separated tables (db.table or table)")
    filt.add_argument("--table-regex", help="Only tables whose name matches regex")

    opt = r.add_argument_group("options")
    opt.add_argument("--only", help="Restrict analyzers: csv of tables,spark,hive")
    opt.add_argument("--output-dir", default=os.environ.get("PB_OUTDIR", "."),
                     help="Directory for the report/JSON/CSVs (default: .)")
    opt.add_argument("--top", type=int, default=20,
                     help="Rows to show/list for top-N sections (default 20)")
    opt.add_argument("--small-file-mb", type=int, default=128,
                     help="Average file size below which a table is a small-file "
                          "offender (default 128 MB)")
    opt.add_argument("--high-partition", type=int, default=10000,
                     help="Flag tables with at least this many partitions (default 10000)")
    opt.add_argument("-v", "--verbose", action="store_true", help="Debug logging")
    return p


def setup_logging(verbose):
    logging.basicConfig(level=logging.DEBUG if verbose else logging.INFO,
                        format="%(asctime)s %(levelname)-7s %(message)s",
                        handlers=[logging.StreamHandler(sys.stdout)])


def run_report(args):
    only = set(_split_csv(args.only)) if args.only else {"tables", "spark", "hive"}
    results = OrderedDict()

    if "tables" in only and args.jdbc_url:
        try:
            maybe_kinit(args.principal, args.keytab)
        except CommandError as exc:
            LOG.error("%s", exc)
            return EXIT_ERROR
        hive = HiveRunner(args.jdbc_url, args.beeline_path, args.beeline_arg,
                          args.beeline_timeout)
        filters = Filters(args.databases, args.exclude_databases, args.tables,
                          args.table_regex)
        try:
            LOG.info("TABLES: querying HMS via beeline sys.* ...")
            results["tables"] = analyze_tables(hive, filters, args.small_file_mb,
                                               args.high_partition, args.top)
            LOG.info("TABLES: analyzed %d tables", results["tables"]["n_tables"])
        except CommandError as exc:
            LOG.error("TABLES analysis failed: %s", exc)
            return EXIT_ERROR
    elif "tables" in only and not args.jdbc_url and args.only:
        LOG.warning("TABLES requested but no --jdbc-url provided; skipping.")

    if "spark" in only and args.spark_logs:
        results["spark"] = analyze_spark(args.spark_logs, args.top)
        LOG.info("SPARK: parsed %d application(s)", results["spark"]["n_apps"])

    if "hive" in only and args.hive_logs:
        results["hive"] = analyze_hive_logs(args.hive_logs, args.top)
        LOG.info("HIVE: parsed %d queries", results["hive"]["n_queries"])

    if not results:
        LOG.error("Nothing to analyze. Provide --jdbc-url and/or --spark-logs "
                  "and/or --hive-logs. See --help.")
        return EXIT_WARN

    reporter = Reporter(args.output_dir)
    outputs, text = reporter.write_all(results, args.top)
    print("")
    print(text)
    LOG.info("wrote: %s", ", ".join(outputs.values()))
    return EXIT_OK


def main(argv=None):
    if sys.version_info < (3, 6):
        sys.stderr.write("WARNING: Python 3.6+ recommended; detected %s\n"
                         % ".".join(str(x) for x in sys.version_info[:3]))
    args = build_parser().parse_args(argv)
    setup_logging(getattr(args, "verbose", False))
    try:
        if args.mode == "report":
            return run_report(args)
    except CommandError as exc:
        LOG.error("fatal: %s", exc)
        return EXIT_ERROR
    except KeyboardInterrupt:
        LOG.error("interrupted")
        return EXIT_ERROR
    return EXIT_OK


if __name__ == "__main__":
    sys.exit(main())
