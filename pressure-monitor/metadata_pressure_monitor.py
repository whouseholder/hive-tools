#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
metadata_pressure_monitor.py
============================

Identify sources of excessive Hive Metastore (HMS) metadata pressure on the
MySQL backend of a CDP 7.1.9 (Hive 3) cluster.

Where the companion partition_ops_monitor.py focuses narrowly on
drop_partition / add_partitions / MSCK, this tool generalizes to *any* HMS API
that is hammering the metastore, and answers:

  - Which HMS methods dominate call volume and total time?
  - Which Kerberos IDs (ugi) and client IPs are generating the pressure?
  - Which HMS host instances are hottest (fleet skew)?
  - Which tables are hot?
  - How does load trend over time (per bucket) - where are the storms?
  - Which patterns look like loops / repetitive calls, and how do we fix them?

It is fully standalone: Python 3.6+, standard library only, no imports from the
partition_ops_monitor framework.

------------------------------------------------------------------------------
INPUTS
------------------------------------------------------------------------------
It reads Hive Metastore server logs (Cloudera hadoop-cmf-hive-HIVEMETASTORE-*
role logs, or hms-audit / hms-perf logs). It understands two line shapes:

  PERFLOG (has method + duration):
    ... PerfLogger: [..WorkerProcess-84]: </PERFLOG method=get_table_req
        start=.. end=.. duration=2026 .. threadId=925 retryCount=0 error=false>

  AUDIT (has ugi + ip + cmd):
    ... HiveMetaStore.audit: [..WorkerProcess-84]: ugi=user/host@REALM
        ip=10.0.0.1  cmd=get_partitions_by_expr : tbl=cat.db.table ...

Gzipped (*.gz) and plain files are both supported, as are directories
(scanned recursively) and globs. The HMS hostname is taken from the CM log
filename when present (...HIVEMETASTORE-<host>.log...).

------------------------------------------------------------------------------
MODES
------------------------------------------------------------------------------
  report    Ad-hoc. Scan historical logs (fleet-wide) over a time window and
            write an executive summary + JSON + CSVs ranking pressure sources
            and prescribing remediation.
  monitor   Scheduled. Incrementally tail live logs, keep a rolling window in a
            small event store, and emit de-duplicated alerts (stdout + email).

------------------------------------------------------------------------------
QUICK EXAMPLES
------------------------------------------------------------------------------
  # Ad-hoc report over a directory of captured HMS logs (gz ok), last 24h
  ./metadata_pressure_monitor.py report --paths /path/to/hms_logs --last 24h \
      --output-dir ./pressure_out

  # Same, but ignore delegation-token noise and show more rows
  ./metadata_pressure_monitor.py report --paths '/logs/**/HIVEMETASTORE-*.gz' \
      --exclude-methods get_token --top 30 --output-dir ./pressure_out

  # Scheduled single pass (cron), using a config file
  ./metadata_pressure_monitor.py monitor --config metadata_pressure_monitor.config.json --once

------------------------------------------------------------------------------
EXIT CODES
------------------------------------------------------------------------------
  0  success
  1  runtime error (bad args, unreadable inputs)
  2  findings at or above the alert severity were produced (useful for cron)
  3  soft warning (no input lines matched)
"""

from __future__ import print_function

import argparse
import csv
import datetime
import glob
import gzip
import hashlib
import io
import json
import logging
import os
import random
import re
import smtplib
import sys
import time
from collections import Counter, defaultdict
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText

LOG = logging.getLogger("metadata_pressure_monitor")

EXIT_OK = 0
EXIT_ERROR = 1
EXIT_FINDINGS = 2
EXIT_WARN = 3

# ----------------------------------------------------------------------------
# Regexes (grounded in real CDP 7.1.9 HMS logs)
# ----------------------------------------------------------------------------
HOST_PREFIX_RE = re.compile(r"^(?P<host>[A-Za-z0-9._-]+)\s+\|\s+(?P<line>.+)$")
TS_RE = re.compile(r"^(\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}),(\d{3})")
PERF_END_RE = re.compile(
    r"</PERFLOG method=(?P<method>[^ ]+) start=(?P<start>\d+) end=(?P<end>\d+) "
    r"duration=(?P<duration>\d+).*?threadId=(?P<thread_id>\d+)"
)
AUDIT_RE = re.compile(
    r"HiveMetaStore\.audit:.*?ugi=(?P<ugi>\S+)\s+ip=(?P<ip>\S+)\s+cmd=(?P<cmd>[^:]+):?\s*(?P<detail>.*)$"
)
HOST_FROM_FILE_RE = re.compile(r"HIVEMETASTORE-(?P<host>.+?)\.log", re.IGNORECASE)

# Table extraction from audit detail (best effort; several HMS shapes).
TBL_FQ_RE = re.compile(r"tbl=(?:(?P<cat>\w+)\.)?(?P<db>[^.\s]+)\.(?P<table>\S+)")
DB_TAB_RE = re.compile(r"\bdb=(?P<db>\S+)\s+tab=(?P<table>\S+)")
DBNAME_TBLNAME_RE = re.compile(r"dbName[=:](?P<db>\S+)\s+tbl(?:Name)?[=:](?P<table>\S+)")

WINDOW_RE = re.compile(r"^(?P<n>\d+)\s*(?P<u>[smhdw])$", re.IGNORECASE)
_UNIT_SECONDS = {"s": 1, "m": 60, "h": 3600, "d": 86400, "w": 604800}


# ----------------------------------------------------------------------------
# Method categorization
# ----------------------------------------------------------------------------
CAT_AUTH = "AUTH"
CAT_CONSTRAINTS = "CONSTRAINTS"
CAT_STATS = "STATS"
CAT_PARTITION_WRITE = "PARTITION_WRITE"
CAT_PARTITION_READ = "PARTITION_READ"
CAT_DDL = "DDL"
CAT_METADATA_READ = "METADATA_READ"
CAT_CACHE = "CACHE"
CAT_OTHER = "OTHER"

_AUTH_METHODS = {
    "get_token", "add_token", "remove_token", "renew_token",
    "get_delegation_token", "renew_delegation_token", "cancel_delegation_token",
    "get_master_keys", "add_master_key", "update_master_key", "remove_master_key",
}
_CONSTRAINT_METHODS = {
    "get_primary_keys", "get_foreign_keys", "get_not_null_constraints",
    "get_unique_constraints", "get_check_constraints", "get_default_constraints",
    "get_all_table_constraints",
}
_CACHE_METHODS = {"flushcache", "flush_cache"}


def canonical_method(method):
    """Normalize a PERFLOG method or audit cmd token to a canonical name.

    - takes the first whitespace token (audit cmd may be 'get_token for')
    - strips a trailing '_req' so PERFLOG 'get_table_req' merges with audit
      'get_table'
    """
    if not method:
        return ""
    tok = method.strip().split()[0]
    if tok.endswith("_req"):
        tok = tok[:-4]
    return tok


def categorize(method):
    """Classify a canonical method name into a pressure category."""
    m = method.lower()
    if m in _AUTH_METHODS or m.endswith("_token") or "delegation_token" in m or "master_key" in m:
        return CAT_AUTH
    if m in _CONSTRAINT_METHODS or m.endswith("_constraints"):
        return CAT_CONSTRAINTS
    if "stats" in m or "statistics" in m:
        return CAT_STATS
    if m in _CACHE_METHODS:
        return CAT_CACHE
    if "partition" in m:
        if any(w in m for w in ("add_partition", "append_partition", "drop_partition",
                                "alter_partition", "rename_partition",
                                "exchange_partition", "mark_partition")):
            return CAT_PARTITION_WRITE
        return CAT_PARTITION_READ
    if any(m.startswith(p) for p in ("create_", "drop_", "alter_", "truncate_",
                                     "rename_")) and "partition" not in m:
        return CAT_DDL
    if m.startswith("get") or m.startswith("getmetaconf"):
        return CAT_METADATA_READ
    return CAT_OTHER


# ----------------------------------------------------------------------------
# Time helpers
# ----------------------------------------------------------------------------
def parse_ts(line):
    m = TS_RE.match(line)
    if not m:
        return None
    dt = datetime.datetime.strptime(m.group(1), "%Y-%m-%d %H:%M:%S")
    return dt.timestamp() + int(m.group(2)) / 1000.0


def parse_window(spec):
    """'6h' / '30m' / '2d' -> seconds. Returns None on failure."""
    if not spec:
        return None
    m = WINDOW_RE.match(str(spec).strip())
    if not m:
        return None
    return int(m.group("n")) * _UNIT_SECONDS[m.group("u").lower()]


def parse_iso(spec):
    """Parse an ISO-ish timestamp to epoch seconds."""
    if not spec:
        return None
    spec = str(spec).strip()
    for fmt in ("%Y-%m-%d %H:%M:%S", "%Y-%m-%dT%H:%M:%S", "%Y-%m-%d %H:%M", "%Y-%m-%d"):
        try:
            return datetime.datetime.strptime(spec, fmt).timestamp()
        except ValueError:
            continue
    return None


def human_ts(epoch):
    if not epoch:
        return "-"
    return datetime.datetime.fromtimestamp(epoch).strftime("%Y-%m-%d %H:%M:%S")


def bucket_key(epoch, bucket_seconds):
    return int(epoch // bucket_seconds) * int(bucket_seconds)


# ----------------------------------------------------------------------------
# Kerberos ID normalization
# ----------------------------------------------------------------------------
def normalize_ugi(ugi):
    """Return (short_user, principal, realm) from a ugi string.

    'user/host@REALM' -> ('user', 'user/host@REALM', 'REALM')
    'user@REALM'      -> ('user', 'user@REALM', 'REALM')
    'user'            -> ('user', 'user', '')
    """
    if not ugi:
        return ("", "", "")
    principal = ugi.strip()
    realm = ""
    body = principal
    if "@" in principal:
        body, realm = principal.split("@", 1)
    short = body.split("/", 1)[0] if "/" in body else body
    return (short, principal, realm)


# ----------------------------------------------------------------------------
# Table extraction (best effort)
# ----------------------------------------------------------------------------
def extract_table(detail):
    if not detail:
        return ("", "")
    m = TBL_FQ_RE.search(detail)
    if m:
        return (m.group("db"), m.group("table").strip("`"))
    m = DB_TAB_RE.search(detail)
    if m:
        return (m.group("db").strip("`"), m.group("table").strip("`"))
    m = DBNAME_TBLNAME_RE.search(detail)
    if m:
        return (m.group("db").strip("`"), m.group("table").strip("`"))
    return ("", "")


# ----------------------------------------------------------------------------
# File discovery + gzip-aware reading
# ----------------------------------------------------------------------------
def discover_files(paths):
    """Expand a list of files / globs / directories into a sorted file list."""
    out = []
    seen = set()

    def add(fp):
        if fp not in seen and os.path.isfile(fp):
            seen.add(fp)
            out.append(fp)

    for p in paths:
        if os.path.isdir(p):
            for root, _dirs, files in os.walk(p):
                for name in files:
                    add(os.path.join(root, name))
        elif any(ch in p for ch in "*?["):
            for fp in glob.glob(p, recursive=True):
                add(fp)
        else:
            add(p)
    return sorted(out)


def host_from_filename(path):
    m = HOST_FROM_FILE_RE.search(os.path.basename(path))
    return m.group("host") if m else ""


def open_text(path):
    """Open plain or gzip file as a text stream."""
    if path.endswith(".gz"):
        raw = gzip.open(path, "rb")
        return io.TextIOWrapper(raw, encoding="utf-8", errors="replace")
    return open(path, "r", encoding="utf-8", errors="replace")


def iter_lines(path):
    """Yield (line_no, line) for a plain or gz file."""
    try:
        fh = open_text(path)
    except (OSError, IOError) as exc:
        LOG.warning("cannot open %s: %s", path, exc)
        return
    with fh:
        for line_no, line in enumerate(fh, start=1):
            yield line_no, line.rstrip("\n")


def scan_incremental(path, offset, handler):
    """Tail an (uncompressed) file from a byte offset; returns new offset."""
    if not os.path.isfile(path) or path.endswith(".gz"):
        return offset
    size = os.path.getsize(path)
    if offset > size:
        offset = 0  # rotated/truncated
    with open(path, "r", encoding="utf-8", errors="replace") as fh:
        fh.seek(offset)
        for line_no, line in enumerate(fh, start=1):
            handler(line_no, line.rstrip("\n"))
        return fh.tell()


# ----------------------------------------------------------------------------
# Line parsing
# ----------------------------------------------------------------------------
def parse_line(raw_line):
    """Parse one raw log line.

    Returns (perf, audit) where each is a dict or None.
      perf  = {ts, method, duration_ms, thread_id}
      audit = {ts, ugi, ip, cmd, method, db, table}
    """
    line = raw_line
    m = HOST_PREFIX_RE.match(raw_line)
    if m:
        line = m.group("line")
    ts = parse_ts(line)
    if ts is None:
        return (None, None)

    perf = None
    pm = PERF_END_RE.search(line)
    if pm:
        perf = {
            "ts": ts,
            "method": canonical_method(pm.group("method")),
            "duration_ms": int(pm.group("duration")),
            "thread_id": pm.group("thread_id"),
        }

    audit = None
    if "HiveMetaStore.audit:" in line:
        am = AUDIT_RE.search(line)
        if am:
            db, table = extract_table(am.group("detail"))
            audit = {
                "ts": ts,
                "ugi": am.group("ugi"),
                "ip": am.group("ip"),
                "cmd": am.group("cmd").strip(),
                "method": canonical_method(am.group("cmd")),
                "db": db,
                "table": table,
            }
    return (perf, audit)


# ----------------------------------------------------------------------------
# Reservoir timer (bounded-memory percentile estimate)
# ----------------------------------------------------------------------------
class Timer(object):
    __slots__ = ("count", "total", "max", "_sample", "_cap", "_seen")

    def __init__(self, cap=4000):
        self.count = 0
        self.total = 0
        self.max = 0
        self._sample = []
        self._cap = cap
        self._seen = 0

    def add(self, ms):
        self.count += 1
        self.total += ms
        if ms > self.max:
            self.max = ms
        self._seen += 1
        if len(self._sample) < self._cap:
            self._sample.append(ms)
        else:
            j = random.randint(0, self._seen - 1)
            if j < self._cap:
                self._sample[j] = ms

    @property
    def avg(self):
        return (self.total / self.count) if self.count else 0

    def percentile(self, pct):
        if not self._sample:
            return 0
        s = sorted(self._sample)
        idx = min(len(s) - 1, int(round((pct / 100.0) * (len(s) - 1))))
        return s[idx]


# ----------------------------------------------------------------------------
# Analyzer: aggregate parsed events across all dimensions
# ----------------------------------------------------------------------------
class Analyzer(object):
    def __init__(self, bucket_seconds=3600, loop_window_seconds=60,
                 track_key_cap=300000):
        self.bucket_seconds = int(bucket_seconds)
        self.loop_window_seconds = int(loop_window_seconds)
        self.track_key_cap = int(track_key_cap)

        self.total_audit = 0
        self.total_perf = 0
        self.min_ts = None
        self.max_ts = None

        self.audit_by_method = Counter()
        self.perf_timer = defaultdict(Timer)          # method -> Timer
        self.by_category_calls = Counter()             # from audit
        self.by_category_time = Counter()              # from perf (ms)
        self.by_ugi = Counter()                        # short user
        self.ugi_methods = defaultdict(Counter)        # short user -> method counts
        self.ugi_principal = defaultdict(Counter)      # short user -> principal counts
        self.by_ip = Counter()
        self.by_host = Counter()
        self.host_methods = defaultdict(Counter)
        self.by_bucket = Counter()
        self.bucket_methods = defaultdict(Counter)
        self.by_table = Counter()

        # loop / repetition detection: key -> {bucket -> count}
        self._loop = defaultdict(lambda: defaultdict(int))
        self._loop_keys_full = False

    def _note_ts(self, ts):
        if self.min_ts is None or ts < self.min_ts:
            self.min_ts = ts
        if self.max_ts is None or ts > self.max_ts:
            self.max_ts = ts

    def ingest_perf(self, perf, host=""):
        method = perf["method"]
        if not method:
            return
        self.total_perf += 1
        self._note_ts(perf["ts"])
        self.perf_timer[method].add(perf["duration_ms"])
        self.by_category_time[categorize(method)] += perf["duration_ms"]

    def ingest_audit(self, audit, host=""):
        method = audit["method"]
        if not method:
            return
        self.total_audit += 1
        ts = audit["ts"]
        self._note_ts(ts)
        cat = categorize(method)
        self.audit_by_method[method] += 1
        self.by_category_calls[cat] += 1

        short, principal, _realm = normalize_ugi(audit.get("ugi", ""))
        if short:
            self.by_ugi[short] += 1
            self.ugi_methods[short][method] += 1
            self.ugi_principal[short][principal] += 1
        ip = audit.get("ip", "")
        if ip:
            self.by_ip[ip] += 1
        if host:
            self.by_host[host] += 1
            self.host_methods[host][method] += 1

        b = bucket_key(ts, self.bucket_seconds)
        self.by_bucket[b] += 1
        self.bucket_methods[b][method] += 1

        table = audit.get("table", "")
        if table:
            fq = "{0}.{1}".format(audit.get("db", "") or "?", table)
            self.by_table[fq] += 1

        # loop tracking (bounded)
        lb = bucket_key(ts, self.loop_window_seconds)
        key = (short, ip, method, table)
        if key in self._loop or not self._loop_keys_full:
            self._loop[key][lb] += 1
            if len(self._loop) >= self.track_key_cap:
                self._loop_keys_full = True

    def duration_seconds(self):
        if self.min_ts is None or self.max_ts is None:
            return 0
        return max(1.0, self.max_ts - self.min_ts)

    def loop_peaks(self):
        """Peak calls-in-window per (ugi,ip,method,table)."""
        peaks = []
        for (short, ip, method, table), buckets in self._loop.items():
            if not buckets:
                continue
            peak = max(buckets.values())
            total = sum(buckets.values())
            peaks.append({
                "ugi": short, "ip": ip, "method": method, "table": table,
                "peak_in_window": peak, "total": total,
                "window_seconds": self.loop_window_seconds,
                "rate_per_sec": round(peak / float(self.loop_window_seconds), 2),
            })
        return peaks


# ----------------------------------------------------------------------------
# Findings + remediation catalog
# ----------------------------------------------------------------------------
SEV_ORDER = {"INFO": 0, "WARN": 1, "CRITICAL": 2}


class Finding(object):
    def __init__(self, severity, ftype, message, evidence, resolution):
        self.severity = severity
        self.ftype = ftype
        self.message = message
        self.evidence = evidence or {}
        self.resolution = resolution
        self.fingerprint = hashlib.sha256(
            ("{0}|{1}|{2}".format(ftype, message,
                                  json.dumps(self.evidence, sort_keys=True))
             ).encode("utf-8")).hexdigest()[:16]

    def to_dict(self):
        return {
            "severity": self.severity,
            "type": self.ftype,
            "message": self.message,
            "evidence": self.evidence,
            "resolution": self.resolution,
            "fingerprint": self.fingerprint,
        }


DEFAULT_THRESHOLDS = {
    "min_total_calls_for_findings": 500,
    "method_share_pct": 25.0,
    "token_share_pct": 35.0,
    "ugi_share_pct": 25.0,
    "ip_share_pct": 25.0,
    "host_skew_pct": 60.0,
    "loop_min_peak": 300,
    "loop_min_rate_per_sec": 15.0,
    "fetch_all_partitions_calls": 500,
    "partition_write_calls": 500,
    "stats_calls": 500,
    "constraints_share_pct": 20.0,
    "bucket_spike_factor": 3.0,
    "slow_method_avg_ms": 1000,
    "slow_method_min_calls": 50,
    "top_findings_per_type": 8,
}


def _pct(part, whole):
    return (100.0 * part / whole) if whole else 0.0


def _resolution_for_method(method, cat):
    if cat == CAT_PARTITION_READ:
        return ("Add partition pruning/predicates and prefer get_partitions_by_expr; "
                "cap results with metastore.limit.partition.request.")
    if cat == CAT_PARTITION_WRITE:
        return ("Batch partition writes (multi-partition ADD, dynamic-partition INSERT); "
                "avoid MSCK loops.")
    if cat == CAT_STATS:
        return ("Tune stats fetch/gather (hive.stats.fetch.partition.stats, aggregate "
                "stats cache) and run ANALYZE off-peak.")
    if cat == CAT_METADATA_READ:
        return ("Reduce repeated table/db opens via HS2 metastore caching and session "
                "reuse; batch lookups (get_table_objects_by_name).")
    if cat == CAT_AUTH:
        return "Move delegation-token store to ZooKeeper and reuse connections/tokens."
    if cat == CAT_CONSTRAINTS:
        return ("Constraint lookups fire per table open in Hive 3; reduce open frequency "
                "via caching.")
    return "Investigate the driving workload and batch/cache its metadata access."


def run_detectors(an, thresholds, top_n=20):
    """Evaluate all detectors against an Analyzer, return list[Finding]."""
    t = dict(DEFAULT_THRESHOLDS)
    t.update(thresholds or {})
    findings = []
    total_calls = an.total_audit or sum(an.audit_by_method.values())
    if total_calls < t["min_total_calls_for_findings"]:
        return findings

    # 1) Dominant methods by call share (auth handled separately)
    for method, count in an.audit_by_method.most_common(top_n):
        share = _pct(count, total_calls)
        cat = categorize(method)
        if cat == CAT_AUTH:
            continue
        if share >= t["method_share_pct"]:
            findings.append(Finding(
                "WARN" if share < t["method_share_pct"] * 2 else "CRITICAL",
                "dominant_method",
                "HMS method '{0}' is {1:.1f}% of all calls ({2})".format(method, share, count),
                {"method": method, "category": cat, "calls": count, "share_pct": round(share, 1)},
                _resolution_for_method(method, cat),
            ))

    # 2) Delegation-token storm
    token_calls = sum(c for m, c in an.audit_by_method.items() if categorize(m) == CAT_AUTH)
    token_share = _pct(token_calls, total_calls)
    if token_calls and token_share >= t["token_share_pct"]:
        findings.append(Finding(
            "CRITICAL" if token_share >= t["token_share_pct"] * 1.5 else "WARN",
            "delegation_token_storm",
            "Delegation-token traffic is {0:.1f}% of HMS calls ({1})".format(token_share, token_calls),
            {"token_calls": token_calls, "share_pct": round(token_share, 1)},
            "Delegation tokens are hitting the metastore. Move the token store off "
            "the DB by setting hive.cluster.delegation.token.store.class to "
            "org.apache.hadoop.hive.thrift.ZooKeeperTokenStore (or MemoryTokenStore "
            "for a single HMS). Ensure clients reuse HMS connections/tokens instead "
            "of re-authenticating per call (connection pooling), and raise token "
            "renew/lifetime settings. A get_token storm usually means many short "
            "sessions (e.g. per-task Spark executors) each fetching a token.",
        ))

    # 3) Fetch-all-partitions pressure
    fetch_all = (an.audit_by_method.get("get_partitions", 0)
                 + an.audit_by_method.get("get_partitions_with_auth", 0)
                 + an.audit_by_method.get("get_partitions_ps_with_auth", 0))
    if fetch_all >= t["fetch_all_partitions_calls"]:
        findings.append(Finding(
            "WARN" if fetch_all < t["fetch_all_partitions_calls"] * 3 else "CRITICAL",
            "fetch_all_partitions",
            "Unfiltered partition fetches (get_partitions*) = {0}".format(fetch_all),
            {"calls": fetch_all},
            "Queries are loading ALL partitions of tables. Enable partition pruning "
            "so the metastore returns only needed partitions: set "
            "metastore.limit.partition.request / hive.metastore.limit.partition.request "
            "to a sane cap, prefer get_partitions_by_expr/by_filter (pushdown), and in "
            "Spark keep spark.sql.hive.metastorePartitionPruning=true. Investigate the "
            "top tables/IDs below and add partition predicates to the offending queries.",
        ))

    # 4) Partition write bursts (add/drop/alter)
    pw_calls = sum(c for m, c in an.audit_by_method.items()
                   if categorize(m) == CAT_PARTITION_WRITE)
    if pw_calls >= t["partition_write_calls"]:
        findings.append(Finding(
            "WARN" if pw_calls < t["partition_write_calls"] * 3 else "CRITICAL",
            "partition_write_burst",
            "Partition write operations (add/drop/alter_partition*) = {0}".format(pw_calls),
            {"calls": pw_calls,
             "by_method": {m: c for m, c in an.audit_by_method.items()
                           if categorize(m) == CAT_PARTITION_WRITE}},
            "High partition-write volume. Batch changes (multi-partition ALTER TABLE "
            "ADD PARTITION, dynamic-partition INSERT instead of per-partition calls), "
            "avoid repeated MSCK REPAIR loops (deploy the msck-block hook), and use the "
            "orphan-cleanup tool to remove dangling partitions in controlled batches "
            "rather than ad-hoc drops. alter_partitions storms are often stats "
            "auto-gather writing back per partition.",
        ))

    # 5) Stats storm
    stats_calls = sum(c for m, c in an.audit_by_method.items()
                      if categorize(m) == CAT_STATS)
    if stats_calls >= t["stats_calls"]:
        findings.append(Finding(
            "WARN" if stats_calls < t["stats_calls"] * 3 else "CRITICAL",
            "stats_storm",
            "Statistics calls (get_aggr_stats_for/*_statistics/*_column_statistics) = {0}".format(stats_calls),
            {"calls": stats_calls,
             "by_method": {m: c for m, c in an.audit_by_method.items()
                           if categorize(m) == CAT_STATS}},
            "Column/partition statistics traffic is heavy - this is the same load "
            "behind get_partitions_statistics/get_aggr_stats_for pressure. Review "
            "hive.stats.fetch.partition.stats (disable per-query partition-stats fetch "
            "where safe), enable the aggregate stats cache "
            "(metastore.aggregate.stats.cache.enabled), schedule ANALYZE off-peak "
            "instead of relying on autogather, and consider the hive-stats-analyzer to "
            "prune/curate stale stats.",
        ))

    # 6) Constraint-fetch amplification
    constr_calls = sum(c for m, c in an.audit_by_method.items()
                       if categorize(m) == CAT_CONSTRAINTS)
    constr_share = _pct(constr_calls, total_calls)
    get_table_calls = (an.audit_by_method.get("get_table", 0)
                       + an.audit_by_method.get("get_table_objects_by_name", 0))
    if constr_calls and constr_share >= t["constraints_share_pct"]:
        findings.append(Finding(
            "INFO" if constr_share < t["constraints_share_pct"] * 2 else "WARN",
            "constraint_amplification",
            "Constraint lookups are {0:.1f}% of calls ({1}); ~{2} get_table calls".format(
                constr_share, constr_calls, get_table_calls),
            {"constraint_calls": constr_calls, "share_pct": round(constr_share, 1),
             "get_table_calls": get_table_calls},
            "Hive 3 fetches primary/foreign/unique/not-null/check/default constraints "
            "on every table open, multiplying metastore reads. Reduce table-open churn: "
            "enable/expand HS2 metastore caching, reuse sessions, and avoid workloads "
            "that repeatedly re-open the same tables. This is inherent to get_table in "
            "Hive 3, so the lever is call frequency, not the per-call cost.",
        ))

    # 7) Loop / repetitive-call offenders
    loop_findings = []
    for peak in an.loop_peaks():
        if peak["peak_in_window"] >= t["loop_min_peak"] or peak["rate_per_sec"] >= t["loop_min_rate_per_sec"]:
            who = peak["ugi"] or "?"
            tbl = peak["table"] or "-"
            loop_findings.append(Finding(
                "CRITICAL" if peak["peak_in_window"] >= t["loop_min_peak"] * 3 else "WARN",
                "repetitive_loop",
                "Repetitive '{0}' by {1} on {2}: {3} calls in {4}s ({5}/s)".format(
                    peak["method"], who, tbl, peak["peak_in_window"],
                    peak["window_seconds"], peak["rate_per_sec"]),
                peak,
                "This looks like an application loop or missing client-side cache: the "
                "same call repeats rapidly. Fix the client to batch or cache results "
                "(broadcast metadata once per job rather than per task/row), add "
                "partition filters if it is a partition read, and check for retry loops. "
                "Correlate the ugi/ip/time to the offending Spark app / HS2 queryId.",
            ))
    loop_findings.sort(key=lambda f: f.evidence.get("peak_in_window", 0), reverse=True)
    findings.extend(loop_findings[: t["top_findings_per_type"]])

    # 8) Heavy Kerberos IDs
    for short, count in an.by_ugi.most_common(top_n):
        share = _pct(count, total_calls)
        if share >= t["ugi_share_pct"]:
            top_methods = dict(an.ugi_methods[short].most_common(5))
            principals = dict(an.ugi_principal[short].most_common(3))
            findings.append(Finding(
                "WARN" if share < t["ugi_share_pct"] * 2 else "CRITICAL",
                "heavy_kerberos_id",
                "Kerberos ID '{0}' generated {1:.1f}% of HMS calls ({2})".format(short, share, count),
                {"ugi": short, "calls": count, "share_pct": round(share, 1),
                 "top_methods": top_methods, "principals": principals},
                "A single identity dominates HMS traffic. Work with the owning team to "
                "batch/cache their metadata access, add partition pruning, and review "
                "job scheduling. If it is a service account fanning out across many "
                "executors, enable connection pooling and token reuse.",
            ))

    # 9) Heavy client IPs
    for ip, count in an.by_ip.most_common(top_n):
        share = _pct(count, total_calls)
        if share >= t["ip_share_pct"]:
            findings.append(Finding(
                "WARN",
                "heavy_client_ip",
                "Client IP {0} generated {1:.1f}% of HMS calls ({2})".format(ip, share, count),
                {"ip": ip, "calls": count, "share_pct": round(share, 1)},
                "Trace this host to the responsible service (HS2, Spark driver, gateway) "
                "and apply batching/caching. Concentrated volume from one IP often means "
                "a single misbehaving client or gateway funneling many jobs.",
            ))

    # 10) HMS host skew
    if len(an.by_host) >= 2:
        host_total = sum(an.by_host.values())
        top_host, top_count = an.by_host.most_common(1)[0]
        skew = _pct(top_count, host_total)
        if skew >= t["host_skew_pct"]:
            findings.append(Finding(
                "INFO",
                "hms_host_skew",
                "HMS host '{0}' handled {1:.1f}% of audited calls".format(top_host, skew),
                {"host": top_host, "share_pct": round(skew, 1),
                 "by_host": dict(an.by_host.most_common(10))},
                "Load is skewed across HMS instances. Check the metastore client load "
                "balancer / DNS, hive.metastore.uris ordering, and whether some clients "
                "pin a single HMS. Even distribution reduces per-instance MySQL pressure.",
            ))

    # 11) Time-bucket spikes
    if an.by_bucket:
        counts = list(an.by_bucket.values())
        avg = sum(counts) / float(len(counts))
        for b, c in sorted(an.by_bucket.items()):
            if avg > 0 and c >= avg * t["bucket_spike_factor"] and c >= t["min_total_calls_for_findings"]:
                top_m = dict(an.bucket_methods[b].most_common(5))
                findings.append(Finding(
                    "WARN",
                    "load_spike",
                    "Load spike at {0}: {1} calls ({2:.1f}x window average)".format(
                        human_ts(b), c, c / avg if avg else 0),
                    {"bucket_start": human_ts(b), "calls": c,
                     "avg_calls": int(avg), "top_methods": top_m},
                    "A burst of metadata calls in this window. Inspect the top methods "
                    "and IDs during this interval to find the driving job; consider "
                    "rescheduling it or batching its metadata access.",
                ))

    # 12) Slow methods (avg duration from PERFLOG)
    for method, timer in an.perf_timer.items():
        if timer.count >= t["slow_method_min_calls"] and timer.avg >= t["slow_method_avg_ms"]:
            findings.append(Finding(
                "WARN" if timer.avg < t["slow_method_avg_ms"] * 3 else "CRITICAL",
                "slow_method",
                "Method '{0}' is slow: avg {1} ms over {2} calls (p95 {3} ms, max {4} ms)".format(
                    method, int(timer.avg), timer.count, timer.percentile(95), timer.max),
                {"method": method, "avg_ms": int(timer.avg), "calls": timer.count,
                 "p95_ms": timer.percentile(95), "max_ms": timer.max},
                _resolution_for_method(method, categorize(method))
                + " High per-call latency also points at MySQL contention/locking or "
                "missing indexes on the backing tables (PARTITIONS/PART_COL_STATS); "
                "check the DB during these windows.",
            ))

    findings.sort(key=lambda f: (SEV_ORDER.get(f.severity, 0),
                                 f.evidence.get("calls", 0)), reverse=True)
    return findings


# ----------------------------------------------------------------------------
# Reporting
# ----------------------------------------------------------------------------
def _wrap(text, width, indent):
    words = text.split()
    lines = []
    cur = ""
    for w in words:
        if len(cur) + len(w) + 1 > width:
            lines.append(cur)
            cur = w
        else:
            cur = (cur + " " + w).strip()
    if cur:
        lines.append(cur)
    return ("\n" + indent).join(lines)


class Reporter(object):
    def __init__(self, output_dir, top_n=20):
        self.output_dir = output_dir or "."
        if not os.path.isdir(self.output_dir):
            os.makedirs(self.output_dir)
        self.top_n = top_n
        self.ts = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")

    def _p(self, name):
        return os.path.join(self.output_dir, name)

    def write_all(self, an, findings, meta):
        paths = {}
        paths["txt"] = self._p("pressure_report_%s.txt" % self.ts)
        paths["json"] = self._p("pressure_report_%s.json" % self.ts)
        self._write_csvs(an, findings, paths)
        self._write_json(an, findings, meta, paths["json"])
        summary = self._write_txt(an, findings, meta, paths["txt"])
        return paths, summary

    def _write_csvs(self, an, findings, paths):
        total_calls = an.total_audit or 1
        total_time = sum(tm.total for tm in an.perf_timer.values()) or 1

        p = self._p("by_method_%s.csv" % self.ts)
        paths["by_method"] = p
        methods = set(an.audit_by_method) | set(an.perf_timer)
        with open(p, "w", newline="") as fh:
            w = csv.writer(fh)
            w.writerow(["method", "category", "audit_calls", "perf_calls",
                        "total_ms", "avg_ms", "p95_ms", "max_ms",
                        "pct_calls", "pct_time"])
            rows = []
            for m in methods:
                ac = an.audit_by_method.get(m, 0)
                tm = an.perf_timer.get(m)
                pc = tm.count if tm else 0
                tot = tm.total if tm else 0
                rows.append((m, categorize(m), ac, pc, tot,
                             int(tm.avg) if tm else 0,
                             tm.percentile(95) if tm else 0,
                             tm.max if tm else 0,
                             round(_pct(ac, total_calls), 2),
                             round(_pct(tot, total_time), 2)))
            rows.sort(key=lambda r: r[2], reverse=True)
            for r in rows:
                w.writerow(r)

        p = self._p("by_ugi_%s.csv" % self.ts)
        paths["by_ugi"] = p
        with open(p, "w", newline="") as fh:
            w = csv.writer(fh)
            w.writerow(["kerberos_id", "calls", "pct_calls", "top_methods", "principals"])
            for short, c in an.by_ugi.most_common():
                w.writerow([short, c, round(_pct(c, total_calls), 2),
                            ";".join("%s=%d" % (m, n) for m, n in an.ugi_methods[short].most_common(5)),
                            ";".join("%s=%d" % (pr, n) for pr, n in an.ugi_principal[short].most_common(3))])

        p = self._p("by_host_%s.csv" % self.ts)
        paths["by_host"] = p
        host_total = sum(an.by_host.values()) or 1
        with open(p, "w", newline="") as fh:
            w = csv.writer(fh)
            w.writerow(["hms_host", "calls", "pct_calls", "top_methods"])
            for host, c in an.by_host.most_common():
                w.writerow([host, c, round(_pct(c, host_total), 2),
                            ";".join("%s=%d" % (m, n) for m, n in an.host_methods[host].most_common(5))])

        p = self._p("by_bucket_%s.csv" % self.ts)
        paths["by_bucket"] = p
        with open(p, "w", newline="") as fh:
            w = csv.writer(fh)
            w.writerow(["bucket_start", "bucket_seconds", "calls", "top_methods"])
            for b, c in sorted(an.by_bucket.items()):
                w.writerow([human_ts(b), an.bucket_seconds, c,
                            ";".join("%s=%d" % (m, n) for m, n in an.bucket_methods[b].most_common(5))])

        p = self._p("hot_tables_%s.csv" % self.ts)
        paths["hot_tables"] = p
        with open(p, "w", newline="") as fh:
            w = csv.writer(fh)
            w.writerow(["table", "calls"])
            for tbl, c in an.by_table.most_common(200):
                w.writerow([tbl, c])

        p = self._p("by_ip_%s.csv" % self.ts)
        paths["by_ip"] = p
        with open(p, "w", newline="") as fh:
            w = csv.writer(fh)
            w.writerow(["client_ip", "calls", "pct_calls"])
            for ip, c in an.by_ip.most_common(200):
                w.writerow([ip, c, round(_pct(c, total_calls), 2)])

        p = self._p("findings_%s.csv" % self.ts)
        paths["findings"] = p
        with open(p, "w", newline="") as fh:
            w = csv.writer(fh)
            w.writerow(["severity", "type", "message", "resolution"])
            for f in findings:
                w.writerow([f.severity, f.ftype, f.message, f.resolution])

    def _write_json(self, an, findings, meta, path):
        total_calls = an.total_audit or 1
        total_time = sum(tm.total for tm in an.perf_timer.values())
        doc = {
            "generated_at": datetime.datetime.now().isoformat(),
            "meta": meta,
            "totals": {
                "audit_calls": an.total_audit,
                "perf_calls": an.total_perf,
                "total_perf_ms": total_time,
                "window_start": human_ts(an.min_ts),
                "window_end": human_ts(an.max_ts),
                "duration_seconds": int(an.duration_seconds()),
            },
            "by_category_calls": dict(an.by_category_calls),
            "by_category_time_ms": dict(an.by_category_time),
            "top_methods": [
                {"method": m, "calls": c, "pct_calls": round(_pct(c, total_calls), 2),
                 "category": categorize(m)}
                for m, c in an.audit_by_method.most_common(self.top_n)
            ],
            "top_methods_by_time": [
                {"method": m, "total_ms": tm.total, "avg_ms": int(tm.avg),
                 "p95_ms": tm.percentile(95), "calls": tm.count}
                for m, tm in sorted(an.perf_timer.items(),
                                    key=lambda kv: kv[1].total, reverse=True)[: self.top_n]
            ],
            "top_kerberos_ids": [
                {"ugi": s, "calls": c, "pct_calls": round(_pct(c, total_calls), 2)}
                for s, c in an.by_ugi.most_common(self.top_n)
            ],
            "top_ips": [{"ip": ip, "calls": c} for ip, c in an.by_ip.most_common(self.top_n)],
            "by_host": dict(an.by_host.most_common()),
            "hot_tables": [{"table": t, "calls": c} for t, c in an.by_table.most_common(self.top_n)],
            "findings": [f.to_dict() for f in findings],
        }
        with open(path, "w") as fh:
            json.dump(doc, fh, indent=2)
        LOG.info("wrote JSON report: %s", path)

    def _write_txt(self, an, findings, meta, path):
        total_calls = an.total_audit or 1
        total_time = sum(tm.total for tm in an.perf_timer.values())
        L = []
        L.append("=" * 78)
        L.append("HIVE METASTORE METADATA PRESSURE REPORT")
        L.append("Generated: %s" % datetime.datetime.now().isoformat())
        L.append("=" * 78)
        L.append("")
        L.append("SCAN")
        L.append("  Files scanned      : %s" % meta.get("files_scanned", "?"))
        L.append("  Lines parsed       : %s" % meta.get("lines_parsed", "?"))
        L.append("  Window (data)      : %s  ->  %s" % (human_ts(an.min_ts), human_ts(an.max_ts)))
        L.append("  Audit calls        : %d" % an.total_audit)
        L.append("  PERFLOG calls      : %d  (total %d ms)" % (an.total_perf, total_time))
        L.append("  HMS hosts          : %d" % len(an.by_host))
        L.append("  Distinct Kerb IDs  : %d" % len(an.by_ugi))
        L.append("")

        crit = sum(1 for f in findings if f.severity == "CRITICAL")
        warn = sum(1 for f in findings if f.severity == "WARN")
        info = sum(1 for f in findings if f.severity == "INFO")
        L.append("FINDINGS: %d CRITICAL, %d WARN, %d INFO" % (crit, warn, info))
        L.append("")

        L.append("-" * 78)
        L.append("TOP HMS METHODS BY CALL VOLUME (audit)")
        L.append("  %-34s %-14s %10s %7s" % ("method", "category", "calls", "%"))
        for m, c in an.audit_by_method.most_common(self.top_n):
            L.append("  %-34s %-14s %10d %6.1f%%" % (m[:34], categorize(m), c, _pct(c, total_calls)))
        L.append("")

        if an.perf_timer:
            L.append("-" * 78)
            L.append("TOP HMS METHODS BY TOTAL TIME (PERFLOG)")
            L.append("  %-30s %10s %9s %8s %8s" % ("method", "calls", "total_ms", "avg_ms", "p95_ms"))
            for m, tm in sorted(an.perf_timer.items(), key=lambda kv: kv[1].total, reverse=True)[: self.top_n]:
                L.append("  %-30s %10d %9d %8d %8d" % (m[:30], tm.count, tm.total, int(tm.avg), tm.percentile(95)))
            L.append("")

        L.append("-" * 78)
        L.append("PRESSURE BY CATEGORY (calls / time)")
        cats = set(an.by_category_calls) | set(an.by_category_time)
        L.append("  %-16s %12s %8s %14s" % ("category", "calls", "%calls", "time_ms"))
        for cat in sorted(cats, key=lambda c: an.by_category_calls.get(c, 0), reverse=True):
            cc = an.by_category_calls.get(cat, 0)
            ct = an.by_category_time.get(cat, 0)
            L.append("  %-16s %12d %7.1f%% %14d" % (cat, cc, _pct(cc, total_calls), ct))
        L.append("")

        L.append("-" * 78)
        L.append("TOP KERBEROS IDs (who is generating pressure)")
        L.append("  %-30s %12s %7s   %s" % ("kerberos_id", "calls", "%", "top methods"))
        for s, c in an.by_ugi.most_common(self.top_n):
            tm = ", ".join("%s=%d" % (m, n) for m, n in an.ugi_methods[s].most_common(3))
            L.append("  %-30s %12d %6.1f%%   %s" % (s[:30], c, _pct(c, total_calls), tm))
        L.append("")

        if an.by_host:
            L.append("-" * 78)
            L.append("HMS HOST DISTRIBUTION")
            host_total = sum(an.by_host.values()) or 1
            for host, c in an.by_host.most_common():
                L.append("  %-45s %12d %6.1f%%" % (host[:45], c, _pct(c, host_total)))
            L.append("")

        if an.by_table:
            L.append("-" * 78)
            L.append("HOT TABLES")
            for tbl, c in an.by_table.most_common(self.top_n):
                L.append("  %-55s %10d" % (tbl[:55], c))
            L.append("")

        if len(an.by_bucket) > 1:
            L.append("-" * 78)
            L.append("LOAD TREND (per %ds bucket)" % an.bucket_seconds)
            peak = max(an.by_bucket.values()) or 1
            for b, c in sorted(an.by_bucket.items()):
                bar = "#" * int(40 * c / peak)
                L.append("  %s %10d %s" % (human_ts(b), c, bar))
            L.append("")

        L.append("=" * 78)
        L.append("FINDINGS & RECOMMENDED RESOLUTIONS")
        L.append("=" * 78)
        if not findings:
            L.append("  No threshold breaches detected in this window.")
        for i, f in enumerate(findings, 1):
            L.append("")
            L.append("[%d] %-8s %s" % (i, f.severity, f.message))
            L.append("     type: %s" % f.ftype)
            if f.evidence:
                ev = ", ".join("%s=%s" % (k, v) for k, v in f.evidence.items()
                               if not isinstance(v, (dict, list)))
                if ev:
                    L.append("     evidence: %s" % ev)
            L.append("     resolution: %s" % _wrap(f.resolution, 70, "                 "))
        L.append("")
        L.append("=" * 78)
        text = "\n".join(L) + "\n"
        with open(path, "w") as fh:
            fh.write(text)
        LOG.info("wrote text report: %s", path)
        return text


# ----------------------------------------------------------------------------
# Scanning drivers
# ----------------------------------------------------------------------------
def _line_ts(raw):
    """Timestamp of a raw line, tolerating a 'host | ' prefix."""
    m = HOST_PREFIX_RE.match(raw)
    return parse_ts(m.group("line") if m else raw)


def _peek_max_ts(files, sample_tail_bytes=2000000):
    """Cheaply find the newest timestamp across files (tail of each)."""
    max_ts = None
    for path in files:
        try:
            if path.endswith(".gz"):
                # gz: must stream; keep the last valid timestamp seen
                last = None
                for _ln, raw in iter_lines(path):
                    ts = _line_ts(raw)
                    if ts:
                        last = ts
                if last and (max_ts is None or last > max_ts):
                    max_ts = last
                continue
            size = os.path.getsize(path)
            with open(path, "r", encoding="utf-8", errors="replace") as fh:
                if size > sample_tail_bytes:
                    fh.seek(size - sample_tail_bytes)
                    fh.readline()
                for line in fh:
                    ts = _line_ts(line)
                    if ts and (max_ts is None or ts > max_ts):
                        max_ts = ts
        except (OSError, IOError):
            continue
    return max_ts


def scan_files_into_analyzer(files, an, since_ts=None, until_ts=None, exclude_methods=None):
    exclude = set(exclude_methods or [])
    lines_parsed = 0
    for path in files:
        host = host_from_filename(path)
        for _line_no, raw in iter_lines(path):
            perf, audit = parse_line(raw)
            if perf:
                ts = perf["ts"]
                if (since_ts and ts < since_ts) or (until_ts and ts > until_ts):
                    pass
                elif perf["method"] not in exclude:
                    an.ingest_perf(perf, host=host)
                    lines_parsed += 1
            if audit:
                ts = audit["ts"]
                if (since_ts and ts < since_ts) or (until_ts and ts > until_ts):
                    continue
                if audit["method"] in exclude:
                    continue
                an.ingest_audit(audit, host=host)
                lines_parsed += 1
    return lines_parsed


# ----------------------------------------------------------------------------
# Monitor mode (scheduled)
# ----------------------------------------------------------------------------
def load_state(path):
    if not path or not os.path.exists(path):
        return {"offsets": {}, "alerted": []}
    try:
        with open(path, "r", encoding="utf-8", errors="replace") as fh:
            return json.load(fh)
    except (ValueError, OSError):
        return {"offsets": {}, "alerted": []}


def save_state(path, state):
    d = os.path.dirname(path)
    if d and not os.path.exists(d):
        os.makedirs(d)
    with open(path, "w", encoding="utf-8") as fh:
        json.dump(state, fh, indent=2, sort_keys=True)


class EventStore(object):
    """Compact JSONL rolling store of audit call records for monitor mode."""

    def __init__(self, path, retention_seconds, enabled=True):
        self.path = path
        self.retention_seconds = int(retention_seconds)
        self.enabled = enabled and bool(path)

    def append_many(self, records):
        if not self.enabled or not records:
            return
        d = os.path.dirname(self.path)
        if d and not os.path.exists(d):
            os.makedirs(d)
        with open(self.path, "a", encoding="utf-8") as fh:
            for r in records:
                fh.write(json.dumps(r, sort_keys=True) + "\n")

    def load_since(self, since_ts):
        if not self.enabled or not os.path.exists(self.path):
            return []
        out = []
        with open(self.path, "r", encoding="utf-8", errors="replace") as fh:
            for line in fh:
                line = line.strip()
                if not line:
                    continue
                try:
                    r = json.loads(line)
                except ValueError:
                    continue
                if r.get("ts", 0) >= since_ts:
                    out.append(r)
        return out

    def compact(self):
        if not self.enabled or not os.path.exists(self.path):
            return 0
        cutoff = time.time() - self.retention_seconds
        kept = self.load_since(cutoff)
        tmp = self.path + ".tmp"
        with open(tmp, "w", encoding="utf-8") as fh:
            for r in kept:
                fh.write(json.dumps(r, sort_keys=True) + "\n")
        os.replace(tmp, self.path)
        return len(kept)


def send_email(email_cfg, findings, dry_run=False):
    if not findings or not email_cfg.get("enabled"):
        return
    if dry_run:
        LOG.info("[dry-run] would email %d finding(s)", len(findings))
        return
    lines = []
    for f in findings:
        lines.append("[%s] %s: %s" % (f.severity, f.ftype, f.message))
        lines.append("    resolution: %s" % f.resolution)
        lines.append("")
    msg = MIMEMultipart()
    msg["From"] = email_cfg["from_addr"]
    msg["To"] = ", ".join(email_cfg["to_addrs"])
    msg["Subject"] = "%s %d HMS pressure finding(s)" % (
        email_cfg.get("subject_prefix", "[HMS Pressure Monitor]"), len(findings))
    msg.attach(MIMEText("\n".join(lines), "plain"))
    smtp = smtplib.SMTP(email_cfg["smtp_host"], email_cfg["smtp_port"], timeout=30)
    try:
        if email_cfg.get("use_tls", True):
            smtp.starttls()
        if email_cfg.get("username") and email_cfg.get("password"):
            smtp.login(email_cfg["username"], email_cfg["password"])
        smtp.sendmail(email_cfg["from_addr"], email_cfg["to_addrs"], msg.as_string())
    finally:
        smtp.quit()


def run_monitor_once(config, dry_run=False):
    log_paths = config.get("log_paths", [])
    if isinstance(log_paths, str):
        log_paths = [log_paths]
    state_file = config.get("state_file", "/var/lib/metadata_pressure_monitor/state.json")
    state = load_state(state_file)
    offsets = state.setdefault("offsets", {})
    alerted = set(state.get("alerted", []))

    store_cfg = config.get("event_store", {})
    store = EventStore(
        store_cfg.get("path", "/var/lib/metadata_pressure_monitor/events.jsonl"),
        store_cfg.get("retention_seconds", 86400),
        enabled=store_cfg.get("enabled", True),
    )
    exclude = set(config.get("exclude_methods", []))
    bucket_seconds = parse_window(config.get("window", {}).get("bucket", "1h")) or 3600
    loop_window = int(config.get("thresholds", {}).get("loop_window_seconds", 60))

    files = discover_files(log_paths)
    LOG.info("monitor: scanning %d file(s)", len(files))
    pending = []

    def handle(host, line_no, raw):
        _perf, audit = parse_line(raw)
        if audit and audit["method"] not in exclude:
            pending.append({
                "ts": audit["ts"], "method": audit["method"],
                "ugi": normalize_ugi(audit.get("ugi", ""))[0],
                "ip": audit.get("ip", ""), "host": host,
                "db": audit.get("db", ""), "table": audit.get("table", ""),
            })

    for path in files:
        if path.endswith(".gz"):
            continue  # monitor tails live (uncompressed) logs only
        host = host_from_filename(path)
        offsets[path] = scan_incremental(
            path, int(offsets.get(path, 0)),
            lambda ln, raw, h=host: handle(h, ln, raw))

    if not dry_run:
        store.append_many(pending)

    max_window = int(store_cfg.get("max_aggregation_window_seconds", 3600))
    since_ts = time.time() - max_window
    records = store.load_since(since_ts) if not dry_run else list(pending)

    an = Analyzer(bucket_seconds=bucket_seconds, loop_window_seconds=loop_window)
    for r in records:
        an.ingest_audit(r, host=r.get("host", ""))

    findings = run_detectors(an, config.get("thresholds", {}),
                             top_n=config.get("top_n", 20))
    alert_sev = config.get("alert_severity", "WARN")
    new_findings = [f for f in findings
                    if SEV_ORDER.get(f.severity, 0) >= SEV_ORDER.get(alert_sev, 1)
                    and f.fingerprint not in alerted]

    for f in new_findings:
        LOG.info("[%s] %s: %s", f.severity, f.ftype, f.message)
        alerted.add(f.fingerprint)
    if not new_findings:
        LOG.info("monitor: no new findings at/above %s", alert_sev)

    send_email(config.get("email", {}), new_findings, dry_run=dry_run)

    if not dry_run:
        store.compact()
        state["alerted"] = sorted(alerted)[-10000:]
        save_state(state_file, state)
    return EXIT_FINDINGS if new_findings else EXIT_OK


# ----------------------------------------------------------------------------
# CLI
# ----------------------------------------------------------------------------
def load_config(path):
    if not path:
        return {}
    with open(path, "r", encoding="utf-8", errors="replace") as fh:
        return json.load(fh)


def build_parser():
    p = argparse.ArgumentParser(
        prog="metadata_pressure_monitor.py",
        description="Detect sources of excessive Hive Metastore metadata pressure "
                    "on the MySQL backend (CDP 7.1.9).",
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    p.add_argument("-v", "--verbose", action="store_true", help="Debug logging")
    sub = p.add_subparsers(dest="mode", metavar="{report,monitor}")
    sub.required = True

    r = sub.add_parser("report", help="Ad-hoc: analyze historical logs and write a report")
    r.add_argument("--paths", nargs="+", help="Files, globs, or directories of HMS logs (gz ok)")
    r.add_argument("--config", help="Optional JSON config (log_paths, thresholds, etc.)")
    r.add_argument("--last", help="Analyze only the last window, e.g. 6h, 2d, 90m")
    r.add_argument("--since", help="Start time 'YYYY-MM-DD HH:MM:SS'")
    r.add_argument("--until", help="End time 'YYYY-MM-DD HH:MM:SS'")
    r.add_argument("--bucket", default="1h", help="Trend bucket size (e.g. 15m, 1h). Default 1h")
    r.add_argument("--top", type=int, default=20, help="Top-N rows in report (default 20)")
    r.add_argument("--exclude-methods", help="Comma-separated methods to ignore (e.g. get_token)")
    r.add_argument("--loop-window", type=int, default=60,
                   help="Seconds window for loop/repetition detection (default 60)")
    r.add_argument("--output-dir", default=".", help="Directory for report outputs")

    m = sub.add_parser("monitor", help="Scheduled: incremental tail with alerts")
    m.add_argument("--config", required=True, help="Path to JSON config file")
    m.add_argument("--once", action="store_true", help="Run one pass and exit")
    m.add_argument("--dry-run", action="store_true", help="Do not persist state or email")
    m.add_argument("--reset-state", action="store_true", help="Delete state + event store first")
    return p


def setup_logging(verbose):
    logging.basicConfig(
        level=logging.DEBUG if verbose else logging.INFO,
        format="%(asctime)s %(levelname)-7s %(message)s",
        handlers=[logging.StreamHandler(sys.stdout)],
    )


def run_report(args):
    config = load_config(args.config) if args.config else {}
    paths = args.paths or config.get("log_paths")
    if isinstance(paths, str):
        paths = [paths]
    if not paths:
        LOG.error("no --paths (or config log_paths) provided")
        return EXIT_ERROR

    files = discover_files(paths)
    if not files:
        LOG.error("no files matched: %s", paths)
        return EXIT_WARN
    LOG.info("scanning %d file(s)", len(files))

    since_ts = parse_iso(args.since)
    until_ts = parse_iso(args.until)
    bucket_seconds = parse_window(args.bucket) or 3600
    exclude = [m.strip() for m in (args.exclude_methods or "").split(",") if m.strip()]

    if args.last:
        w = parse_window(args.last)
        if not w:
            LOG.error("bad --last value: %s", args.last)
            return EXIT_ERROR
        max_ts = _peek_max_ts(files)
        if max_ts:
            since_ts = max_ts - w
            LOG.info("--last %s -> window from %s", args.last, human_ts(since_ts))

    an = Analyzer(bucket_seconds=bucket_seconds, loop_window_seconds=args.loop_window)
    lines = scan_files_into_analyzer(files, an, since_ts=since_ts,
                                     until_ts=until_ts, exclude_methods=exclude)
    if lines == 0:
        LOG.warning("no HMS audit/PERFLOG lines matched in the given window")
        return EXIT_WARN

    thresholds = config.get("thresholds", {})
    findings = run_detectors(an, thresholds, top_n=args.top)

    reporter = Reporter(args.output_dir, top_n=args.top)
    meta = {"files_scanned": len(files), "lines_parsed": lines,
            "since": human_ts(since_ts) if since_ts else None,
            "until": human_ts(until_ts) if until_ts else None,
            "excluded_methods": exclude}
    paths_out, summary = reporter.write_all(an, findings, meta)

    print(summary)
    LOG.info("reports: %s", ", ".join(sorted(set(paths_out.values()))))
    return EXIT_FINDINGS if any(f.severity in ("WARN", "CRITICAL") for f in findings) else EXIT_OK


def run_monitor(args):
    config = load_config(args.config)
    if args.reset_state:
        for pth in (config.get("state_file"),
                    config.get("event_store", {}).get("path")):
            if pth and os.path.exists(pth):
                os.remove(pth)
                LOG.info("removed %s", pth)

    if args.once:
        return run_monitor_once(config, dry_run=args.dry_run)

    interval = int(config.get("interval_seconds", 300))
    LOG.info("starting monitor loop every %ds", interval)
    rc = EXIT_OK
    while True:
        try:
            rc = run_monitor_once(config, dry_run=args.dry_run)
        except Exception as exc:  # keep the daemon alive
            LOG.error("monitor cycle error: %s", exc)
        time.sleep(interval)
    return rc


def main(argv=None):
    parser = build_parser()
    args = parser.parse_args(argv)
    setup_logging(args.verbose)
    try:
        if args.mode == "report":
            return run_report(args)
        if args.mode == "monitor":
            return run_monitor(args)
        parser.error("unknown mode: %s" % args.mode)
    except KeyboardInterrupt:
        LOG.error("interrupted")
        return EXIT_ERROR
    except (OSError, IOError, ValueError) as exc:
        LOG.error("fatal: %s", exc)
        return EXIT_ERROR


if __name__ == "__main__":
    sys.exit(main())
