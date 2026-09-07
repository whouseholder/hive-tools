# hive-tools

Operational tooling for keeping a **Hive Metastore (HMS)** and its **MySQL
backend** healthy on **CDP 7.1.9 (Hive 3)** on-prem clusters. Two standalone,
dependency-free Python tools:

| Tool | What it does | Folder |
|------|--------------|--------|
| **Orphan Cleanup** | Finds and removes orphaned Hive tables/partitions (metadata whose storage is gone) via standard Hive DML, reducing metastore rows. | [`orphan-cleanup/`](orphan-cleanup/) |
| **Metadata Pressure Monitor** | Analyzes HMS logs to find what is overloading the metastore/MySQL (hot methods, users, tables, loops) and prescribes fixes. | [`pressure-monitor/`](pressure-monitor/) |

Both are **Python 3.6+ and use only the standard library** - no `pip install`,
no virtualenv required. Copy a script to an edge node and run it.

---

## Which tool do I want?

- "The metastore/MySQL is under heavy load and I need to know **why** and **who**."
  -> [Metadata Pressure Monitor](pressure-monitor/)
- "We have stale tables/partitions whose data is gone and I want to **clean up
  the metastore**." -> [Orphan Cleanup](orphan-cleanup/)

A common workflow: run the **pressure monitor** to understand load and find
churn, then use **orphan cleanup** to remove the dead metadata that is inflating
row counts.

---

## Quick start (60 seconds)

Both tools are safe by default (read-only / dry-run) and print what they would
do before changing anything.

### Metadata Pressure Monitor - analyze some HMS logs

```bash
cd pressure-monitor
./metadata_pressure_monitor.py report --paths /var/log/hive --last 24h --output-dir ./out
# -> writes ./out/pressure_report_<ts>.txt (+ .json and CSVs)
```

Gzipped logs, directories, and globs all work. No cluster connection needed -
it just reads logs.

### Orphan Cleanup - find orphaned objects (read-only)

Run this on a CDP edge node that has `beeline` and `hdfs` on the PATH and a
valid Kerberos ticket:

```bash
cd orphan-cleanup
./hive_orphan_cleanup.py report \
    --jdbc-url 'jdbc:hive2://hs2.example.com:10000/default;principal=hive/_HOST@REALM' \
    --output-dir ./out
# -> writes ./out/orphans_<ts>.csv (+ .json and a summary). Nothing is changed.
```

To actually clean up later, review the report, (optionally trim it,) then run
`apply --report <file> --execute`. You will be shown an itemized plan and must
type `yes`. By default **only EXTERNAL tables/partitions are touched** and
**ACID/transactional tables are never dropped**. See the tool README for details.

---

## Requirements

- **Python 3.6 or newer** (standard library only).
- **Orphan Cleanup** additionally needs, on the machine it runs on:
  - the `beeline` and `hdfs` CLIs (present on any CDP edge/gateway node), and
  - a Kerberos ticket (`kinit`), or a keytab/principal you pass to the tool.
- **Metadata Pressure Monitor** needs only read access to the HMS log files.

There is nothing to install. See [`requirements.txt`](requirements.txt).

---

## Repository layout

```
hive-tools/
├── README.md                     # this file
├── requirements.txt              # (documents: no third-party deps)
├── .gitignore
├── orphan-cleanup/
│   ├── hive_orphan_cleanup.py    # the tool
│   └── README.md                 # full guide
└── pressure-monitor/
    ├── metadata_pressure_monitor.py
    ├── metadata_pressure_monitor.config.example.json
    └── README.md                 # full guide
```

---

## Safety model

- **Pressure Monitor** is strictly **read-only**. It never touches the cluster;
  it only reads logs and writes reports/alerts.
- **Orphan Cleanup** is conservative by default and designed for production:
  - **Dry-run by default** - DML is only issued with `--execute`.
  - **External-only by default** - dropping a MANAGED table deletes data, so
    managed objects are skipped unless you pass `--allow-managed`.
  - **ACID/transactional tables are never auto-dropped** (hard rule).
  - **Positive proof of absence** - an object is dropped only when its parent
    directory lists successfully *and* the object is genuinely missing; any
    transient storage error is skipped, so an Isilon/NameNode blip can't
    manufacture orphans.
  - **Bulk-safety guard** - refuses to act on a suspiciously large set
    (`--max-drops`, `--max-orphan-pct`) unless `--allow-bulk`, since that
    usually signals a storage outage rather than real orphans.
  - **Notify + confirm** - prints an itemized plan and requires you to type
    `yes` (or pass `--yes` for automation); writes an audit log of every
    statement. Uses **standard Hive DML** only - never raw `DELETE` against the
    metastore database.

---

## Pushing to GitHub

This directory is a ready-to-push git repository. After creating an empty repo
on your Git host:

```bash
cd /Users/whouseholder/Documents/GitHub/hive-tools
git remote add origin <your-repo-url>
git push -u origin main
```
