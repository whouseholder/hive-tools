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

To actually clean up later, review the report, then run `clean --execute` or
`apply --report <file> --execute`. See the tool README for details.

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
- **Orphan Cleanup** defaults to **dry-run**. It only issues DML when you pass
  `--execute`, re-verifies each object against storage immediately before a
  DROP, and writes an audit log of every statement. It uses **standard Hive
  DML** only - never raw `DELETE` against the metastore database.

---

## Pushing to GitHub

This directory is a ready-to-push git repository. After creating an empty repo
on your Git host:

```bash
cd /Users/whouseholder/Documents/GitHub/hive-tools
git remote add origin <your-repo-url>
git push -u origin main
```
