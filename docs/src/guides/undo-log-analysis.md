# Undo Log Analysis

This guide explains how to use `inno undo` to inspect undo tablespace internals, diagnose long-running transactions, and understand rollback segment utilization.

## When to Use

- **Long-running transaction investigation** — identify active transactions holding undo history
- **Undo tablespace bloat** — check segment states and purge backlog
- **XA transaction recovery** — find PREPARED transactions that may need manual resolution
- **Forensic analysis** — review transaction IDs and types from undo log headers

## Undo Tablespace Structure

MySQL 8.0+ dedicated undo tablespaces (`.ibu` files) have this internal layout:

```text
Page 0: FSP_HDR (file space header)
Page 1: IBUF_BITMAP (unused in undo tablespaces)
Page 2: INODE
Page 3: RSEG_ARRAY — rollback segment slot directory
Page 4+: RSEG header pages and undo segment pages
```

The RSEG array page contains up to 128 slots, each pointing to a rollback segment header page. Each rollback segment header manages up to 1024 undo log slots.

## Basic Analysis

```bash
inno undo -f /var/lib/mysql/undo_001.ibu
```

This shows the RSEG slot layout, per-segment states, and transaction summary.

## Identifying Active Transactions

```bash
inno undo -f undo_001.ibu --json | jq '.segments[] | select(.segment_header.state == "Active")'
```

Active segments are currently in use by running transactions. If you see many active segments while no queries are running, you may have abandoned XA transactions or a hung purge thread.

## Checking Purge Backlog

```bash
inno undo -f undo_001.ibu --json | jq '{
  total: .total_transactions,
  active: .active_transactions,
  to_purge: [.segments[] | select(.segment_header.state == "ToPurge")] | length
}'
```

A large number of `ToPurge` segments indicates the purge thread is falling behind, which causes undo tablespace growth.

## Verbose Record Inspection

```bash
inno undo -f undo_001.ibu -v
```

Verbose mode walks individual undo records within each segment, reporting record types (INSERT, UPDATE_EXIST, UPDATE_DEL, DEL_MARK) and table IDs. This is useful for understanding which tables are generating the most undo history.

## Single Page Inspection

```bash
inno undo -f undo_001.ibu -p 5
```

Inspect a specific undo page to see its page header, segment header, and all undo log headers on that page.

## Encrypted Undo Tablespaces

MySQL 8.0.16+ supports encryption of undo tablespaces:

```bash
inno undo -f undo_001.ibu --keyring /var/lib/mysql-keyring/keyring
```

The keyring file must contain the master encryption key matching the tablespace.

## Undo Segment Lifecycle

Understanding segment states helps diagnose undo tablespace issues:

```text
┌─────────┐    ┌─────────┐    ┌──────────┐    ┌─────────┐
│  Free   │───▶│ Active  │───▶│ ToPurge  │───▶│ Cached  │
└─────────┘    └─────────┘    └──────────┘    └─────────┘
                    │                              │
                    ▼                              │
               ┌──────────┐                        │
               │ Prepared │                        │
               └──────────┘                        │
                    │              ┌──────────┐    │
                    └─────────────▶│  ToFree  │◀───┘
                                   └──────────┘
```

- **Free** — slot is empty, available for allocation
- **Active** — transaction is actively writing undo records
- **ToPurge** — transaction committed, waiting for purge
- **Cached** — purged and available for reuse without reallocation
- **Prepared** — XA transaction in PREPARED state
- **ToFree** — segment being deallocated during truncation
