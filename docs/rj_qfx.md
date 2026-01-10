# Raymond James QFX/OFX (Quicken Downloads)

Investor supports importing Raymond James “Quicken Downloads” files (`.qfx` / `.ofx`) as the preferred offline sync format for RJ accounts.

## Export from Raymond James
In Client Access:
- Go to the account → download/export options
- Choose **Quicken Downloads** (produces `.qfx` or `.ofx`)
- Choose a **Starting Date** (you can re-export overlapping ranges; the importer is idempotent)

## Import via Web UI
1) Go to `Sync → Connections`
2) Create or open your `Raymond James (Offline QFX/OFX)` connection
3) Upload one or more `.qfx`/`.ofx` files under **Statement files**
4) Click **Sync now**

Notes:
- Re-importing the same files is safe: transactions dedupe by FITID (or a stable hash fallback).
- Holdings snapshots are imported from QFX positions and stored as read-only snapshots.

## Import via API
`POST /api/connectors/rj/qfx-import` (multipart)

Fields:
- `connection_id` (int, required)
- `uploads` (file[], required) — `.qfx`/`.ofx`
- `since` (YYYY-MM-DD, optional) — start date override for the sync run
- `mode` (`INCREMENTAL` or `FULL`, optional; default `INCREMENTAL`)
- `dry_run` (bool, optional) — parse counts only, no DB writes

## Import via CLI
`investor sync rj --connection-id <id> --qfx <file_or_dir> [--since YYYY-MM-DD]`

Examples:
- Import a single file:
  - `investor sync rj --connection-id 6 --qfx ~/Downloads/RJ_Quicken.qfx`
- Import a directory:
  - `investor sync rj --connection-id 6 --qfx ~/Downloads/rj_qfx/ --since 2025-01-01`
- Dry run (parse counts only):
  - `investor sync rj --connection-id 6 --qfx ~/Downloads/RJ_Quicken.qfx --dry-run`

## Audit / safety
- Every ingested offline file is copied into `data/external/raw_archive/conn_<id>/` (append-only).
- The ingest record stores the file SHA256 and best-effort date hints from the QFX header (DTSTART/DTEND).

