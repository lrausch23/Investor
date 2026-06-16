from __future__ import annotations

import csv
import datetime as dt
import hashlib
import json
import os
import shutil
import sqlite3
import tempfile
import urllib.request
import zipfile
from pathlib import Path
from typing import Any, Mapping, Sequence

import pandas as pd

from .store import DEFAULT_SHARADAR_DIR, FACT_TABLES, META_TABLES

DEFAULT_TABLES = ("SF1", "SEP", "TICKERS", "ACTIONS", "DAILY", "SP500")


class SharadarIngestionError(RuntimeError):
    pass


def ingest_sharadar(
    *,
    root: str | Path = DEFAULT_SHARADAR_DIR,
    tables: Sequence[str] | None = None,
    refresh: bool = False,
) -> dict[str, Any]:
    """Bulk download Sharadar tables and build the local reproducible store."""

    root_path = Path(root)
    manifest_path = root_path / "manifest.json"
    selected = [_table_name(table) for table in (tables or DEFAULT_TABLES)]
    if manifest_path.exists() and not refresh:
        current = dict(json.loads(manifest_path.read_text(encoding="utf-8")))
        existing_tables = {_table_name(table) for table in dict(current.get("tables") or {})}
        missing_tables = [table for table in selected if table not in existing_tables]
        if not missing_tables:
            return current
    else:
        missing_tables = selected
    api_key = os.environ.get("NASDAQ_DATA_LINK_API_KEY")
    if not api_key:
        raise SharadarIngestionError("NASDAQ_DATA_LINK_API_KEY is required for Sharadar ingestion.")
    root_path.mkdir(parents=True, exist_ok=True)
    downloads = root_path / "downloads"
    downloads.mkdir(parents=True, exist_ok=True)
    zip_paths: dict[str, Path] = {}
    optional_errors: dict[str, str] = {}
    for table in missing_tables:
        try:
            zip_paths[table] = export_table_zip(table, downloads, api_key=api_key, refresh=refresh)
        except Exception as exc:
            if table == "SP500":
                optional_errors[table] = str(exc)
                continue
            raise
    if manifest_path.exists() and not refresh:
        manifest = build_store_from_sources(
            root_path,
            csv_paths=_existing_csv_paths(root_path),
            zip_paths=zip_paths,
            optional_errors=optional_errors,
        )
    else:
        manifest = build_store_from_exports(root_path, zip_paths, optional_errors=optional_errors)
    return manifest


def export_table_zip(table: str, output_dir: Path, *, api_key: str, refresh: bool = False) -> Path:
    """Download one Nasdaq Data Link bulk export as a local zip file.

    The nasdaqdatalink package has changed the shape of export_table responses
    over time, so this helper accepts common return forms: a local file path, a
    URL string, or an object exposing download_file/download.
    """

    table_name = _table_name(table)
    target = output_dir / f"{table_name}.zip"
    if target.exists() and not refresh:
        return target
    legacy_path = Path.cwd() / f"SHARADAR_{table_name}.zip"
    if legacy_path.exists() and legacy_path.stat().st_size > 0:
        return _copy_or_download(str(legacy_path), target)
    try:
        import nasdaqdatalink  # type: ignore
    except Exception as exc:
        raise SharadarIngestionError("Install nasdaqdatalink to run Sharadar ingestion.") from exc
    try:
        nasdaqdatalink.ApiConfig.api_key = api_key
    except Exception:
        pass
    result = nasdaqdatalink.export_table(f"SHARADAR/{table_name}", filename=str(target))
    if target.exists() and target.stat().st_size > 0:
        return target
    if isinstance(result, (str, Path)):
        return _copy_or_download(str(result), target)
    for method_name in ("download_file", "download"):
        method = getattr(result, method_name, None)
        if callable(method):
            downloaded = method(str(output_dir))
            if downloaded:
                return _copy_or_download(str(downloaded), target)
    file_attr = getattr(result, "file", None)
    link = getattr(file_attr, "link", None) if file_attr is not None else None
    if link:
        return _copy_or_download(str(link), target)
    if legacy_path.exists() and legacy_path.stat().st_size > 0:
        return _copy_or_download(str(legacy_path), target)
    raise SharadarIngestionError(f"Unsupported nasdaqdatalink export_table response for {table_name}.")


def build_store_from_exports(
    root: str | Path,
    zip_paths: Mapping[str, str | Path],
    *,
    optional_errors: Mapping[str, str] | None = None,
) -> dict[str, Any]:
    return build_store_from_sources(root, csv_paths={}, zip_paths=zip_paths, optional_errors=optional_errors)


def build_store_from_sources(
    root: str | Path,
    *,
    csv_paths: Mapping[str, str | Path],
    zip_paths: Mapping[str, str | Path],
    optional_errors: Mapping[str, str] | None = None,
) -> dict[str, Any]:
    root_path = Path(root)
    temp_dir = Path(tempfile.mkdtemp(prefix="sharadar_build_", dir=str(root_path.parent if root_path.parent.exists() else Path.cwd())))
    try:
        facts_dir = temp_dir / "facts"
        facts_dir.mkdir(parents=True, exist_ok=True)
        sqlite_path = temp_dir / "metadata.sqlite"
        stats: dict[str, dict[str, Any]] = {}
        ticker_map: dict[str, dict[str, int]] = {}
        csv_sources: dict[str, Path] = {_table_name(table): Path(path) for table, path in csv_paths.items()}
        for table, zip_path in zip_paths.items():
            table_name = _table_name(table)
            csv_sources[table_name] = _extract_first_csv(Path(zip_path), temp_dir / "csv" / table_name)
        with sqlite3.connect(sqlite_path) as conn:
            if "TICKERS" in csv_sources:
                csv_path = csv_sources["TICKERS"]
                stats["TICKERS"] = _write_meta_table("TICKERS", csv_path, conn)
                ticker_map = _ticker_permaticker_map(pd.read_csv(csv_path, low_memory=False))
            for table_name, csv_path in sorted(csv_sources.items()):
                table_name = _table_name(table_name)
                if table_name == "TICKERS":
                    continue
                if table_name in FACT_TABLES:
                    stats[table_name] = _write_fact_table(table_name, csv_path, facts_dir, ticker_map=ticker_map)
                elif table_name in META_TABLES:
                    stats[table_name] = _write_meta_table(table_name, csv_path, conn, ticker_map=ticker_map)
                else:
                    stats[table_name] = _write_meta_table(table_name, csv_path, conn, ticker_map=ticker_map)
        file_digests = _file_digests(temp_dir)
        manifest = _manifest(stats, file_digests, optional_errors=optional_errors or {})
        manifest_path = temp_dir / "manifest.json"
        manifest_path.write_text(json.dumps(manifest, indent=2, sort_keys=True), encoding="utf-8")
        if root_path.exists():
            backup = root_path.with_name(f"{root_path.name}.previous")
            if backup.exists():
                shutil.rmtree(backup)
            root_path.rename(backup)
        temp_dir.rename(root_path)
        return manifest
    except Exception:
        if temp_dir.exists():
            shutil.rmtree(temp_dir, ignore_errors=True)
        raise


def build_store_from_frames(
    root: str | Path,
    tables: Mapping[str, pd.DataFrame],
    *,
    optional_errors: Mapping[str, str] | None = None,
) -> dict[str, Any]:
    """Fixture-friendly local-store writer used by tests and small offline builds."""

    root_path = Path(root)
    if root_path.exists():
        shutil.rmtree(root_path)
    (root_path / "facts").mkdir(parents=True, exist_ok=True)
    stats: dict[str, dict[str, Any]] = {}
    with sqlite3.connect(root_path / "metadata.sqlite") as conn:
        ticker_map = _ticker_permaticker_map(tables.get("TICKERS", pd.DataFrame()))
        for table, frame in sorted(tables.items()):
            table_name = _table_name(table)
            normalized = _normalize_columns(frame)
            if table_name in FACT_TABLES:
                normalized = _add_permaticker(normalized, table_name, ticker_map)
                path = root_path / "facts" / f"{table_name}.parquet"
                normalized.to_parquet(path, index=False)
                stats[table_name] = _table_stats(normalized, table_name)
            else:
                if table_name != "TICKERS":
                    normalized = _add_permaticker(normalized, table_name, ticker_map)
                normalized.to_sql(table_name, conn, if_exists="replace", index=False)
                stats[table_name] = _table_stats(normalized, table_name)
    digests = _file_digests(root_path)
    manifest = _manifest(stats, digests, optional_errors=optional_errors or {})
    (root_path / "manifest.json").write_text(json.dumps(manifest, indent=2, sort_keys=True), encoding="utf-8")
    return manifest


def _existing_csv_paths(root: Path) -> dict[str, Path]:
    csv_root = root / "csv"
    if not csv_root.exists():
        return {}
    out: dict[str, Path] = {}
    for table_dir in sorted(path for path in csv_root.iterdir() if path.is_dir()):
        files = sorted(table_dir.glob("*.csv"))
        if files:
            out[_table_name(table_dir.name)] = files[0]
    return out


def sharadar_status(root: str | Path = DEFAULT_SHARADAR_DIR) -> dict[str, Any]:
    root_path = Path(root)
    manifest_path = root_path / "manifest.json"
    if not manifest_path.exists():
        return {"store_dir": str(root_path), "exists": False, "message": "No Sharadar snapshot found."}
    manifest = dict(json.loads(manifest_path.read_text(encoding="utf-8")))
    tables = manifest.get("tables") or {}
    return {
        "store_dir": str(root_path),
        "exists": True,
        "data_snapshot_hash": manifest.get("data_snapshot_hash"),
        "downloaded_at": manifest.get("downloaded_at"),
        "tables": {
            table: {
                "rows": meta.get("row_count"),
                "min_date": meta.get("min_date"),
                "max_date": meta.get("max_date"),
                "lastupdated": meta.get("lastupdated"),
            }
            for table, meta in sorted(dict(tables).items())
        },
        "optional_errors": manifest.get("optional_errors") or {},
    }


def validate_sample_stub() -> dict[str, Any]:
    return {
        "status": "not_implemented",
        "message": "EDGAR SF1 as-reported validation is a manual follow-up; this CLI stub is intentionally non-destructive.",
    }


def _copy_or_download(source: str, target: Path) -> Path:
    target.parent.mkdir(parents=True, exist_ok=True)
    if source.startswith("http://") or source.startswith("https://"):
        with urllib.request.urlopen(source) as response, target.open("wb") as handle:  # noqa: S310 - user-configured data vendor URL
            shutil.copyfileobj(response, handle, length=1024 * 1024)
        return target
    source_path = Path(source)
    if source_path.resolve() != target.resolve():
        shutil.copyfile(source_path, target)
    return target


def _extract_first_csv(zip_path: Path, output_dir: Path) -> Path:
    output_dir.mkdir(parents=True, exist_ok=True)
    with zipfile.ZipFile(zip_path) as archive:
        names = [name for name in archive.namelist() if name.lower().endswith(".csv")]
        if not names:
            raise SharadarIngestionError(f"No CSV member found in {zip_path}.")
        member = names[0]
        target = output_dir / Path(member).name
        with archive.open(member) as source, target.open("wb") as handle:
            shutil.copyfileobj(source, handle, length=1024 * 1024)
    return target


def _write_fact_table(
    table: str,
    csv_path: Path,
    facts_dir: Path,
    *,
    chunksize: int = 250_000,
    ticker_map: Mapping[str, Mapping[str, int]] | None = None,
) -> dict[str, Any]:
    table_dir = facts_dir / table
    table_dir.mkdir(parents=True, exist_ok=True)
    stats: dict[str, Any] = {"row_count": 0, "min_date": None, "max_date": None, "lastupdated": None}
    for idx, chunk in enumerate(pd.read_csv(csv_path, chunksize=chunksize, low_memory=False)):
        normalized = _normalize_columns(chunk)
        normalized = _add_permaticker(normalized, table, ticker_map or {})
        normalized = _sort_table(normalized, table)
        part_path = table_dir / f"part-{idx:05d}.parquet"
        normalized.to_parquet(part_path, index=False)
        _update_stats(stats, normalized)
    return stats


def _write_meta_table(
    table: str,
    csv_path: Path,
    conn: sqlite3.Connection,
    *,
    chunksize: int = 250_000,
    ticker_map: Mapping[str, Mapping[str, int]] | None = None,
) -> dict[str, Any]:
    stats: dict[str, Any] = {"row_count": 0, "min_date": None, "max_date": None, "lastupdated": None}
    first = True
    for chunk in pd.read_csv(csv_path, chunksize=chunksize, low_memory=False):
        normalized = _normalize_columns(chunk)
        if table != "TICKERS":
            normalized = _add_permaticker(normalized, table, ticker_map or {})
        normalized.to_sql(table, conn, if_exists="replace" if first else "append", index=False)
        _update_stats(stats, normalized)
        first = False
    return stats


def _table_stats(frame: pd.DataFrame, table: str) -> dict[str, Any]:
    del table
    stats: dict[str, Any] = {"row_count": int(len(frame)), "min_date": None, "max_date": None, "lastupdated": None}
    _update_stats(stats, frame)
    return stats


def _update_stats(stats: dict[str, Any], frame: pd.DataFrame) -> None:
    stats["row_count"] = int(stats.get("row_count") or 0) + int(len(frame))
    date_col = _first_existing(frame, ["date", "datekey", "calendardate", "lastpricedate"])
    if date_col:
        dates = pd.to_datetime(frame[date_col], errors="coerce").dropna()
        if not dates.empty:
            min_date = dates.min().date().isoformat()
            max_date = dates.max().date().isoformat()
            stats["min_date"] = min([value for value in [stats.get("min_date"), min_date] if value])
            stats["max_date"] = max([value for value in [stats.get("max_date"), max_date] if value])
    updated_col = _first_existing(frame, ["lastupdated", "updated", "last_updated"])
    if updated_col:
        values = pd.to_datetime(frame[updated_col], errors="coerce").dropna()
        if not values.empty:
            value = values.max().date().isoformat()
            stats["lastupdated"] = max([item for item in [stats.get("lastupdated"), value] if item])


def _manifest(stats: Mapping[str, dict[str, Any]], file_digests: Mapping[str, str], *, optional_errors: Mapping[str, str]) -> dict[str, Any]:
    snapshot_input = {"tables": stats, "file_digests": file_digests, "optional_errors": dict(optional_errors)}
    snapshot_hash = hashlib.sha256(json.dumps(snapshot_input, sort_keys=True, default=str).encode("utf-8")).hexdigest()
    return {
        "schema": "regime_sharadar_snapshot.v1",
        "downloaded_at": dt.datetime.now(dt.timezone.utc).isoformat(),
        "tables": {table: dict(meta) for table, meta in sorted(stats.items())},
        "file_digests": dict(sorted(file_digests.items())),
        "optional_errors": dict(optional_errors),
        "data_snapshot_hash": snapshot_hash,
    }


def _file_digests(root: Path) -> dict[str, str]:
    out: dict[str, str] = {}
    for path in sorted(root.rglob("*")):
        if not path.is_file() or path.name == "manifest.json":
            continue
        digest = hashlib.sha256()
        with path.open("rb") as handle:
            for chunk in iter(lambda: handle.read(1024 * 1024), b""):
                digest.update(chunk)
        out[str(path.relative_to(root))] = digest.hexdigest()
    return out


def _sort_table(frame: pd.DataFrame, table: str) -> pd.DataFrame:
    keys = ["permaticker"]
    if table in {"SEP", "DAILY"}:
        keys.append("date")
    elif table == "SF1":
        keys.extend(["datekey", "dimension"])
    keys = [key for key in keys if key in frame.columns]
    return frame.sort_values(keys) if keys else frame


def _normalize_columns(frame: pd.DataFrame) -> pd.DataFrame:
    out = frame.copy()
    out.columns = [str(column).strip().lower().replace(" ", "_") for column in out.columns]
    return out


def _ticker_permaticker_map(frame: pd.DataFrame) -> dict[str, dict[str, int]]:
    normalized = _normalize_columns(frame) if not frame.empty else pd.DataFrame()
    if normalized.empty or "ticker" not in normalized.columns or "permaticker" not in normalized.columns:
        return {}
    normalized["ticker"] = normalized["ticker"].astype(str).str.upper()
    normalized["permaticker"] = pd.to_numeric(normalized["permaticker"], errors="coerce")
    normalized = normalized.dropna(subset=["ticker", "permaticker"]).copy()
    out: dict[str, dict[str, int]] = {"*": {}}
    for _, row in normalized.iterrows():
        ticker = str(row["ticker"]).upper()
        perma = int(row["permaticker"])
        table = str(row.get("table") or "*").upper()
        out.setdefault(table, {})[ticker] = perma
        out["*"].setdefault(ticker, perma)
    return out


def _add_permaticker(
    frame: pd.DataFrame,
    table: str,
    ticker_map: Mapping[str, Mapping[str, int]],
) -> pd.DataFrame:
    if frame.empty or "permaticker" in frame.columns or "ticker" not in frame.columns or not ticker_map:
        return frame
    table_map = dict(ticker_map.get(_table_name(table)) or {})
    fallback_map = dict(ticker_map.get("*") or {})
    tickers = frame["ticker"].astype(str).str.upper()
    mapped = tickers.map(table_map)
    if fallback_map:
        mapped = mapped.fillna(tickers.map(fallback_map))
    out = frame.copy()
    out["permaticker"] = pd.to_numeric(mapped, errors="coerce").astype("Int64")
    return out


def _first_existing(frame: pd.DataFrame, names: Sequence[str]) -> str | None:
    columns = set(frame.columns)
    for name in names:
        if name in columns:
            return name
    return None


def _table_name(table: str) -> str:
    return str(table or "").strip().upper()
