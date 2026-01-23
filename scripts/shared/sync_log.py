#!/usr/bin/env python3
"""
Centralized Sync Logging for Data Pipelines

Tracks all data sync operations across pipelines with:
- Timestamp
- Pipeline name
- Sync type (incremental, full, scrape, etc.)
- Files processed
- Records added/removed (delta)
- Status and error messages

Usage:
    from scripts.shared.sync_log import SyncLog, SyncType

    # Log a sync operation
    SyncLog.log(
        pipeline="tbm",
        sync_type=SyncType.INCREMENTAL,
        files_processed=5,
        records_delta=150,
        status="success"
    )

    # View recent logs
    SyncLog.show_recent(10)

    # Get DataFrame of all logs
    df = SyncLog.get_logs()
"""

import json
import sys
from datetime import datetime
from enum import Enum
from pathlib import Path
from typing import Optional

import pandas as pd

# Add project root to path
sys.path.insert(0, str(Path(__file__).parent.parent.parent))
from src.config.settings import Settings


class SyncType(str, Enum):
    """Types of sync operations."""
    INCREMENTAL = "incremental"  # Process only new files
    FULL = "full"                # Reprocess all files
    DRY_RUN = "dry_run"          # Preview without processing
    SCRAPE = "scrape"            # Web scraping operation
    DOCUMENT = "document"        # Document processing pipeline
    ENRICH = "enrich"            # Dimension enrichment
    CONSOLIDATE = "consolidate"  # Consolidation step
    MANUAL = "manual"            # Manual/ad-hoc run


class SyncLog:
    """Centralized sync logging for all data pipelines."""

    # Log file location
    LOG_FILE = Settings.DATA_DIR / "sync_log.csv"

    # CSV columns
    COLUMNS = [
        "timestamp",
        "pipeline",
        "sync_type",
        "files_processed",
        "files_skipped",
        "files_failed",
        "records_before",
        "records_after",
        "records_delta",
        "duration_seconds",
        "status",
        "message",
    ]

    @classmethod
    def log(
        cls,
        pipeline: str,
        sync_type: SyncType | str,
        files_processed: int = 0,
        files_skipped: int = 0,
        files_failed: int = 0,
        records_before: int = 0,
        records_after: int = 0,
        records_delta: Optional[int] = None,
        duration_seconds: Optional[float] = None,
        status: str = "success",
        message: str = "",
    ) -> dict:
        """
        Log a sync operation.

        Args:
            pipeline: Pipeline name (e.g., "tbm", "primavera", "raba")
            sync_type: Type of sync operation
            files_processed: Number of new files processed
            files_skipped: Number of files skipped (already processed)
            files_failed: Number of files that failed
            records_before: Record count before sync (for delta calculation)
            records_after: Record count after sync
            records_delta: Explicit delta (overrides calculation from before/after)
            duration_seconds: Time taken for the operation
            status: "success", "partial", "error", "no_change"
            message: Optional message or error description

        Returns:
            The log entry as a dict
        """
        # Calculate delta if not provided
        if records_delta is None:
            records_delta = records_after - records_before

        # Normalize sync_type
        if isinstance(sync_type, SyncType):
            sync_type = sync_type.value

        entry = {
            "timestamp": datetime.now().isoformat(),
            "pipeline": pipeline.lower(),
            "sync_type": sync_type,
            "files_processed": files_processed,
            "files_skipped": files_skipped,
            "files_failed": files_failed,
            "records_before": records_before,
            "records_after": records_after,
            "records_delta": records_delta,
            "duration_seconds": round(duration_seconds, 2) if duration_seconds else None,
            "status": status,
            "message": message,
        }

        # Append to log file
        cls._append_entry(entry)

        return entry

    @classmethod
    def _append_entry(cls, entry: dict) -> None:
        """Append a single entry to the log file."""
        # Create parent directory if needed
        cls.LOG_FILE.parent.mkdir(parents=True, exist_ok=True)

        # Check if file exists and has headers
        write_header = not cls.LOG_FILE.exists() or cls.LOG_FILE.stat().st_size == 0

        # Append entry
        df = pd.DataFrame([entry])
        df.to_csv(
            cls.LOG_FILE,
            mode="a",
            header=write_header,
            index=False,
            columns=cls.COLUMNS,
        )

    @classmethod
    def get_logs(
        cls,
        pipeline: Optional[str] = None,
        sync_type: Optional[str] = None,
        since: Optional[str] = None,
        limit: Optional[int] = None,
    ) -> pd.DataFrame:
        """
        Get sync logs as DataFrame with optional filtering.

        Args:
            pipeline: Filter by pipeline name
            sync_type: Filter by sync type
            since: Filter entries after this ISO timestamp
            limit: Limit number of entries returned (most recent)

        Returns:
            DataFrame of log entries
        """
        if not cls.LOG_FILE.exists():
            return pd.DataFrame(columns=cls.COLUMNS)

        df = pd.read_csv(cls.LOG_FILE)

        # Apply filters
        if pipeline:
            df = df[df["pipeline"] == pipeline.lower()]
        if sync_type:
            df = df[df["sync_type"] == sync_type]
        if since:
            df = df[df["timestamp"] >= since]

        # Sort by timestamp descending
        df = df.sort_values("timestamp", ascending=False)

        # Apply limit
        if limit:
            df = df.head(limit)

        return df

    @classmethod
    def show_recent(cls, n: int = 10, pipeline: Optional[str] = None) -> None:
        """Print recent sync logs to console."""
        df = cls.get_logs(pipeline=pipeline, limit=n)

        if df.empty:
            print("No sync logs found.")
            return

        print(f"\n{'='*80}")
        print(f"Recent Sync Logs{f' ({pipeline})' if pipeline else ''}")
        print(f"{'='*80}")

        for _, row in df.iterrows():
            ts = row["timestamp"][:19].replace("T", " ")
            delta_str = f"+{row['records_delta']}" if row["records_delta"] >= 0 else str(row["records_delta"])
            duration = f" ({row['duration_seconds']:.1f}s)" if pd.notna(row["duration_seconds"]) else ""

            status_icon = {
                "success": "✓",
                "partial": "⚠",
                "error": "✗",
                "no_change": "○",
            }.get(row["status"], "?")

            print(
                f"{status_icon} {ts} | {row['pipeline']:12} | {row['sync_type']:12} | "
                f"files: {row['files_processed']:3} | records: {delta_str:>8}{duration}"
            )
            if row["message"]:
                print(f"    {row['message']}")

        print(f"{'='*80}\n")

    @classmethod
    def get_summary(cls, days: int = 7) -> pd.DataFrame:
        """
        Get summary statistics for recent sync operations.

        Args:
            days: Number of days to include in summary

        Returns:
            DataFrame with summary by pipeline
        """
        since = (datetime.now() - pd.Timedelta(days=days)).isoformat()
        df = cls.get_logs(since=since)

        if df.empty:
            return pd.DataFrame()

        summary = df.groupby("pipeline").agg({
            "timestamp": "count",
            "files_processed": "sum",
            "records_delta": "sum",
            "status": lambda x: (x == "success").sum(),
        }).rename(columns={
            "timestamp": "syncs",
            "status": "successful",
        })

        summary["success_rate"] = (summary["successful"] / summary["syncs"] * 100).round(1)

        return summary.sort_values("syncs", ascending=False)


# Convenience function for CLI
def main():
    """CLI to view sync logs."""
    import argparse

    parser = argparse.ArgumentParser(description="View sync logs")
    parser.add_argument("-n", "--limit", type=int, default=20, help="Number of entries to show")
    parser.add_argument("-p", "--pipeline", type=str, help="Filter by pipeline")
    parser.add_argument("--summary", action="store_true", help="Show summary statistics")
    parser.add_argument("--days", type=int, default=7, help="Days to include in summary")

    args = parser.parse_args()

    if args.summary:
        summary = SyncLog.get_summary(days=args.days)
        if summary.empty:
            print("No sync logs found.")
        else:
            print(f"\nSync Summary (last {args.days} days):")
            print(summary.to_string())
            print()
    else:
        SyncLog.show_recent(n=args.limit, pipeline=args.pipeline)


if __name__ == "__main__":
    main()
