"""CLI interface for narrative embeddings."""

import argparse
import sys
from datetime import datetime
from pathlib import Path
from typing import List

from . import config
from .builder import build_index
from .store import (
    get_store,
    search_chunks,
    ChunkResult
)


def cmd_build(args):
    """Build or rebuild the embeddings index."""
    try:
        result = build_index(
            source=args.source,
            force=args.force,
            verbose=True,
            limit=args.limit,
            cleanup_deleted=args.cleanup_deleted
        )

        if result.errors:
            sys.exit(1)

        # Optionally sync to OneDrive after successful build
        if args.sync and not result.errors:
            print()
            config.sync_to_onedrive()

    except ValueError as e:
        print(f"Error: {e}", file=sys.stderr)
        sys.exit(1)


def cmd_sync(args):
    """Sync local database to OneDrive."""
    config.sync_to_onedrive()


def cmd_search(args):
    """Search the embeddings index."""
    query = " ".join(args.query)

    results = search_chunks(
        query=query,
        source_type=args.source,
        document_type=args.type,
        author=args.author,
        subfolder=args.subfolder,
        after=args.after,
        before=args.before,
        limit=args.limit
    )

    print_chunk_results(results, query, args.context)


def cmd_status(args):
    """Show index status."""
    from .manifest import Manifest

    store = get_store()
    stats = store.get_chunks_stats()
    manifest = Manifest(config.MANIFEST_PATH)

    print("=" * 60)
    print("Document Embeddings Index Status")
    print("=" * 60)
    print(f"ChromaDB: {stats['chroma_path']}")
    print(f"Manifest: {config.MANIFEST_PATH}")
    print()

    # Manifest stats per source
    print("Indexed files (from manifest):")
    for source_name in config.SOURCE_DIRS.keys():
        file_count = manifest.get_file_count(source_name)
        chunk_count = manifest.get_chunk_count(source_name)
        print(f"  {source_name}: {file_count} files, {chunk_count} chunks")
    print()

    # ChromaDB stats (for verification)
    print(f"ChromaDB total: {stats['chunks_count']} chunks, {stats['files_count']} files")
    print()

    # Backup info
    from .backup import BackupManager
    manager = BackupManager(config.CHROMA_PATH, config.BACKUP_DIR)
    backups = manager.list_backups()
    if backups:
        latest = backups[0]
        size_mb = latest.stat().st_size / 1024 / 1024
        print(f"Latest backup: {latest.name} ({size_mb:.1f} MB)")
    else:
        print("No backups available")

    print("=" * 60)


def cmd_backup(args):
    """Create a backup of the database."""
    from .backup import BackupManager

    manager = BackupManager(config.CHROMA_PATH, config.BACKUP_DIR)
    backup_path = manager.create_backup()

    if backup_path:
        print(f"Backup created: {backup_path}")
        print(f"Size: {backup_path.stat().st_size / 1024 / 1024:.1f} MB")
    else:
        print("Nothing to backup (database empty)")


def cmd_restore(args):
    """Restore from a backup."""
    from .backup import BackupManager

    manager = BackupManager(config.CHROMA_PATH, config.BACKUP_DIR)

    if args.backup:
        backup_path = Path(args.backup)
    else:
        backup_path = manager.get_latest_backup()
        if not backup_path:
            print("No backups available")
            return

    if not backup_path.exists():
        print(f"Backup not found: {backup_path}")
        return

    print(f"Restoring from: {backup_path}")

    if not args.yes:
        response = input("This will replace the current database. Continue? [y/N] ")
        if response.lower() != 'y':
            print("Aborted")
            return

    manager.restore_backup(backup_path)
    print("Restore complete")


def cmd_list_backups(args):
    """List available backups."""
    from .backup import BackupManager

    manager = BackupManager(config.CHROMA_PATH, config.BACKUP_DIR)
    backups = manager.list_backups()

    if not backups:
        print("No backups available")
        return

    print(f"Available backups ({len(backups)}):")
    for backup in backups:
        size_mb = backup.stat().st_size / 1024 / 1024
        mtime = datetime.fromtimestamp(backup.stat().st_mtime)
        print(f"  {backup.name}  ({size_mb:.1f} MB, {mtime:%Y-%m-%d %H:%M})")


def cmd_verify(args):
    """Verify manifest and ChromaDB are in sync."""
    from .manifest import Manifest

    manifest = Manifest(config.MANIFEST_PATH)
    store = get_store()

    issues = []

    for source_name in config.SOURCE_DIRS.keys():
        manifest_chunks = manifest.get_all_chunk_ids(source_name)

        # Get ChromaDB chunks for this source
        all_meta = store.get_all_chunk_metadata()
        db_chunks = {
            chunk_id for chunk_id, meta in all_meta.items()
            if meta.get("source_type") == source_name
        }

        # Check for mismatches
        in_manifest_not_db = manifest_chunks - db_chunks
        in_db_not_manifest = db_chunks - manifest_chunks

        if in_manifest_not_db:
            issues.append(f"{source_name}: {len(in_manifest_not_db)} chunks in manifest but not in DB")

        if in_db_not_manifest:
            issues.append(f"{source_name}: {len(in_db_not_manifest)} chunks in DB but not in manifest")

    if issues:
        print("Issues found:")
        for issue in issues:
            print(f"  - {issue}")
        print()
        print("Run 'rebuild' to fix, or 'restore' to revert to backup")
        return 1
    else:
        print("Manifest and ChromaDB are in sync")
        return 0


def print_chunk_results(results: List[ChunkResult], query: str, context: int):
    """Print chunk search results."""
    if not results:
        print(f"No matches found for: {query}")
        return

    print(f"\nFound {len(results)} matches:\n")

    store = get_store() if context > 0 else None

    for i, r in enumerate(results, 1):
        print(f"[{i}] Score: {r.score}")

        # Truncate long text
        text = r.text
        if len(text) > 500:
            text = text[:500] + "..."
        # Clean up whitespace for display
        text = " ".join(text.split())
        print(f"    Text: \"{text}\"")

        # Metadata line
        meta_parts = []
        source_type = r.metadata.get("source_type", "")
        if source_type:
            meta_parts.append(f"Source: {source_type}")
        if r.document_type:
            meta_parts.append(f"Type: {r.document_type}")
        if r.file_date:
            meta_parts.append(f"Date: {r.file_date}")
        if r.author:
            meta_parts.append(f"Author: {r.author}")
        if meta_parts:
            print(f"    {' | '.join(meta_parts)}")

        # Source info
        page_str = f" (page {r.page_number})" if r.page_number > 0 else ""
        chunk_info = f"chunk {r.metadata.get('chunk_index', 0)+1}/{r.metadata.get('total_chunks', 1)}"
        print(f"    Source: {r.source_file}{page_str} [{chunk_info}]")

        # Context chunks
        if context > 0 and store:
            chunk_index = r.metadata.get("chunk_index", 0)
            adjacent = store.get_adjacent_chunks(
                source_file=r.source_file,
                chunk_index=chunk_index,
                before=context,
                after=context
            )

            if adjacent:
                print()
                print("    Context:")
                for adj in adjacent:
                    adj_idx = adj.metadata.get("chunk_index", 0)
                    direction = "prev" if adj_idx < chunk_index else "next"
                    adj_text = " ".join(adj.text.split())[:100]
                    if len(adj.text) > 100:
                        adj_text += "..."
                    print(f"      [{direction}] \"{adj_text}\"")

        print()


def main():
    """Main CLI entry point."""
    valid_sources = ", ".join(config.SOURCE_DIRS.keys())

    parser = argparse.ArgumentParser(
        description="Document embeddings - semantic search for raw documents",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=f"""
Examples:
  # Build the index (requires --source)
  python -m scripts.narratives.embeddings build --source narratives
  python -m scripts.narratives.embeddings build --source raba --limit 100
  python -m scripts.narratives.embeddings build --source narratives --force

  # Search all sources
  python -m scripts.narratives.embeddings search "HVAC delays"

  # Search specific source
  python -m scripts.narratives.embeddings search "inspection failed" --source raba
  python -m scripts.narratives.embeddings search "scope changes" --source narratives

  # Search with filters
  python -m scripts.narratives.embeddings search "delay" --author Yates
  python -m scripts.narratives.embeddings search "meeting" --subfolder meeting_notes
  python -m scripts.narratives.embeddings search "delay" --after 2024-01-01 --before 2024-06-30
  python -m scripts.narratives.embeddings search "delay" --context 2

  # Check status
  python -m scripts.narratives.embeddings status

Valid sources: {valid_sources}
"""
    )

    subparsers = parser.add_subparsers(dest="command", required=True)

    # Build command
    build_parser = subparsers.add_parser("build", help="Build or rebuild the embeddings index")
    build_parser.add_argument(
        "--source", "-s",
        required=True,
        choices=list(config.SOURCE_DIRS.keys()),
        help="Source to build (required): narratives, raba, or psi"
    )
    build_parser.add_argument(
        "--force", "-f",
        action="store_true",
        help="Force rebuild all embeddings (ignore cache)"
    )
    build_parser.add_argument(
        "--limit", "-n",
        type=int,
        default=None,
        help="Limit number of files to process (for testing)"
    )
    build_parser.add_argument(
        "--sync",
        action="store_true",
        help="Sync to OneDrive after successful build"
    )
    build_parser.add_argument(
        "--cleanup-deleted",
        action="store_true",
        help="Delete chunks for files that no longer exist in source (only for full runs without --limit)"
    )
    build_parser.set_defaults(func=cmd_build)

    # Search command
    search_parser = subparsers.add_parser("search", help="Search document chunks")
    search_parser.add_argument(
        "query",
        nargs="+",
        help="Search query"
    )
    search_parser.add_argument(
        "--source", "-s",
        choices=list(config.SOURCE_DIRS.keys()),
        help="Filter by source (narratives, raba, psi)"
    )
    search_parser.add_argument(
        "--type", "-t",
        help="Filter by document type (schedule_narrative, meeting_notes, etc.)"
    )
    search_parser.add_argument(
        "--author", "-a",
        help="Filter by author (Yates, SECAI, BRG, Samsung)"
    )
    search_parser.add_argument(
        "--subfolder",
        help="Filter by subfolder (substring match)"
    )
    search_parser.add_argument(
        "--after",
        help="Filter files dated after (YYYY-MM-DD)"
    )
    search_parser.add_argument(
        "--before",
        help="Filter files dated before (YYYY-MM-DD)"
    )
    search_parser.add_argument(
        "--limit", "-n",
        type=int,
        default=config.DEFAULT_LIMIT,
        help=f"Maximum results (default: {config.DEFAULT_LIMIT})"
    )
    search_parser.add_argument(
        "--context", "-C",
        type=int,
        default=config.DEFAULT_CONTEXT,
        help="Number of adjacent chunks to show for context"
    )
    search_parser.set_defaults(func=cmd_search)

    # Status command
    status_parser = subparsers.add_parser("status", help="Show index status")
    status_parser.set_defaults(func=cmd_status)

    # Sync command
    sync_parser = subparsers.add_parser("sync", help="Sync local database to OneDrive")
    sync_parser.set_defaults(func=cmd_sync)

    # Backup command
    backup_parser = subparsers.add_parser("backup", help="Create a backup")
    backup_parser.set_defaults(func=cmd_backup)

    # Restore command
    restore_parser = subparsers.add_parser("restore", help="Restore from backup")
    restore_parser.add_argument(
        "--backup", "-b",
        help="Path to backup file (default: latest)"
    )
    restore_parser.add_argument(
        "--yes", "-y",
        action="store_true",
        help="Skip confirmation prompt"
    )
    restore_parser.set_defaults(func=cmd_restore)

    # List backups command
    list_backups_parser = subparsers.add_parser("list-backups", help="List available backups")
    list_backups_parser.set_defaults(func=cmd_list_backups)

    # Verify command
    verify_parser = subparsers.add_parser("verify", help="Verify manifest/DB consistency")
    verify_parser.set_defaults(func=cmd_verify)

    args = parser.parse_args()
    args.func(args)


if __name__ == "__main__":
    main()
