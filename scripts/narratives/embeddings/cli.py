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
from .visualize import generate_all_visualizations, generate_interactive_visualizations, prepare_visualization


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

        # Auto-sync to OneDrive after successful build (unless --no-sync)
        if not args.no_sync and not result.errors:
            print()
            config.sync_to_onedrive()

    except ValueError as e:
        print(f"Error: {e}", file=sys.stderr)
        sys.exit(1)


def cmd_sync(args):
    """Sync local database to OneDrive."""
    config.sync_to_onedrive()


def cmd_enrich(args):
    """Enrich existing chunks with structured metadata (locations, CSI, companies)."""
    import time
    from pathlib import Path
    from .manifest import Manifest
    from .metadata_enrichment import (
        extract_document_metadata,
        extract_chunk_metadata,
        load_company_aliases
    )
    from src.config.settings import settings

    METADATA_VERSION = "v1"  # Increment when enrichment logic changes

    source = args.source
    force = args.force
    limit = args.limit

    print(f"Enriching {source} chunks with structured metadata...")
    print(f"Metadata version: {METADATA_VERSION}")
    if force:
        print("Force mode: Re-enriching all files")
    if limit:
        print(f"Limit: Processing max {limit} files")
    print()

    # Load company aliases
    print("Loading company aliases from dim_company.csv...")
    company_aliases = load_company_aliases()
    if company_aliases:
        print(f"  Loaded {len(company_aliases)} company aliases")
    else:
        print("  Warning: No company aliases loaded. Company extraction will be skipped.")
    print()

    # Get source directory
    source_dir = config.SOURCE_DIRS.get(source)
    if not source_dir:
        print(f"Error: Unknown source '{source}'")
        sys.exit(1)

    # Load manifest and store
    manifest = Manifest(config.MANIFEST_PATH)
    store = get_store()

    # Determine which files need enrichment
    if force:
        # Re-enrich all indexed files
        files_to_enrich = list(manifest.get_all_files(source).keys())
    else:
        # Only enrich files not at current metadata version
        files_to_enrich = manifest.get_unenriched_files(source, METADATA_VERSION)

    if not files_to_enrich:
        print(f"All {source} files already enriched with {METADATA_VERSION}")
        return

    print(f"Files to enrich: {len(files_to_enrich)}")
    if limit:
        files_to_enrich = files_to_enrich[:limit]
        print(f"Processing first {len(files_to_enrich)} files (--limit {limit})")
    print()

    # Enrich files
    start_time = time.time()
    files_processed = 0
    files_errors = 0
    chunks_enriched = 0

    for i, relative_path in enumerate(files_to_enrich, 1):
        # Get full file path
        file_path = source_dir / relative_path

        if not file_path.exists():
            print(f"[{i}/{len(files_to_enrich)}] Skipped (file not found): {relative_path}")
            files_errors += 1
            continue

        try:
            # Read full document text for document-level extraction
            # Get all chunks and concatenate their text
            from .chunker import chunk_document
            chunks_list = list(chunk_document(file_path))

            if not chunks_list:
                print(f"[{i}/{len(files_to_enrich)}] Skipped (no chunks): {relative_path}")
                continue

            # Concatenate all chunk text for document-level extraction
            full_text = "\n".join(chunk.text for chunk in chunks_list)

            if not full_text:
                print(f"[{i}/{len(files_to_enrich)}] Skipped (no text): {relative_path}")
                continue

            # Extract document-level metadata
            doc_metadata = extract_document_metadata(
                full_text,
                company_aliases,
                include_csi_keywords=True
            )

            # Get existing chunks for this file
            # Use the base filename without full path for source_file lookup
            source_file = relative_path
            existing_chunks = store.get_chunks_by_file(source_file)

            if not existing_chunks:
                print(f"[{i}/{len(files_to_enrich)}] Skipped (no chunks in DB): {relative_path}")
                continue

            # Enrich each chunk
            chunk_updates = []
            for chunk in existing_chunks:
                # Extract chunk-level metadata
                chunk_metadata = extract_chunk_metadata(
                    chunk.text,
                    company_aliases,
                    include_csi_keywords=False  # More conservative for chunks
                )

                # Merge doc + chunk metadata
                enriched_metadata = {**doc_metadata, **chunk_metadata}

                # Add to batch
                chunk_updates.append((chunk.id, enriched_metadata))

            # Batch update all chunks for this file
            if chunk_updates:
                store.update_chunks_metadata_batch(chunk_updates)
                chunks_enriched += len(chunk_updates)

            # Mark as enriched in manifest
            manifest.mark_enriched(source, relative_path, METADATA_VERSION)

            files_processed += 1

            # Progress update
            if i % 10 == 0 or i == len(files_to_enrich):
                elapsed = time.time() - start_time
                rate = files_processed / elapsed if elapsed > 0 else 0
                print(f"[{i}/{len(files_to_enrich)}] {relative_path}")
                print(f"  Progress: {files_processed} processed, {chunks_enriched} chunks enriched ({rate:.1f} files/sec)")

        except Exception as e:
            print(f"[{i}/{len(files_to_enrich)}] Error: {relative_path}")
            print(f"  {type(e).__name__}: {e}")
            files_errors += 1

    # Save manifest
    manifest.save()

    # Summary
    elapsed = time.time() - start_time
    print()
    print("=" * 70)
    print(f"Enrichment Complete - {source}")
    print("=" * 70)
    print(f"Files processed: {files_processed}")
    print(f"Files errors: {files_errors}")
    print(f"Chunks enriched: {chunks_enriched}")
    print(f"Time elapsed: {int(elapsed)}s")
    print(f"Metadata version: {METADATA_VERSION}")
    print("=" * 70)

    # Auto-sync to OneDrive (unless --no-sync)
    if not args.no_sync:
        print()
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


def cmd_visualize(args):
    """Generate embedding visualizations."""
    from pathlib import Path

    output_dir = Path(args.output) if args.output else config.CHROMA_PATH.parent / "visualizations"
    n_dimensions = 3 if args.three_d else 2

    try:
        if args.interactive:
            data = generate_interactive_visualizations(
                output_dir=output_dir,
                source_type=args.source,
                limit=args.limit,
                n_neighbors=args.n_neighbors,
                min_dist=args.min_dist,
                min_cluster_size=args.min_cluster_size,
                n_dimensions=n_dimensions,
                generate_labels=not args.no_labels,
                verbose=True
            )
        else:
            data = generate_all_visualizations(
                output_dir=output_dir,
                source_type=args.source,
                limit=args.limit,
                n_neighbors=args.n_neighbors,
                min_dist=args.min_dist,
                min_cluster_size=args.min_cluster_size,
                n_dimensions=n_dimensions,
                generate_labels=not args.no_labels,
                create_gif=args.gif,
                verbose=True
            )

        # Print summary
        n_clusters = len([c for c in data.cluster_info if c != -1])
        n_noise = (data.cluster_labels == -1).sum()
        mode = "Interactive" if args.interactive else "Static"
        print()
        print("=" * 60)
        print(f"Visualization Summary ({n_dimensions}D {mode})")
        print("=" * 60)
        print(f"Total points: {len(data.ids):,}")
        print(f"Clusters: {n_clusters}")
        print(f"Noise points: {n_noise:,} ({n_noise/len(data.ids)*100:.1f}%)")
        print()
        print("Top clusters:")
        sorted_clusters = sorted(
            [(cid, info) for cid, info in data.cluster_info.items() if cid != -1],
            key=lambda x: -x[1].size
        )[:10]
        for cid, info in sorted_clusters:
            print(f"  {info.label}: {info.size:,} points")
        print("=" * 60)

    except ImportError as e:
        print(f"Error: Missing dependency - {e}")
        print("Install with: pip install umap-learn hdbscan matplotlib scipy plotly")
        sys.exit(1)
    except Exception as e:
        print(f"Error: {e}")
        sys.exit(1)


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
  # Build the index (requires --source, auto-syncs to OneDrive on success)
  python -m scripts.narratives.embeddings build --source narratives
  python -m scripts.narratives.embeddings build --source raba --limit 100
  python -m scripts.narratives.embeddings build --source narratives --force

  # Enrich existing chunks with structured metadata (locations, CSI, companies)
  python -m scripts.narratives.embeddings enrich --source narratives
  python -m scripts.narratives.embeddings enrich --source narratives --limit 10

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
        "--no-sync",
        action="store_true",
        help="Skip automatic sync to OneDrive after successful build"
    )
    build_parser.add_argument(
        "--cleanup-deleted",
        action="store_true",
        help="Delete chunks for files that no longer exist in source (only for full runs without --limit)"
    )
    build_parser.set_defaults(func=cmd_build)

    # Enrich command
    enrich_parser = subparsers.add_parser(
        "enrich",
        help="Enrich existing chunks with structured metadata (locations, CSI, companies)",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Enrich all narratives with metadata (auto-syncs on success)
  python -m scripts.narratives.embeddings enrich --source narratives

  # Test on subset
  python -m scripts.narratives.embeddings enrich --source narratives --limit 10

  # Force re-enrichment
  python -m scripts.narratives.embeddings enrich --source narratives --force

  # Enrich without syncing to OneDrive
  python -m scripts.narratives.embeddings enrich --source narratives --no-sync

Metadata extracted:
  - Location codes (FAB116101), buildings (SUE), levels (1F)
  - CSI codes (033053) and sections (03)
  - Company IDs (from dim_company.csv)

Note: Enrichment does NOT re-embed. It only updates metadata on existing chunks.
"""
    )
    enrich_parser.add_argument(
        "--source", "-s",
        required=True,
        choices=list(config.SOURCE_DIRS.keys()),
        help="Source to enrich (required): narratives, raba, or psi"
    )
    enrich_parser.add_argument(
        "--force", "-f",
        action="store_true",
        help="Force re-enrichment of all files (ignore metadata version)"
    )
    enrich_parser.add_argument(
        "--limit", "-n",
        type=int,
        default=None,
        help="Limit number of files to process (for testing)"
    )
    enrich_parser.add_argument(
        "--no-sync",
        action="store_true",
        help="Skip automatic sync to OneDrive after successful enrichment"
    )
    enrich_parser.set_defaults(func=cmd_enrich)

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

    # Visualize command
    viz_parser = subparsers.add_parser(
        "visualize",
        help="Generate embedding visualizations (UMAP + clustering)",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Generate 2D static images (default)
  python -m scripts.narratives.embeddings visualize

  # Generate interactive HTML (zoom, pan, hover)
  python -m scripts.narratives.embeddings visualize --interactive

  # Generate interactive 3D (rotate in browser)
  python -m scripts.narratives.embeddings visualize --interactive --3d

  # Generate 3D static images (multiple angle views)
  python -m scripts.narratives.embeddings visualize --3d

  # Generate 3D with rotating GIF animation
  python -m scripts.narratives.embeddings visualize --3d --gif

  # Filter by source
  python -m scripts.narratives.embeddings visualize --source narratives

  # Custom output directory
  python -m scripts.narratives.embeddings visualize -o ./my_plots

  # Tune UMAP/clustering parameters
  python -m scripts.narratives.embeddings visualize --n-neighbors 30 --min-cluster-size 100

  # Skip LLM labeling (faster, uses generic labels)
  python -m scripts.narratives.embeddings visualize --no-labels

  # Test with subset of data
  python -m scripts.narratives.embeddings visualize --limit 1000
"""
    )
    viz_parser.add_argument(
        "--source", "-s",
        choices=list(config.SOURCE_DIRS.keys()),
        help="Filter by source type"
    )
    viz_parser.add_argument(
        "--output", "-o",
        help="Output directory for visualizations (default: ~/.local/share/samsung-embeddings/visualizations)"
    )
    viz_parser.add_argument(
        "--limit", "-n",
        type=int,
        help="Limit number of points (for testing)"
    )
    viz_parser.add_argument(
        "--n-neighbors",
        type=int,
        default=15,
        help="UMAP n_neighbors parameter (default: 15, higher=more global structure)"
    )
    viz_parser.add_argument(
        "--min-dist",
        type=float,
        default=0.1,
        help="UMAP min_dist parameter (default: 0.1, lower=tighter clusters)"
    )
    viz_parser.add_argument(
        "--min-cluster-size",
        type=int,
        default=50,
        help="HDBSCAN min_cluster_size (default: 50)"
    )
    viz_parser.add_argument(
        "--no-labels",
        action="store_true",
        help="Skip LLM-generated cluster labels (faster)"
    )
    viz_parser.add_argument(
        "--3d",
        dest="three_d",
        action="store_true",
        help="Generate 3D visualizations (multiple angle views)"
    )
    viz_parser.add_argument(
        "--gif",
        action="store_true",
        help="Create rotating GIF animation (only with --3d, slower)"
    )
    viz_parser.add_argument(
        "--interactive", "-i",
        action="store_true",
        help="Generate interactive HTML files (Plotly) instead of static images"
    )
    viz_parser.set_defaults(func=cmd_visualize)

    args = parser.parse_args()
    args.func(args)


if __name__ == "__main__":
    main()
