"""CLI interface for narrative embeddings."""

import argparse
import sys
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
    store = get_store()
    stats = store.get_chunks_stats()

    print("=" * 60)
    print("Document Embeddings Index Status")
    print("=" * 60)
    print(f"ChromaDB location: {stats['chroma_path']}")
    print()

    print("Index contents:")
    print(f"  Total chunks: {stats['chunks_count']}")
    print(f"  Total files: {stats['files_count']}")
    print()

    # Get breakdown by source_type
    all_meta = store.get_all_chunk_metadata()
    by_source = {}
    source_files = {}
    for chunk_id, meta in all_meta.items():
        src = meta.get("source_type", "") or "unknown"
        by_source[src] = by_source.get(src, 0) + 1
        if src not in source_files:
            source_files[src] = set()
        source_files[src].add(meta.get("source_file", ""))

    if by_source:
        print("  By source:")
        for src in sorted(by_source.keys()):
            chunk_count = by_source[src]
            file_count = len(source_files[src])
            print(f"    {src}: {chunk_count} chunks ({file_count} files)")
        print()

    # Check each source directory
    from .builder import scan_documents_for_source
    print("Source directories:")
    for source_name, source_dir in config.SOURCE_DIRS.items():
        try:
            docs = list(scan_documents_for_source(source_name))
            indexed = len(source_files.get(source_name, set()))
            status = "✓" if indexed == len(docs) else f"{indexed}/{len(docs)}"
            print(f"  {source_name}: {len(docs)} files [{status}]")
            print(f"    Path: {source_dir}")
        except Exception as e:
            print(f"  {source_name}: ERROR - {e}")

    print()
    print("=" * 60)


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

    args = parser.parse_args()
    args.func(args)


if __name__ == "__main__":
    main()
