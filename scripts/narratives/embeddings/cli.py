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
    result = build_index(
        force=args.force,
        verbose=True,
        limit=args.limit
    )

    if result.errors:
        sys.exit(1)


def cmd_search(args):
    """Search the embeddings index."""
    query = " ".join(args.query)

    results = search_chunks(
        query=query,
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
    print("Narrative Embeddings Index Status")
    print("=" * 60)
    print(f"ChromaDB location: {stats['chroma_path']}")
    print()

    print("Index contents:")
    print(f"  Chunks: {stats['chunks_count']}")
    print(f"  Files: {stats['files_count']}")
    print()

    # Check source directory
    from .builder import scan_documents
    try:
        documents = list(scan_documents(config.NARRATIVES_RAW_DIR))
        print("Source directory status:")
        print(f"  Raw narratives path: {config.NARRATIVES_RAW_DIR}")
        print(f"  Documents found: {len(documents)} files")
        print()

        # List document types
        by_ext = {}
        for doc in documents:
            ext = doc.suffix.lower()
            by_ext[ext] = by_ext.get(ext, 0) + 1

        if by_ext:
            print("  By file type:")
            for ext, count in sorted(by_ext.items(), key=lambda x: -x[1]):
                print(f"    {ext}: {count}")

    except Exception as e:
        print(f"Error scanning source directory: {e}")

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
    parser = argparse.ArgumentParser(
        description="Narrative embeddings - semantic search for raw narrative documents",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Build the index
  python -m scripts.narratives.embeddings build
  python -m scripts.narratives.embeddings build --limit 10  # Test with 10 files
  python -m scripts.narratives.embeddings build --force     # Force rebuild all

  # Search chunks
  python -m scripts.narratives.embeddings search "HVAC delays"
  python -m scripts.narratives.embeddings search "scope changes" --type schedule_narrative
  python -m scripts.narratives.embeddings search "delay" --author Yates
  python -m scripts.narratives.embeddings search "meeting" --subfolder meeting_notes
  python -m scripts.narratives.embeddings search "delay" --context 2  # Show adjacent chunks

  # Search with date filters
  python -m scripts.narratives.embeddings search "delay" --after 2024-01-01 --before 2024-06-30

  # Check status
  python -m scripts.narratives.embeddings status
"""
    )

    subparsers = parser.add_subparsers(dest="command", required=True)

    # Build command
    build_parser = subparsers.add_parser("build", help="Build or rebuild the embeddings index")
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
    build_parser.set_defaults(func=cmd_build)

    # Search command
    search_parser = subparsers.add_parser("search", help="Search document chunks")
    search_parser.add_argument(
        "query",
        nargs="+",
        help="Search query"
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
        "--subfolder", "-s",
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

    args = parser.parse_args()
    args.func(args)


if __name__ == "__main__":
    main()
