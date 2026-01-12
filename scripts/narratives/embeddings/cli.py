"""CLI interface for narrative embeddings."""

import argparse
import sys
from typing import List, Optional

from . import config
from .builder import build_index, load_documents, load_statements
from .store import (
    get_store,
    search_statements,
    search_documents,
    StatementResult,
    SearchResult
)


def cmd_build(args):
    """Build or rebuild the embeddings index."""
    result = build_index(force=args.force, verbose=True)

    if result.errors:
        sys.exit(1)


def cmd_search(args):
    """Search the embeddings index."""
    query = " ".join(args.query)

    if args.documents:
        results = search_documents(
            query=query,
            doc_type=args.type,
            after=args.after,
            before=args.before,
            limit=args.limit
        )
        print_document_results(results, query)
    else:
        results = search_statements(
            query=query,
            category=args.category,
            party=args.party,
            location=args.location,
            after=args.after,
            before=args.before,
            limit=args.limit,
            context=args.context
        )
        print_statement_results(results, query, args.context)


def cmd_status(args):
    """Show index status."""
    store = get_store()
    stats = store.get_stats()

    print("=" * 60)
    print("Narrative Embeddings Index Status")
    print("=" * 60)
    print(f"ChromaDB location: {stats['chroma_path']}")
    print()

    print("Index contents:")
    print(f"  Documents: {stats['documents_count']}")
    print(f"  Statements: {stats['statements_count']}")
    print()

    # Check source data
    try:
        documents = load_documents()
        statements = load_statements()
        print("Source CSV status:")
        print(f"  dim_narrative_file.csv: {len(documents)} records")
        print(f"  narrative_statements.csv: {len(statements)} records")
        print()

        # Sync status
        docs_synced = stats['documents_count'] == len(documents)
        stmts_synced = stats['statements_count'] == len(statements)

        if docs_synced and stmts_synced:
            print("Sync status: IN SYNC")
        else:
            print("Sync status: OUT OF SYNC")
            if not docs_synced:
                print(f"  Documents: index={stats['documents_count']}, csv={len(documents)}")
            if not stmts_synced:
                print(f"  Statements: index={stats['statements_count']}, csv={len(statements)}")
            print("\nRun 'build' to synchronize.")

    except FileNotFoundError as e:
        print(f"Source data not found: {e}")
        print("Run the narratives document processing pipeline first.")

    print("=" * 60)


def print_statement_results(results: List[StatementResult], query: str, context: int):
    """Print statement search results."""
    if not results:
        print(f"No matches found for: {query}")
        return

    print(f"\nFound {len(results)} matches in statements:\n")

    for i, r in enumerate(results, 1):
        print(f"[{i}] Score: {r.score}")
        print(f"    Statement: \"{r.text}\"")

        # Metadata line
        meta_parts = []
        if r.metadata.get("category"):
            meta_parts.append(f"Category: {r.metadata['category']}")
        if r.metadata.get("event_date"):
            meta_parts.append(f"Event: {r.metadata['event_date']}")
        if r.metadata.get("impact_days") and r.metadata["impact_days"] > 0:
            meta_parts.append(f"Impact: {r.metadata['impact_days']} days")
        if meta_parts:
            print(f"    {' | '.join(meta_parts)}")

        # Parties and locations
        if r.metadata.get("parties"):
            print(f"    Parties: {r.metadata['parties'].replace('|', ', ')}")
        if r.metadata.get("locations"):
            print(f"    Location: {r.metadata['locations'].replace('|', ', ')}")

        # Source info
        source_page = r.metadata.get("source_page", -1)
        page_str = f" (page {source_page})" if source_page > 0 else ""
        print(f"    Source: {r.metadata.get('narrative_file_id', 'unknown')}{page_str}")

        # Context statements
        if context > 0:
            if r.prev_statements or r.next_statements:
                print()
                print("    Context:")
                for prev in r.prev_statements:
                    print(f"      [prev] \"{prev.text[:100]}{'...' if len(prev.text) > 100 else ''}\"")
                for next_ in r.next_statements:
                    print(f"      [next] \"{next_.text[:100]}{'...' if len(next_.text) > 100 else ''}\"")

        print()


def print_document_results(results: List[SearchResult], query: str):
    """Print document search results."""
    if not results:
        print(f"No matching documents for: {query}")
        return

    print(f"\nFound {len(results)} matching documents:\n")

    for i, r in enumerate(results, 1):
        print(f"[{i}] Score: {r.score}")
        print(f"    Document: {r.metadata.get('title', 'Untitled')} ({r.id})")

        # Metadata line
        meta_parts = []
        if r.metadata.get("type"):
            meta_parts.append(f"Type: {r.metadata['type']}")
        if r.metadata.get("date"):
            meta_parts.append(f"Date: {r.metadata['date']}")
        if r.metadata.get("data_date"):
            meta_parts.append(f"Data Date: {r.metadata['data_date']}")
        if meta_parts:
            print(f"    {' | '.join(meta_parts)}")

        if r.metadata.get("author"):
            print(f"    Author: {r.metadata['author']}")

        print(f"    Path: {r.metadata.get('path', 'unknown')}")
        print(f"    Statements: {r.metadata.get('statement_count', 0)}")

        # Summary (truncated)
        if r.text:
            summary = r.text[:300] + "..." if len(r.text) > 300 else r.text
            print()
            print(f"    Summary: {summary}")

        print()


def main():
    """Main CLI entry point."""
    parser = argparse.ArgumentParser(
        description="Narrative embeddings - semantic search for narrative documents",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Build the index
  python -m scripts.narratives.embeddings build

  # Search statements
  python -m scripts.narratives.embeddings search "HVAC delays"
  python -m scripts.narratives.embeddings search "scope changes" --category scope_change
  python -m scripts.narratives.embeddings search "delay" --context 2

  # Search documents
  python -m scripts.narratives.embeddings search "milestone variance" --documents

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
    build_parser.set_defaults(func=cmd_build)

    # Search command
    search_parser = subparsers.add_parser("search", help="Search the embeddings index")
    search_parser.add_argument(
        "query",
        nargs="+",
        help="Search query"
    )
    search_parser.add_argument(
        "--documents", "-d",
        action="store_true",
        help="Search document summaries instead of statements"
    )
    search_parser.add_argument(
        "--category", "-c",
        help="Filter by statement category (delay, scope_change, quality_issue, etc.)"
    )
    search_parser.add_argument(
        "--party", "-p",
        help="Filter by party (substring match)"
    )
    search_parser.add_argument(
        "--location", "-l",
        help="Filter by location (substring match)"
    )
    search_parser.add_argument(
        "--type", "-t",
        help="Filter by document type (for --documents mode)"
    )
    search_parser.add_argument(
        "--after",
        help="Filter events after date (YYYY-MM-DD)"
    )
    search_parser.add_argument(
        "--before",
        help="Filter events before date (YYYY-MM-DD)"
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
        help="Number of surrounding statements to show"
    )
    search_parser.set_defaults(func=cmd_search)

    # Status command
    status_parser = subparsers.add_parser("status", help="Show index status")
    status_parser.set_defaults(func=cmd_status)

    args = parser.parse_args()
    args.func(args)


if __name__ == "__main__":
    main()
