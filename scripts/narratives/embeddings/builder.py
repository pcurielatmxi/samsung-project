"""Build and update the document embeddings index from raw documents."""

import hashlib
import time
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from pathlib import Path
from typing import List, Dict, Any, Optional, Iterator, Tuple, Set

from . import config
from .client import embed_for_index
from .store import get_store
from .chunker import chunk_document, Chunk
from .metadata import extract_file_metadata


# Supported file extensions
SUPPORTED_EXTENSIONS = {".pdf", ".docx", ".doc", ".txt", ".xlsx", ".xls", ".pptx"}

# Skip patterns (extracted text files, temp files)
SKIP_PATTERNS = {"extracted", ".tmp", "~$"}

# Batch size for incremental embedding/storage (files per batch)
INCREMENTAL_BATCH_SIZE = 10

# Cost estimation (Gemini embedding: ~$0.00001 per 1K chars, but free tier is generous)
# Using conservative estimate for paid tier
COST_PER_1K_CHARS = 0.00001


@dataclass
class ProgressTracker:
    """Track build progress with timing and cost estimates."""

    total_files: int
    start_time: float = field(default_factory=time.time)

    # Counters
    files_processed: int = 0
    files_skipped: int = 0  # No content
    files_unchanged: int = 0
    files_errors: int = 0
    chunks_embedded: int = 0
    chars_embedded: int = 0

    # Timing
    last_status_time: float = field(default_factory=time.time)
    status_interval: float = 30.0  # Print status every 30 seconds

    def file_done(self, status: str, chunks: int = 0, chars: int = 0):
        """Record a file completion."""
        if status == "processed":
            self.files_processed += 1
            self.chunks_embedded += chunks
            self.chars_embedded += chars
        elif status == "unchanged":
            self.files_unchanged += 1
        elif status == "skipped":
            self.files_skipped += 1
        elif status == "error":
            self.files_errors += 1

    @property
    def files_done(self) -> int:
        return self.files_processed + self.files_unchanged + self.files_skipped + self.files_errors

    @property
    def files_remaining(self) -> int:
        return self.total_files - self.files_done

    @property
    def elapsed_seconds(self) -> float:
        return time.time() - self.start_time

    @property
    def files_per_second(self) -> float:
        if self.elapsed_seconds < 1:
            return 0
        return self.files_done / self.elapsed_seconds

    @property
    def eta_seconds(self) -> Optional[float]:
        if self.files_per_second < 0.001:
            return None
        return self.files_remaining / self.files_per_second

    @property
    def estimated_cost(self) -> float:
        return (self.chars_embedded / 1000) * COST_PER_1K_CHARS

    def format_duration(self, seconds: float) -> str:
        """Format seconds as human-readable duration."""
        if seconds < 60:
            return f"{seconds:.0f}s"
        elif seconds < 3600:
            return f"{seconds/60:.1f}m"
        else:
            return f"{seconds/3600:.1f}h"

    def should_print_status(self) -> bool:
        """Check if it's time to print a status update."""
        now = time.time()
        if now - self.last_status_time >= self.status_interval:
            self.last_status_time = now
            return True
        return False

    def get_status_line(self) -> str:
        """Get a formatted status line."""
        elapsed = self.format_duration(self.elapsed_seconds)

        eta_str = "calculating..."
        if self.eta_seconds is not None:
            eta_str = self.format_duration(self.eta_seconds)

        rate = f"{self.files_per_second:.1f}" if self.files_per_second > 0 else "—"

        return (
            f"Progress: {self.files_done}/{self.total_files} files "
            f"({self.files_processed} new, {self.files_unchanged} unchanged, "
            f"{self.files_skipped} skipped, {self.files_errors} errors) | "
            f"Chunks: {self.chunks_embedded} | "
            f"Rate: {rate} files/s | "
            f"Elapsed: {elapsed} | ETA: {eta_str} | "
            f"Est. cost: ${self.estimated_cost:.4f}"
        )

    def print_status(self):
        """Print current status."""
        print(f"\n{'─' * 80}")
        print(self.get_status_line())
        print(f"{'─' * 80}\n")


def scan_documents(root_dir: Path) -> Iterator[Path]:
    """Scan directory for supported document files."""
    for ext in SUPPORTED_EXTENSIONS:
        for filepath in root_dir.rglob(f"*{ext}"):
            # Skip files in extracted folders or temp files
            path_str = str(filepath).lower()
            if any(skip in path_str for skip in SKIP_PATTERNS):
                continue
            yield filepath


def scan_documents_for_source(source_type: str) -> Iterator[Path]:
    """Scan documents for a specific source type.

    Args:
        source_type: One of the keys in config.SOURCE_DIRS (e.g., "narratives", "raba", "psi").

    Yields:
        Path objects for each document file.

    Raises:
        ValueError: If source_type is not a valid source.
    """
    if source_type not in config.SOURCE_DIRS:
        valid = ", ".join(config.SOURCE_DIRS.keys())
        raise ValueError(f"Unknown source_type '{source_type}'. Valid sources: {valid}")

    root_dir = config.SOURCE_DIRS[source_type]
    if not root_dir.exists():
        raise ValueError(f"Source directory does not exist: {root_dir}")

    yield from scan_documents(root_dir)


def deduplicate_files(files: List[Path]) -> Tuple[List[Path], int]:
    """Deduplicate files by (name, size).

    Returns:
        Tuple of (unique_files, duplicate_count)
    """
    seen: Dict[Tuple[str, int], Path] = {}
    unique = []
    duplicates = 0

    for filepath in files:
        try:
            key = (filepath.name, filepath.stat().st_size)
            if key not in seen:
                seen[key] = filepath
                unique.append(filepath)
            else:
                duplicates += 1
        except OSError:
            # File access error, skip
            continue

    return unique, duplicates


def get_file_hash(filepath: Path) -> str:
    """Get hash of file for change detection (uses mtime + size for speed)."""
    stat = filepath.stat()
    content = f"{filepath.name}:{stat.st_size}:{stat.st_mtime}"
    return hashlib.md5(content.encode()).hexdigest()


def build_chunk_metadata(
    chunk: Chunk,
    filepath: Path,
    file_hash: str,
    file_meta: Dict[str, Any],
    source_root: Path,
    source_type: str
) -> Dict[str, Any]:
    """Build metadata dict for a chunk, including file-level metadata.

    Args:
        chunk: The chunk object.
        filepath: Path to the source file.
        file_hash: Hash for change detection.
        file_meta: File-level metadata dict.
        source_root: Root directory for this source.
        source_type: Source type identifier (e.g., "narratives", "raba", "psi").
    """
    # Get relative path from source root
    try:
        rel_path = filepath.relative_to(source_root)
    except ValueError:
        rel_path = filepath.name

    # Combine chunk-level and file-level metadata
    return {
        # Chunk-specific (for sequence/context)
        "source_file": chunk.source_file,
        "relative_path": str(rel_path),
        "chunk_index": chunk.chunk_index,
        "total_chunks": chunk.total_chunks,
        "page_number": chunk.page_number,
        "chunk_type": chunk.file_type,
        "file_hash": file_hash,
        # Source identification
        "source_type": source_type,
        # File-level (for filtering)
        "file_date": file_meta.get("file_date", ""),
        "author": file_meta.get("author", ""),
        "document_type": file_meta.get("document_type", ""),
        "subfolder": file_meta.get("subfolder", ""),
        "file_size_kb": file_meta.get("file_size_kb", 0),
    }


@dataclass
class BuildResult:
    """Result of a build operation."""
    files_processed: int = 0
    files_skipped: int = 0
    files_unchanged: int = 0
    files_duplicates: int = 0
    files_errors: int = 0
    chunks_added: int = 0
    chunks_updated: int = 0
    chunks_deleted: int = 0
    chunks_unchanged: int = 0
    errors: List[str] = field(default_factory=list)

    @property
    def total_chunks(self) -> int:
        return self.chunks_added + self.chunks_updated + self.chunks_unchanged


def _process_file_batch(
    store,
    chunk_batch: List[Tuple[str, str]],
    metadata_batch: List[Dict[str, Any]],
    verbose: bool = True
) -> Tuple[int, int]:
    """Embed and store a batch of chunks.

    Returns:
        Tuple of (chunks_stored, total_chars_embedded)
    """
    if not chunk_batch:
        return 0, 0

    chunk_ids = [c[0] for c in chunk_batch]
    texts = [c[1] for c in chunk_batch]
    total_chars = sum(len(t) for t in texts)

    if verbose:
        print(f"  Embedding {len(chunk_batch)} chunks ({total_chars:,} chars)...")

    # Generate embeddings
    embeddings = embed_for_index(texts)

    # Store immediately
    store.upsert_chunks(
        ids=chunk_ids,
        texts=texts,
        embeddings=embeddings,
        metadatas=metadata_batch
    )

    return len(chunk_batch), total_chars


def build_index(
    source: Optional[str] = None,
    force: bool = False,
    verbose: bool = True,
    limit: Optional[int] = None,
    batch_size: int = INCREMENTAL_BATCH_SIZE
) -> BuildResult:
    """Build or update the embeddings index for a specific source.

    Args:
        source: Source type to build (e.g., "narratives", "raba", "psi").
                Required - must specify which source to build.
        force: If True, rebuild all embeddings ignoring cache.
        verbose: If True, print progress messages.
        limit: If set, only process this many files (for testing).
        batch_size: Number of files to process before storing (for incremental saves).

    Returns:
        BuildResult with counts of operations performed.

    Raises:
        ValueError: If source is not specified or invalid.
    """
    if not source:
        valid = ", ".join(config.SOURCE_DIRS.keys())
        raise ValueError(f"Must specify --source. Valid sources: {valid}")

    if source not in config.SOURCE_DIRS:
        valid = ", ".join(config.SOURCE_DIRS.keys())
        raise ValueError(f"Unknown source '{source}'. Valid sources: {valid}")

    source_root = config.SOURCE_DIRS[source]

    result = BuildResult()
    store = get_store()

    if verbose:
        print("=" * 80)
        print(f"Document Embeddings Builder - {source}")
        print("=" * 80)
        print(f"Source: {source_root}")
        print(f"ChromaDB: {config.CHROMA_PATH}")
        print(f"Force rebuild: {force}")
        print(f"Incremental batch size: {batch_size} files")
        if limit:
            print(f"Limit: {limit} files")
        print()

    # Get existing chunk metadata from ChromaDB (only for this source)
    all_existing_chunks = store.get_all_chunk_metadata()
    existing_chunks = {
        chunk_id: meta
        for chunk_id, meta in all_existing_chunks.items()
        if meta.get("source_type", "") == source or meta.get("source_type", "") == ""
    }
    if verbose:
        print(f"Existing chunks for '{source}': {len(existing_chunks)}")

    # Build a map of file_hash -> list of chunk_ids for that file
    file_to_chunks: Dict[str, List[str]] = {}
    for chunk_id, meta in existing_chunks.items():
        file_hash = meta.get("file_hash", "")
        if file_hash not in file_to_chunks:
            file_to_chunks[file_hash] = []
        file_to_chunks[file_hash].append(chunk_id)

    # Scan for documents
    all_documents = list(scan_documents_for_source(source))
    if verbose:
        print(f"Found {len(all_documents)} total documents")

    # Deduplicate by (name, size)
    documents, dup_count = deduplicate_files(all_documents)
    result.files_duplicates = dup_count
    if verbose:
        print(f"After deduplication: {len(documents)} unique files ({dup_count} duplicates skipped)")
        print()

    if limit:
        documents = documents[:limit]

    # Initialize progress tracker
    progress = ProgressTracker(total_files=len(documents))

    # Track which chunk IDs we've seen (for deletion detection)
    seen_chunk_ids: Set[str] = set()

    # Batch accumulators for incremental processing
    current_batch_chunks: List[Tuple[str, str]] = []
    current_batch_metadata: List[Dict[str, Any]] = []
    files_in_current_batch = 0
    total_chars_embedded = 0

    for i, filepath in enumerate(documents, 1):
        if verbose:
            print(f"[{i}/{len(documents)}] {filepath.name[:55]}...", end=" ", flush=True)

        try:
            file_hash = get_file_hash(filepath)

            # Check if file is unchanged
            if not force and file_hash in file_to_chunks:
                # File unchanged, mark chunks as seen
                for chunk_id in file_to_chunks[file_hash]:
                    seen_chunk_ids.add(chunk_id)
                result.files_unchanged += 1
                result.chunks_unchanged += len(file_to_chunks[file_hash])
                progress.file_done("unchanged")
                if verbose:
                    print("unchanged")
                continue

            # File is new or changed, chunk it
            chunks = list(chunk_document(filepath))

            if not chunks:
                result.files_skipped += 1
                progress.file_done("skipped")
                if verbose:
                    print("no content")
                continue

            # Extract file-level metadata
            file_meta = extract_file_metadata(filepath, source_root, source).to_dict()

            # Get relative path for unique chunk IDs (handles duplicate filenames in subdirs)
            try:
                rel_path = filepath.relative_to(source_root)
            except ValueError:
                rel_path = Path(filepath.name)

            # Calculate chars for this file
            file_chars = sum(len(c.text) for c in chunks)

            # Add chunks to batch
            for chunk in chunks:
                # Generate unique chunk ID: source_type/relative_path__cNNNN
                safe_path = str(rel_path).replace("/", "_").replace("\\", "_")
                chunk_id = f"{source}__{safe_path}__c{chunk.chunk_index:04d}"
                seen_chunk_ids.add(chunk_id)
                current_batch_chunks.append((chunk_id, chunk.text))
                current_batch_metadata.append(
                    build_chunk_metadata(chunk, filepath, file_hash, file_meta, source_root, source)
                )

            result.files_processed += 1
            files_in_current_batch += 1
            if verbose:
                print(f"{len(chunks)} chunks ({file_chars:,} chars)")

            # Process batch if we've accumulated enough files
            if files_in_current_batch >= batch_size:
                chunks_stored, chars_stored = _process_file_batch(
                    store, current_batch_chunks, current_batch_metadata, verbose
                )
                result.chunks_added += chunks_stored
                total_chars_embedded += chars_stored
                progress.file_done("processed", chunks_stored, chars_stored)
                current_batch_chunks = []
                current_batch_metadata = []
                files_in_current_batch = 0

                # Print periodic status update
                if verbose and progress.should_print_status():
                    progress.print_status()
            else:
                # Track progress even before batch is processed
                progress.file_done("processed", len(chunks), file_chars)

        except Exception as e:
            result.files_errors += 1
            result.errors.append(f"{filepath.name}: {e}")
            progress.file_done("error")
            if verbose:
                print(f"ERROR: {e}")

        # Print periodic status update (for unchanged files too)
        if verbose and progress.should_print_status():
            progress.print_status()

    # Process any remaining chunks in the final batch
    if current_batch_chunks:
        if verbose:
            print(f"\nProcessing final batch...")
        chunks_stored, chars_stored = _process_file_batch(
            store, current_batch_chunks, current_batch_metadata, verbose
        )
        result.chunks_added += chunks_stored
        total_chars_embedded += chars_stored

    # Find stale chunks to delete (only for this source)
    stale_chunk_ids = set(existing_chunks.keys()) - seen_chunk_ids
    if stale_chunk_ids:
        if verbose:
            print(f"\nDeleting {len(stale_chunk_ids)} stale chunks for '{source}'...")
        store.delete_chunks(list(stale_chunk_ids))
        result.chunks_deleted = len(stale_chunk_ids)

    # Final summary
    if verbose:
        elapsed = progress.format_duration(progress.elapsed_seconds)
        cost = (total_chars_embedded / 1000) * COST_PER_1K_CHARS

        print("\n" + "=" * 80)
        print(f"Build Complete - {source}")
        print("=" * 80)
        print(f"Files: {result.files_processed} processed, {result.files_unchanged} unchanged, "
              f"{result.files_skipped} skipped, {result.files_duplicates} duplicates, {result.files_errors} errors")
        print(f"Chunks: {result.total_chunks} total")
        print(f"  Added: {result.chunks_added}")
        print(f"  Unchanged: {result.chunks_unchanged}")
        print(f"  Deleted: {result.chunks_deleted}")
        print(f"Characters embedded: {total_chars_embedded:,}")
        print(f"Time elapsed: {elapsed}")
        print(f"Estimated cost: ${cost:.4f}")

        if result.errors:
            print(f"\nErrors ({len(result.errors)}):")
            for err in result.errors[:10]:
                print(f"  - {err}")
            if len(result.errors) > 10:
                print(f"  ... and {len(result.errors) - 10} more")

        print("=" * 80)

    return result
