"""Build and update the narrative embeddings index from raw documents."""

import hashlib
from dataclasses import dataclass, field
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


def scan_documents(root_dir: Path) -> Iterator[Path]:
    """Scan directory for supported document files."""
    for ext in SUPPORTED_EXTENSIONS:
        for filepath in root_dir.rglob(f"*{ext}"):
            # Skip files in extracted folders or temp files
            path_str = str(filepath).lower()
            if any(skip in path_str for skip in SKIP_PATTERNS):
                continue
            yield filepath


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
    file_meta: Dict[str, Any]
) -> Dict[str, Any]:
    """Build metadata dict for a chunk, including file-level metadata."""
    # Get relative path from narratives dir
    try:
        rel_path = filepath.relative_to(config.NARRATIVES_RAW_DIR)
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
) -> int:
    """Embed and store a batch of chunks. Returns count of chunks stored."""
    if not chunk_batch:
        return 0

    chunk_ids = [c[0] for c in chunk_batch]
    texts = [c[1] for c in chunk_batch]

    if verbose:
        print(f"  Embedding and storing {len(chunk_batch)} chunks...")

    # Generate embeddings
    embeddings = embed_for_index(texts, verbose=verbose)

    # Store immediately
    store.upsert_chunks(
        ids=chunk_ids,
        texts=texts,
        embeddings=embeddings,
        metadatas=metadata_batch
    )

    return len(chunk_batch)


def build_index(
    force: bool = False,
    verbose: bool = True,
    limit: Optional[int] = None,
    batch_size: int = INCREMENTAL_BATCH_SIZE
) -> BuildResult:
    """Build or update the embeddings index from raw narratives.

    Args:
        force: If True, rebuild all embeddings ignoring cache.
        verbose: If True, print progress messages.
        limit: If set, only process this many files (for testing).
        batch_size: Number of files to process before storing (for incremental saves).

    Returns:
        BuildResult with counts of operations performed.
    """
    result = BuildResult()
    store = get_store()

    if verbose:
        print("=" * 60)
        print("Narrative Embeddings Builder (Raw Documents)")
        print("=" * 60)
        print(f"Source: {config.NARRATIVES_RAW_DIR}")
        print(f"ChromaDB: {config.CHROMA_PATH}")
        print(f"Force rebuild: {force}")
        print(f"Incremental batch size: {batch_size} files")
        if limit:
            print(f"Limit: {limit} files")
        print()

    # Get existing chunk metadata from ChromaDB
    existing_chunks = store.get_all_chunk_metadata()
    if verbose:
        print(f"Existing chunks in index: {len(existing_chunks)}")

    # Build a map of file_hash -> list of chunk_ids for that file
    file_to_chunks: Dict[str, List[str]] = {}
    for chunk_id, meta in existing_chunks.items():
        file_hash = meta.get("file_hash", "")
        if file_hash not in file_to_chunks:
            file_to_chunks[file_hash] = []
        file_to_chunks[file_hash].append(chunk_id)

    # Scan for documents
    all_documents = list(scan_documents(config.NARRATIVES_RAW_DIR))
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

    # Track which chunk IDs we've seen (for deletion detection)
    seen_chunk_ids: Set[str] = set()

    # Batch accumulators for incremental processing
    current_batch_chunks: List[Tuple[str, str]] = []
    current_batch_metadata: List[Dict[str, Any]] = []
    files_in_current_batch = 0

    for i, filepath in enumerate(documents, 1):
        if verbose:
            print(f"[{i}/{len(documents)}] {filepath.name[:60]}...", end=" ")

        try:
            file_hash = get_file_hash(filepath)

            # Check if file is unchanged
            if not force and file_hash in file_to_chunks:
                # File unchanged, mark chunks as seen
                for chunk_id in file_to_chunks[file_hash]:
                    seen_chunk_ids.add(chunk_id)
                result.files_unchanged += 1
                result.chunks_unchanged += len(file_to_chunks[file_hash])
                if verbose:
                    print("unchanged")
                continue

            # File is new or changed, chunk it
            chunks = list(chunk_document(filepath))

            if not chunks:
                result.files_skipped += 1
                if verbose:
                    print("no content")
                continue

            # Extract file-level metadata
            file_meta = extract_file_metadata(filepath, config.NARRATIVES_RAW_DIR).to_dict()

            # Get relative path for unique chunk IDs (handles duplicate filenames in subdirs)
            try:
                rel_path = filepath.relative_to(config.NARRATIVES_RAW_DIR)
            except ValueError:
                rel_path = Path(filepath.name)

            # Add chunks to batch
            for chunk in chunks:
                # Generate unique chunk ID using relative path (not just filename)
                safe_path = str(rel_path).replace("/", "_").replace("\\", "_")
                chunk_id = f"{safe_path}__c{chunk.chunk_index:04d}"
                seen_chunk_ids.add(chunk_id)
                current_batch_chunks.append((chunk_id, chunk.text))
                current_batch_metadata.append(build_chunk_metadata(chunk, filepath, file_hash, file_meta))

            result.files_processed += 1
            files_in_current_batch += 1
            if verbose:
                print(f"{len(chunks)} chunks")

            # Process batch if we've accumulated enough files
            if files_in_current_batch >= batch_size:
                chunks_stored = _process_file_batch(
                    store, current_batch_chunks, current_batch_metadata, verbose
                )
                result.chunks_added += chunks_stored
                current_batch_chunks = []
                current_batch_metadata = []
                files_in_current_batch = 0

        except Exception as e:
            result.files_errors += 1
            result.errors.append(f"{filepath.name}: {e}")
            if verbose:
                print(f"ERROR: {e}")

    # Process any remaining chunks in the final batch
    if current_batch_chunks:
        if verbose:
            print(f"\nProcessing final batch...")
        chunks_stored = _process_file_batch(
            store, current_batch_chunks, current_batch_metadata, verbose
        )
        result.chunks_added += chunks_stored

    # Find stale chunks to delete
    stale_chunk_ids = set(existing_chunks.keys()) - seen_chunk_ids
    if stale_chunk_ids:
        if verbose:
            print(f"\nDeleting {len(stale_chunk_ids)} stale chunks...")
        store.delete_chunks(list(stale_chunk_ids))
        result.chunks_deleted = len(stale_chunk_ids)

    if verbose:
        print("\n" + "=" * 60)
        print("Build Summary")
        print("=" * 60)
        print(f"Files: {result.files_processed} processed, {result.files_unchanged} unchanged, "
              f"{result.files_skipped} skipped, {result.files_duplicates} duplicates, {result.files_errors} errors")
        print(f"Chunks: {result.total_chunks} total")
        print(f"  Added: {result.chunks_added}")
        print(f"  Unchanged: {result.chunks_unchanged}")
        print(f"  Deleted: {result.chunks_deleted}")

        if result.errors:
            print(f"\nErrors ({len(result.errors)}):")
            for err in result.errors[:10]:
                print(f"  - {err}")
            if len(result.errors) > 10:
                print(f"  ... and {len(result.errors) - 10} more")

        print("=" * 60)

    return result
