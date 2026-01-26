"""ChromaDB storage operations for narrative embeddings."""

from dataclasses import dataclass, field
from typing import List, Dict, Any, Optional, Set
import chromadb
from chromadb.config import Settings

from . import config
from .client import embed_for_query


@dataclass
class SearchResult:
    """A single search result with metadata."""
    id: str
    text: str
    score: float
    metadata: Dict[str, Any]


@dataclass
class ChunkResult(SearchResult):
    """Chunk search result with source file info."""

    @property
    def source_file(self) -> str:
        return self.metadata.get("source_file", "")

    @property
    def page_number(self) -> int:
        return self.metadata.get("page_number", 0)

    @property
    def file_date(self) -> str:
        return self.metadata.get("file_date", "")

    @property
    def author(self) -> str:
        return self.metadata.get("author", "")

    @property
    def document_type(self) -> str:
        return self.metadata.get("document_type", "")


class EmbeddingStore:
    """ChromaDB-based embedding storage."""

    def __init__(self, path: Optional[str] = None):
        """Initialize ChromaDB client.

        If no local database exists, attempts to copy from OneDrive.
        """
        # Ensure local WSL database exists (copy from OneDrive if needed)
        config.ensure_local_db()

        self.path = path or str(config.CHROMA_PATH)

        self.client = chromadb.PersistentClient(
            path=self.path,
            settings=Settings(anonymized_telemetry=False)
        )

    def get_chunks_collection(self):
        """Get or create the chunks collection."""
        return self.client.get_or_create_collection(
            name=config.CHUNKS_COLLECTION,
            metadata={"hnsw:space": "cosine"}
        )

    # --- Chunk operations (new architecture) ---

    def get_all_chunk_metadata(self) -> Dict[str, Dict[str, Any]]:
        """Get all chunk IDs and their metadata."""
        collection = self.get_chunks_collection()
        result = collection.get(include=["metadatas"])

        if not result["ids"]:
            return {}

        return {
            id_: meta
            for id_, meta in zip(result["ids"], result["metadatas"])
        }

    def upsert_chunks(
        self,
        ids: List[str],
        texts: List[str],
        embeddings: List[List[float]],
        metadatas: List[Dict[str, Any]]
    ):
        """Upsert chunks into the chunks collection."""
        collection = self.get_chunks_collection()

        # ChromaDB has batch size limits, process in batches
        batch_size = 5000
        for i in range(0, len(ids), batch_size):
            end = min(i + batch_size, len(ids))
            collection.upsert(
                ids=ids[i:end],
                documents=texts[i:end],
                embeddings=embeddings[i:end],
                metadatas=metadatas[i:end]
            )

    def delete_chunks(self, ids: List[str]):
        """Delete chunks by ID."""
        if not ids:
            return
        collection = self.get_chunks_collection()

        # Delete in batches
        batch_size = 5000
        for i in range(0, len(ids), batch_size):
            end = min(i + batch_size, len(ids))
            collection.delete(ids=ids[i:end])

    def search_chunks(
        self,
        query: str,
        limit: int = 10,
        where: Optional[Dict[str, Any]] = None
    ) -> List[ChunkResult]:
        """Search document chunks.

        Args:
            query: Search query text.
            limit: Maximum results to return.
            where: Optional metadata filter.

        Returns:
            List of ChunkResult objects.
        """
        collection = self.get_chunks_collection()
        query_embedding = embed_for_query(query)

        results = collection.query(
            query_embeddings=[query_embedding],
            n_results=limit,
            where=where,
            include=["documents", "metadatas", "distances"]
        )

        return self._parse_chunk_results(results)

    def _parse_chunk_results(self, results: Dict) -> List[ChunkResult]:
        """Parse ChromaDB query results into ChunkResult objects."""
        if not results["ids"] or not results["ids"][0]:
            return []

        parsed = []
        for i, id_ in enumerate(results["ids"][0]):
            distance = results["distances"][0][i] if results["distances"] else 0
            score = 1 - distance  # Cosine distance to similarity

            parsed.append(ChunkResult(
                id=id_,
                text=results["documents"][0][i] if results["documents"] else "",
                score=round(score, 3),
                metadata=results["metadatas"][0][i] if results["metadatas"] else {}
            ))

        return parsed

    def get_chunks_stats(self) -> Dict[str, Any]:
        """Get chunk index statistics."""
        collection = self.get_chunks_collection()
        count = collection.count()

        # Get unique files
        if count > 0:
            result = collection.get(include=["metadatas"])
            unique_files = len(set(
                m.get("source_file", "") for m in result["metadatas"]
            ))
        else:
            unique_files = 0

        return {
            "chunks_count": count,
            "files_count": unique_files,
            "chroma_path": self.path
        }

    def get_adjacent_chunks(
        self,
        source_file: str,
        chunk_index: int,
        before: int = 1,
        after: int = 1
    ) -> List[ChunkResult]:
        """Get chunks adjacent to the specified chunk.

        Args:
            source_file: The source filename.
            chunk_index: The chunk index to get context around.
            before: Number of chunks before to retrieve.
            after: Number of chunks after to retrieve.

        Returns:
            List of adjacent ChunkResult objects, sorted by chunk_index.
        """
        collection = self.get_chunks_collection()

        # Build chunk IDs to fetch
        safe_name = source_file.replace("/", "_").replace("\\", "_")
        chunk_ids = []
        for i in range(chunk_index - before, chunk_index + after + 1):
            if i >= 0 and i != chunk_index:  # Skip the original chunk
                chunk_ids.append(f"{safe_name}__c{i:04d}")

        if not chunk_ids:
            return []

        # Fetch by IDs
        result = collection.get(
            ids=chunk_ids,
            include=["documents", "metadatas"]
        )

        if not result["ids"]:
            return []

        # Parse results
        chunks = []
        for i, id_ in enumerate(result["ids"]):
            chunks.append(ChunkResult(
                id=id_,
                text=result["documents"][i] if result["documents"] else "",
                score=0,  # No score for adjacent chunks
                metadata=result["metadatas"][i] if result["metadatas"] else {}
            ))

        # Sort by chunk_index
        chunks.sort(key=lambda c: c.metadata.get("chunk_index", 0))
        return chunks

    def get_chunks_by_file(self, source_file: str) -> List[ChunkResult]:
        """Get all chunks for a specific source file.

        Args:
            source_file: The source filename to retrieve chunks for.

        Returns:
            List of ChunkResult objects, sorted by chunk_index.
        """
        collection = self.get_chunks_collection()

        # Query by source_file metadata
        result = collection.get(
            where={"source_file": source_file},
            include=["documents", "metadatas"]
        )

        if not result["ids"]:
            return []

        # Parse results
        chunks = []
        for i, id_ in enumerate(result["ids"]):
            chunks.append(ChunkResult(
                id=id_,
                text=result["documents"][i] if result["documents"] else "",
                score=0,
                metadata=result["metadatas"][i] if result["metadatas"] else {}
            ))

        # Sort by chunk_index
        chunks.sort(key=lambda c: c.metadata.get("chunk_index", 0))
        return chunks

    def update_chunk_metadata(self, chunk_id: str, metadata: Dict[str, Any]):
        """Update metadata for a single chunk without re-embedding.

        Args:
            chunk_id: The chunk ID to update.
            metadata: New metadata dict (will be merged with existing).
        """
        collection = self.get_chunks_collection()

        # Get existing chunk data
        result = collection.get(
            ids=[chunk_id],
            include=["documents", "embeddings", "metadatas"]
        )

        if not result["ids"]:
            return  # Chunk doesn't exist

        # Merge metadata (preserve existing, update with new)
        existing_metadata = result["metadatas"][0] if result["metadatas"] else {}
        merged_metadata = {**existing_metadata, **metadata}

        # Update (upsert with same ID)
        collection.upsert(
            ids=[chunk_id],
            documents=[result["documents"][0]] if result["documents"] else None,
            embeddings=[result["embeddings"][0]] if result["embeddings"] else None,
            metadatas=[merged_metadata]
        )

    def update_chunks_metadata_batch(self, updates: List[tuple[str, Dict[str, Any]]]):
        """Update metadata for multiple chunks in batch.

        Args:
            updates: List of (chunk_id, metadata_updates) tuples.
        """
        collection = self.get_chunks_collection()

        # Get all existing chunks
        chunk_ids = [chunk_id for chunk_id, _ in updates]
        result = collection.get(
            ids=chunk_ids,
            include=["documents", "embeddings", "metadatas"]
        )

        if not result["ids"]:
            return

        # Build update lists
        ids_to_update = []
        documents_to_update = []
        embeddings_to_update = []
        metadatas_to_update = []

        # Create lookup for updates
        updates_dict = {chunk_id: metadata for chunk_id, metadata in updates}

        for i, chunk_id in enumerate(result["ids"]):
            if chunk_id in updates_dict:
                # Merge metadata
                existing_metadata = result["metadatas"][i] if result["metadatas"] else {}
                merged_metadata = {**existing_metadata, **updates_dict[chunk_id]}

                ids_to_update.append(chunk_id)
                documents_to_update.append(result["documents"][i] if result.get("documents") else "")
                embeddings_to_update.append(result["embeddings"][i] if result.get("embeddings") is not None else [])
                metadatas_to_update.append(merged_metadata)

        # Batch upsert
        if ids_to_update:
            self.upsert_chunks(
                ids=ids_to_update,
                texts=documents_to_update,
                embeddings=embeddings_to_update,
                metadatas=metadatas_to_update
            )

    # --- Legacy operations (for backward compatibility) ---

    def get_documents_collection(self):
        """Get or create the documents collection (legacy)."""
        return self.client.get_or_create_collection(
            name=config.DOCUMENTS_COLLECTION,
            metadata={"hnsw:space": "cosine"}
        )

    def get_statements_collection(self):
        """Get or create the statements collection (legacy)."""
        return self.client.get_or_create_collection(
            name=config.STATEMENTS_COLLECTION,
            metadata={"hnsw:space": "cosine"}
        )

    def get_existing_hashes(self, collection_name: str) -> Dict[str, str]:
        """Get mapping of ID -> content_hash for change detection (legacy)."""
        if collection_name == config.DOCUMENTS_COLLECTION:
            collection = self.get_documents_collection()
        else:
            collection = self.get_statements_collection()

        result = collection.get(include=["metadatas"])
        if not result["ids"]:
            return {}

        return {
            id_: meta.get("content_hash", "")
            for id_, meta in zip(result["ids"], result["metadatas"])
        }


# Singleton instance
_store: Optional[EmbeddingStore] = None


def get_store() -> EmbeddingStore:
    """Get or create the singleton store."""
    global _store
    if _store is None:
        _store = EmbeddingStore()
    return _store


# --- New chunk-based search API ---

def search_chunks(
    query: str,
    source_type: Optional[str] = None,
    document_type: Optional[str] = None,
    author: Optional[str] = None,
    subfolder: Optional[str] = None,
    after: Optional[str] = None,
    before: Optional[str] = None,
    limit: int = config.DEFAULT_LIMIT
) -> List[ChunkResult]:
    """Search document chunks with optional filters.

    Args:
        query: Search query text.
        source_type: Filter by source (narratives, raba, psi).
        document_type: Filter by document type (schedule_narrative, meeting_notes, etc.).
        author: Filter by author (Yates, SECAI, etc.).
        subfolder: Filter by subfolder (weekly_reports, etc.).
        after: Filter files dated after this date (YYYY-MM-DD).
        before: Filter files dated before this date (YYYY-MM-DD).
        limit: Maximum results.

    Returns:
        List of ChunkResult objects.
    """
    where = _build_chunk_filter(source_type, document_type, author, subfolder, after, before)
    return get_store().search_chunks(query, limit=limit, where=where)


def _build_chunk_filter(
    source_type: Optional[str] = None,
    document_type: Optional[str] = None,
    author: Optional[str] = None,
    subfolder: Optional[str] = None,
    after: Optional[str] = None,
    before: Optional[str] = None
) -> Optional[Dict[str, Any]]:
    """Build ChromaDB where filter for chunks."""
    conditions = []

    if source_type:
        conditions.append({"source_type": {"$eq": source_type}})

    if document_type:
        conditions.append({"document_type": {"$eq": document_type}})

    if author:
        conditions.append({"author": {"$eq": author}})

    if subfolder:
        conditions.append({"subfolder": {"$contains": subfolder}})

    if after:
        conditions.append({"file_date": {"$gte": after}})

    if before:
        conditions.append({"file_date": {"$lte": before}})

    if not conditions:
        return None

    if len(conditions) == 1:
        return conditions[0]

    return {"$and": conditions}


# Legacy exports for backward compatibility
def search_statements(*args, **kwargs):
    """Legacy: Search statements. Redirects to search_chunks."""
    return search_chunks(*args, **kwargs)


def search_documents(*args, **kwargs):
    """Legacy: Search documents. Redirects to search_chunks."""
    return search_chunks(*args, **kwargs)
