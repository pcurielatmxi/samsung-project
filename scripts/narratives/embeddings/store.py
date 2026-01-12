"""ChromaDB storage operations for narrative embeddings."""

from dataclasses import dataclass
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
class StatementResult(SearchResult):
    """Statement search result with navigation context."""
    prev_statements: List["StatementResult"] = None
    next_statements: List["StatementResult"] = None

    def __post_init__(self):
        if self.prev_statements is None:
            self.prev_statements = []
        if self.next_statements is None:
            self.next_statements = []


class EmbeddingStore:
    """ChromaDB-based embedding storage."""

    def __init__(self, path: Optional[str] = None):
        """Initialize ChromaDB client.

        Args:
            path: Path to ChromaDB storage. Defaults to config.CHROMA_PATH.
        """
        self.path = path or str(config.CHROMA_PATH)

        # Ensure directory exists
        config.CHROMA_PATH.mkdir(parents=True, exist_ok=True)

        # Initialize persistent client
        self.client = chromadb.PersistentClient(
            path=self.path,
            settings=Settings(anonymized_telemetry=False)
        )

    def get_documents_collection(self):
        """Get or create the documents collection."""
        return self.client.get_or_create_collection(
            name=config.DOCUMENTS_COLLECTION,
            metadata={"hnsw:space": "cosine"}
        )

    def get_statements_collection(self):
        """Get or create the statements collection."""
        return self.client.get_or_create_collection(
            name=config.STATEMENTS_COLLECTION,
            metadata={"hnsw:space": "cosine"}
        )

    def get_existing_ids(self, collection_name: str) -> Set[str]:
        """Get all existing IDs in a collection."""
        if collection_name == config.DOCUMENTS_COLLECTION:
            collection = self.get_documents_collection()
        else:
            collection = self.get_statements_collection()

        result = collection.get()
        return set(result["ids"]) if result["ids"] else set()

    def get_existing_hashes(self, collection_name: str) -> Dict[str, str]:
        """Get mapping of ID -> content_hash for change detection."""
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

    def upsert_documents(
        self,
        ids: List[str],
        texts: List[str],
        embeddings: List[List[float]],
        metadatas: List[Dict[str, Any]]
    ):
        """Upsert documents into the documents collection."""
        collection = self.get_documents_collection()
        collection.upsert(
            ids=ids,
            documents=texts,
            embeddings=embeddings,
            metadatas=metadatas
        )

    def upsert_statements(
        self,
        ids: List[str],
        texts: List[str],
        embeddings: List[List[float]],
        metadatas: List[Dict[str, Any]]
    ):
        """Upsert statements into the statements collection."""
        collection = self.get_statements_collection()
        collection.upsert(
            ids=ids,
            documents=texts,
            embeddings=embeddings,
            metadatas=metadatas
        )

    def delete_documents(self, ids: List[str]):
        """Delete documents by ID."""
        if not ids:
            return
        collection = self.get_documents_collection()
        collection.delete(ids=list(ids))

    def delete_statements(self, ids: List[str]):
        """Delete statements by ID."""
        if not ids:
            return
        collection = self.get_statements_collection()
        collection.delete(ids=list(ids))

    def search_documents(
        self,
        query: str,
        limit: int = 10,
        where: Optional[Dict[str, Any]] = None
    ) -> List[SearchResult]:
        """Search document summaries.

        Args:
            query: Search query text.
            limit: Maximum results to return.
            where: Optional metadata filter.

        Returns:
            List of SearchResult objects.
        """
        collection = self.get_documents_collection()
        query_embedding = embed_for_query(query)

        results = collection.query(
            query_embeddings=[query_embedding],
            n_results=limit,
            where=where,
            include=["documents", "metadatas", "distances"]
        )

        return self._parse_results(results)

    def search_statements(
        self,
        query: str,
        limit: int = 10,
        where: Optional[Dict[str, Any]] = None,
        context: int = 0
    ) -> List[StatementResult]:
        """Search statements with optional context.

        Args:
            query: Search query text.
            limit: Maximum results to return.
            where: Optional metadata filter.
            context: Number of prev/next statements to include.

        Returns:
            List of StatementResult objects with context.
        """
        collection = self.get_statements_collection()
        query_embedding = embed_for_query(query)

        results = collection.query(
            query_embeddings=[query_embedding],
            n_results=limit,
            where=where,
            include=["documents", "metadatas", "distances"]
        )

        parsed = self._parse_statement_results(results)

        # Add context if requested
        if context > 0:
            for result in parsed:
                result.prev_statements, result.next_statements = \
                    self._get_statement_context(result, context)

        return parsed

    def _parse_results(self, results: Dict) -> List[SearchResult]:
        """Parse ChromaDB query results into SearchResult objects."""
        if not results["ids"] or not results["ids"][0]:
            return []

        parsed = []
        for i, id_ in enumerate(results["ids"][0]):
            # Convert distance to similarity score (cosine distance -> similarity)
            distance = results["distances"][0][i] if results["distances"] else 0
            score = 1 - distance  # Cosine distance to similarity

            parsed.append(SearchResult(
                id=id_,
                text=results["documents"][0][i] if results["documents"] else "",
                score=round(score, 3),
                metadata=results["metadatas"][0][i] if results["metadatas"] else {}
            ))

        return parsed

    def _parse_statement_results(self, results: Dict) -> List[StatementResult]:
        """Parse ChromaDB query results into StatementResult objects."""
        if not results["ids"] or not results["ids"][0]:
            return []

        parsed = []
        for i, id_ in enumerate(results["ids"][0]):
            distance = results["distances"][0][i] if results["distances"] else 0
            score = 1 - distance

            parsed.append(StatementResult(
                id=id_,
                text=results["documents"][0][i] if results["documents"] else "",
                score=round(score, 3),
                metadata=results["metadatas"][0][i] if results["metadatas"] else {}
            ))

        return parsed

    def _get_statement_context(
        self,
        result: StatementResult,
        n: int
    ) -> tuple[List[StatementResult], List[StatementResult]]:
        """Get N statements before and after the given statement."""
        collection = self.get_statements_collection()
        file_id = result.metadata.get("narrative_file_id", "")
        index = result.metadata.get("statement_index", 0)

        if not file_id:
            return [], []

        prev_statements = []
        next_statements = []

        # Get previous statements
        for i in range(index - n, index):
            if i < 0:
                continue
            prev_result = collection.get(
                where={
                    "$and": [
                        {"narrative_file_id": {"$eq": file_id}},
                        {"statement_index": {"$eq": i}}
                    ]
                },
                include=["documents", "metadatas"]
            )
            if prev_result["ids"]:
                prev_statements.append(StatementResult(
                    id=prev_result["ids"][0],
                    text=prev_result["documents"][0] if prev_result["documents"] else "",
                    score=0,
                    metadata=prev_result["metadatas"][0] if prev_result["metadatas"] else {}
                ))

        # Get next statements
        for i in range(index + 1, index + n + 1):
            next_result = collection.get(
                where={
                    "$and": [
                        {"narrative_file_id": {"$eq": file_id}},
                        {"statement_index": {"$eq": i}}
                    ]
                },
                include=["documents", "metadatas"]
            )
            if next_result["ids"]:
                next_statements.append(StatementResult(
                    id=next_result["ids"][0],
                    text=next_result["documents"][0] if next_result["documents"] else "",
                    score=0,
                    metadata=next_result["metadatas"][0] if next_result["metadatas"] else {}
                ))

        return prev_statements, next_statements

    def get_stats(self) -> Dict[str, Any]:
        """Get index statistics."""
        docs_collection = self.get_documents_collection()
        stmts_collection = self.get_statements_collection()

        return {
            "documents_count": docs_collection.count(),
            "statements_count": stmts_collection.count(),
            "chroma_path": self.path
        }


# Singleton instance
_store: Optional[EmbeddingStore] = None


def get_store() -> EmbeddingStore:
    """Get or create the singleton store."""
    global _store
    if _store is None:
        _store = EmbeddingStore()
    return _store


# Convenience functions for API
def search_statements(
    query: str,
    category: Optional[str] = None,
    party: Optional[str] = None,
    location: Optional[str] = None,
    after: Optional[str] = None,
    before: Optional[str] = None,
    limit: int = config.DEFAULT_LIMIT,
    context: int = config.DEFAULT_CONTEXT
) -> List[StatementResult]:
    """Search statements with optional filters.

    Args:
        query: Search query text.
        category: Filter by statement category.
        party: Filter by party (substring match in parties field).
        location: Filter by location (substring match).
        after: Filter events after this date (YYYY-MM-DD).
        before: Filter events before this date (YYYY-MM-DD).
        limit: Maximum results.
        context: Number of surrounding statements to include.

    Returns:
        List of StatementResult objects.
    """
    where = _build_where_filter(category, party, location, after, before)
    return get_store().search_statements(query, limit=limit, where=where, context=context)


def search_documents(
    query: str,
    doc_type: Optional[str] = None,
    after: Optional[str] = None,
    before: Optional[str] = None,
    limit: int = config.DEFAULT_LIMIT
) -> List[SearchResult]:
    """Search document summaries with optional filters.

    Args:
        query: Search query text.
        doc_type: Filter by document type.
        after: Filter documents after this date.
        before: Filter documents before this date.
        limit: Maximum results.

    Returns:
        List of SearchResult objects.
    """
    where = _build_doc_where_filter(doc_type, after, before)
    return get_store().search_documents(query, limit=limit, where=where)


def _build_where_filter(
    category: Optional[str] = None,
    party: Optional[str] = None,
    location: Optional[str] = None,
    after: Optional[str] = None,
    before: Optional[str] = None
) -> Optional[Dict[str, Any]]:
    """Build ChromaDB where filter for statements."""
    conditions = []

    if category:
        conditions.append({"category": {"$eq": category}})

    if party:
        conditions.append({"parties": {"$contains": party}})

    if location:
        conditions.append({"locations": {"$contains": location}})

    if after:
        conditions.append({"event_date": {"$gte": after}})

    if before:
        conditions.append({"event_date": {"$lte": before}})

    if not conditions:
        return None

    if len(conditions) == 1:
        return conditions[0]

    return {"$and": conditions}


def _build_doc_where_filter(
    doc_type: Optional[str] = None,
    after: Optional[str] = None,
    before: Optional[str] = None
) -> Optional[Dict[str, Any]]:
    """Build ChromaDB where filter for documents."""
    conditions = []

    if doc_type:
        conditions.append({"type": {"$eq": doc_type}})

    if after:
        conditions.append({"date": {"$gte": after}})

    if before:
        conditions.append({"date": {"$lte": before}})

    if not conditions:
        return None

    if len(conditions) == 1:
        return conditions[0]

    return {"$and": conditions}
