"""Build and update the narrative embeddings index."""

import csv
import hashlib
from dataclasses import dataclass
from pathlib import Path
from typing import List, Dict, Any, Optional, Set
from datetime import datetime

from . import config
from .client import embed_for_index
from .store import get_store, EmbeddingStore


@dataclass
class Document:
    """Document record from dim_narrative_file.csv."""
    narrative_file_id: str
    relative_path: str
    filename: str
    document_type: str
    document_title: str
    document_date: str
    data_date: str
    author: str
    summary: str
    statement_count: int
    locate_rate: float
    file_extension: str

    @property
    def content_hash(self) -> str:
        """MD5 hash of summary for change detection."""
        return hashlib.md5(self.summary.encode()).hexdigest()


@dataclass
class Statement:
    """Statement record from narrative_statements.csv."""
    statement_id: str
    narrative_file_id: str
    statement_index: int
    text: str
    category: str
    event_date: str
    parties: str
    locations: str
    impact_days: Optional[int]
    impact_description: str
    references: str
    source_page: Optional[int]
    source_char_offset: Optional[int]
    match_confidence: float
    match_type: str
    is_located: bool

    @property
    def content_hash(self) -> str:
        """MD5 hash of text for change detection."""
        return hashlib.md5(self.text.encode()).hexdigest()

    @property
    def embed_text(self) -> str:
        """Text to embed (with category prefix)."""
        return f"[{self.category}] {self.text}"


def load_documents(path: Optional[Path] = None) -> List[Document]:
    """Load documents from dim_narrative_file.csv."""
    path = path or config.DIM_FILE
    if not path.exists():
        raise FileNotFoundError(f"Document file not found: {path}")

    documents = []
    with open(path, "r", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            documents.append(Document(
                narrative_file_id=row["narrative_file_id"],
                relative_path=row["relative_path"],
                filename=row["filename"],
                document_type=row["document_type"],
                document_title=row["document_title"],
                document_date=row["document_date"],
                data_date=row["data_date"],
                author=row["author"],
                summary=row["summary"],
                statement_count=int(row["statement_count"]) if row["statement_count"] else 0,
                locate_rate=float(row["locate_rate"]) if row["locate_rate"] else 0.0,
                file_extension=row["file_extension"]
            ))

    return documents


def load_statements(path: Optional[Path] = None) -> List[Statement]:
    """Load statements from narrative_statements.csv."""
    path = path or config.STMT_FILE
    if not path.exists():
        raise FileNotFoundError(f"Statements file not found: {path}")

    statements = []
    with open(path, "r", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            statements.append(Statement(
                statement_id=row["statement_id"],
                narrative_file_id=row["narrative_file_id"],
                statement_index=int(row["statement_index"]) if row["statement_index"] else 0,
                text=row["text"],
                category=row["category"],
                event_date=row["event_date"],
                parties=row["parties"],
                locations=row["locations"],
                impact_days=int(row["impact_days"]) if row["impact_days"] else None,
                impact_description=row["impact_description"],
                references=row["references"],
                source_page=int(row["source_page"]) if row["source_page"] else None,
                source_char_offset=int(row["source_char_offset"]) if row["source_char_offset"] else None,
                match_confidence=float(row["match_confidence"]) if row["match_confidence"] else 0.0,
                match_type=row["match_type"],
                is_located=row["is_located"].lower() == "true" if row["is_located"] else False
            ))

    return statements


def build_document_metadata(doc: Document) -> Dict[str, Any]:
    """Build metadata dict for a document."""
    return {
        "title": doc.document_title,
        "type": doc.document_type,
        "date": doc.document_date,
        "data_date": doc.data_date,
        "author": doc.author,
        "path": doc.relative_path,
        "statement_count": doc.statement_count,
        "content_hash": doc.content_hash
    }


def build_statement_metadata(stmt: Statement) -> Dict[str, Any]:
    """Build metadata dict for a statement."""
    return {
        "narrative_file_id": stmt.narrative_file_id,
        "statement_index": stmt.statement_index,
        "category": stmt.category,
        "event_date": stmt.event_date or "",
        "parties": stmt.parties,
        "locations": stmt.locations,
        "impact_days": stmt.impact_days if stmt.impact_days is not None else -1,
        "source_page": stmt.source_page if stmt.source_page is not None else -1,
        "content_hash": stmt.content_hash
    }


@dataclass
class BuildResult:
    """Result of a build operation."""
    documents_added: int = 0
    documents_updated: int = 0
    documents_deleted: int = 0
    documents_unchanged: int = 0
    statements_added: int = 0
    statements_updated: int = 0
    statements_deleted: int = 0
    statements_unchanged: int = 0
    errors: List[str] = None

    def __post_init__(self):
        if self.errors is None:
            self.errors = []

    @property
    def total_documents(self) -> int:
        return self.documents_added + self.documents_updated + self.documents_unchanged

    @property
    def total_statements(self) -> int:
        return self.statements_added + self.statements_updated + self.statements_unchanged


def build_index(force: bool = False, verbose: bool = True) -> BuildResult:
    """Build or update the embeddings index.

    Args:
        force: If True, rebuild all embeddings ignoring cache.
        verbose: If True, print progress messages.

    Returns:
        BuildResult with counts of operations performed.
    """
    result = BuildResult()
    store = get_store()

    if verbose:
        print("=" * 60)
        print("Narrative Embeddings Builder")
        print("=" * 60)
        print(f"Force rebuild: {force}")
        print(f"ChromaDB path: {config.CHROMA_PATH}")
        print()

    # Load source data
    try:
        if verbose:
            print("Loading source data...")
        documents = load_documents()
        statements = load_statements()
        if verbose:
            print(f"  Documents: {len(documents)}")
            print(f"  Statements: {len(statements)}")
    except FileNotFoundError as e:
        result.errors.append(str(e))
        return result

    # Get existing data from ChromaDB
    existing_doc_hashes = store.get_existing_hashes(config.DOCUMENTS_COLLECTION)
    existing_stmt_hashes = store.get_existing_hashes(config.STATEMENTS_COLLECTION)

    if verbose:
        print(f"\nExisting in index:")
        print(f"  Documents: {len(existing_doc_hashes)}")
        print(f"  Statements: {len(existing_stmt_hashes)}")

    # Compute what needs updating
    csv_doc_ids = {d.narrative_file_id for d in documents}
    csv_stmt_ids = {s.statement_id for s in statements}

    # Find stale entries to delete
    stale_docs = set(existing_doc_hashes.keys()) - csv_doc_ids
    stale_stmts = set(existing_stmt_hashes.keys()) - csv_stmt_ids

    if stale_docs:
        if verbose:
            print(f"\nDeleting {len(stale_docs)} stale documents...")
        store.delete_documents(list(stale_docs))
        result.documents_deleted = len(stale_docs)

    if stale_stmts:
        if verbose:
            print(f"Deleting {len(stale_stmts)} stale statements...")
        store.delete_statements(list(stale_stmts))
        result.statements_deleted = len(stale_stmts)

    # Process documents
    if verbose:
        print("\nProcessing documents...")

    docs_to_embed = []
    for doc in documents:
        existing_hash = existing_doc_hashes.get(doc.narrative_file_id)

        if force or existing_hash is None:
            docs_to_embed.append(doc)
        elif existing_hash != doc.content_hash:
            docs_to_embed.append(doc)
            result.documents_updated += 1
        else:
            result.documents_unchanged += 1

    if docs_to_embed:
        if verbose:
            print(f"  Embedding {len(docs_to_embed)} documents...")

        # Generate embeddings
        texts = [d.summary for d in docs_to_embed]
        embeddings = embed_for_index(texts)

        # Upsert to ChromaDB
        store.upsert_documents(
            ids=[d.narrative_file_id for d in docs_to_embed],
            texts=texts,
            embeddings=embeddings,
            metadatas=[build_document_metadata(d) for d in docs_to_embed]
        )

        result.documents_added = len(docs_to_embed) - result.documents_updated
    else:
        if verbose:
            print("  No documents to update")

    # Process statements
    if verbose:
        print("\nProcessing statements...")

    stmts_to_embed = []
    for stmt in statements:
        existing_hash = existing_stmt_hashes.get(stmt.statement_id)

        if force or existing_hash is None:
            stmts_to_embed.append(stmt)
        elif existing_hash != stmt.content_hash:
            stmts_to_embed.append(stmt)
            result.statements_updated += 1
        else:
            result.statements_unchanged += 1

    if stmts_to_embed:
        if verbose:
            print(f"  Embedding {len(stmts_to_embed)} statements...")

        # Generate embeddings (with category prefix)
        texts = [s.embed_text for s in stmts_to_embed]
        embeddings = embed_for_index(texts)

        # Upsert to ChromaDB (store original text, not embed_text)
        store.upsert_statements(
            ids=[s.statement_id for s in stmts_to_embed],
            texts=[s.text for s in stmts_to_embed],
            embeddings=embeddings,
            metadatas=[build_statement_metadata(s) for s in stmts_to_embed]
        )

        result.statements_added = len(stmts_to_embed) - result.statements_updated
    else:
        if verbose:
            print("  No statements to update")

    if verbose:
        print("\n" + "=" * 60)
        print("Build Summary")
        print("=" * 60)
        print(f"Documents: {result.total_documents} total")
        print(f"  Added: {result.documents_added}")
        print(f"  Updated: {result.documents_updated}")
        print(f"  Unchanged: {result.documents_unchanged}")
        print(f"  Deleted: {result.documents_deleted}")
        print(f"\nStatements: {result.total_statements} total")
        print(f"  Added: {result.statements_added}")
        print(f"  Updated: {result.statements_updated}")
        print(f"  Unchanged: {result.statements_unchanged}")
        print(f"  Deleted: {result.statements_deleted}")

        if result.errors:
            print(f"\nErrors: {len(result.errors)}")
            for err in result.errors:
                print(f"  - {err}")

        print("=" * 60)

    return result
