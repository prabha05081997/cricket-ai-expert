from __future__ import annotations

from app.domain import Chunk, Document


def chunk_document(document: Document, chunk_size: int = 850, overlap: int = 120) -> list[Chunk]:
    text = document.content.strip()
    if not text:
        return []

    chunks: list[Chunk] = []
    start = 0
    chunk_index = 0
    while start < len(text):
        end = min(len(text), start + chunk_size)
        window = text[start:end]
        if end < len(text):
            split_at = window.rfind("\n")
            if split_at > chunk_size // 3:
                window = window[:split_at]
                end = start + split_at

        metadata = dict(document.metadata)
        metadata.update(
            {
                "doc_id": document.doc_id,
                "document_type": document.document_type,
                "title": document.title,
                "chunk_index": chunk_index,
            }
        )
        chunks.append(
            Chunk(
                chunk_id=f"{document.doc_id}:chunk:{chunk_index}",
                doc_id=document.doc_id,
                match_id=document.match_id,
                text=window.strip(),
                chunk_index=chunk_index,
                metadata=metadata,
            )
        )
        chunk_index += 1
        if end >= len(text):
            break
        start = max(0, end - overlap)
    return chunks
