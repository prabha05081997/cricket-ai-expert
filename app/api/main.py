from __future__ import annotations

from fastapi import FastAPI
from pydantic import BaseModel

from app.rag.index import LocalIndex
from app.rag.llm import OllamaClient
from app.rag.service import ChatService
from app.settings import get_settings

app = FastAPI(title="Cricket AI Expert API", version="0.1.0")


class QueryRequest(BaseModel):
    question: str
    top_k: int = 6


def _chat_service() -> ChatService:
    settings = get_settings()
    index = LocalIndex(
        registry_db_path=settings.registry_db_path,
        chroma_dir=settings.chroma_dir,
        collection_name=settings.chroma_collection,
        embedding_model_name=settings.embedding_model,
    )
    llm = OllamaClient(settings.ollama_base_url, settings.ollama_model)
    return ChatService(index=index, llm_client=llm)


@app.get("/health")
def health() -> dict[str, str]:
    return {"status": "ok"}


@app.post("/query")
def query(request: QueryRequest) -> dict[str, object]:
    return _chat_service().answer(request.question, top_k=request.top_k)

