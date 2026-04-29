# Cricket AI Expert

Local-first cricket expert chatbot built on top of CricSheet JSON data with a Retrieval-Augmented Generation pipeline.

## What is included

- external CricSheet dataset support through environment config
- ingestion CLI with `update` and `rebuild`
- SQLite registry for tracked files, documents, and chunks
- Chroma-backed vector indexing with a keyword-search fallback
- Ollama-powered answer generation
- Streamlit chat UI
- FastAPI health and query endpoints

## Quick start

1. Create a virtual environment and install dependencies.
2. Copy `.env.example` to `.env` and confirm `CRICSHEET_DATA_DIR` points to your external CricSheet folder.
3. Start Ollama locally and pull a small model such as `llama3.2:3b`.
4. Run an initial ingest:

```bash
python -m app.ingest rebuild
```

5. Start the Streamlit app:

```bash
streamlit run app/ui/streamlit_app.py
```

Optional API:

```bash
uvicorn app.api.main:app --reload
```

## Project layout

```text
app/
  api/
  ingest/
  rag/
  settings/
  ui/
storage/        # created locally and ignored by git
tests/
```

## MVP notes

- The parser is defensive about CricSheet JSON variations, but the dataset may still surface edge cases we will tighten iteratively.
- Retrieval currently combines vector search and a lightweight keyword scorer.
- Aggregate cross-match analytics are not implemented yet; this scaffold is optimized for match lookup and narrative cricket questions first.

