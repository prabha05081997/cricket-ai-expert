# Project Plan: Free, Maintainable Cricket Expert Chatbot with RAG

## 1. Project Goal

Build a chat application that behaves like a cricket expert by answering questions from a **local Retrieval-Augmented Generation (RAG)** system built on top of CricSheet match data.

The system should:

- ingest CricSheet JSON match files from a folder **outside the Git repository**
- build a searchable knowledge base from those files
- answer user questions through a chat UI
- support **incremental updates** when new CricSheet data is added later
- stay **free of cost** to build, run, and maintain

This is not "training" an LLM in the fine-tuning sense. The practical and free approach is:

- keep the LLM unchanged
- preprocess cricket data into structured/searchable documents
- retrieve relevant context at question time
- prompt the model with that retrieved context

That is the RAG system we should build.

---

## 2. Confirmed Constraint: Dataset Must Stay Outside the Repo

The CricSheet dataset is stored at:

`/Users/manojprabhakarm/Documents/work/personal/cricket-data/all_json`

This is the correct setup because:

- the dataset is large (`~3.4 GB`)
- it should not be committed to Git
- it will change over time as new data is downloaded

The application must therefore use a configurable external data path, for example through:

- `.env`
- environment variables
- a small local config file ignored by Git

Recommended variable:

`CRICSHEET_DATA_DIR=/Users/manojprabhakarm/Documents/work/personal/cricket-data/all_json`

The repo should only contain:

- application code
- configuration templates
- ingestion scripts
- small local metadata/state files
- documentation

It should not contain:

- raw CricSheet JSON files
- downloaded models
- vector database artifacts if they become large

---

## 3. Recommended Free Stack

To keep this practical, maintainable, and free, the recommended stack is:

### Backend

- **Python**
- **FastAPI** for the API
- **Uvicorn** for local serving

### RAG / Retrieval

- **Custom CricSheet parser** for structured extraction
- **Sentence Transformers** for embeddings
- **ChromaDB** for local vector storage

### LLM Inference

- **Ollama** running a local model
- Start with a small free model such as:
  - `phi3:mini`
  - `llama3.2:3b`
  - another lightweight instruct model supported by Ollama

### Frontend

- **Streamlit** for fastest delivery, simplest deployment, and least maintenance

Alternative:

- React + FastAPI if we later want a more polished product UI

For now, Streamlit is the best fit because you said you are new to building this and want the best free, maintainable path.
It also keeps the architecture simple while still allowing us to separate the app logic cleanly so a future shared web frontend can be added later.

### Scheduling / Updates

- manual command for updates at first
- optional monthly automation later using:
  - `cron` on macOS/Linux
  - Task Scheduler on Windows

---

## 4. Why This Architecture Fits the Requirements

This architecture is chosen because it is:

- **free**: no paid APIs, vector DBs, or managed hosting required
- **deployable locally**: works on your own machine
- **maintainable**: clear separation between ingestion, retrieval, and chat
- **safe for large data**: dataset remains outside Git
- **incremental**: new matches can be indexed without rebuilding everything

Important practical note:

- running the embedding pipeline is realistic on CPU
- running the chat LLM is also realistic locally with Ollama
- hosting this fully online for free is much harder if the LLM must also run for free

So the primary target should be:

- **local-first deployment**

Then later we can evaluate:

- local backend + local model + local UI
- or split deployment options if you want others to use it remotely

Confirmed direction for version 1:

- local-only usage on your machine
- architecture should remain easy to extend later for multi-user or remote access

---

## 5. Functional Requirements

The system should support:

1. User opens a chat interface.
2. User asks a cricket question in natural language.
3. System retrieves relevant match/player/stat documents from the CricSheet-based knowledge base.
4. System sends retrieved context to the LLM.
5. LLM answers clearly and, ideally, cites supporting matches/chunks.
6. Admin/user can run an update command later to add newly downloaded CricSheet files.

Examples of supported questions:

- "Who won the 2011 World Cup final?"
- "How did Virat Kohli perform in ODI chases in 2019?"
- "Which bowler took the most wickets in matches at Lord's?"
- "What happened in the final overs of the match between India and Pakistan on a given date?"

---

## 6. Non-Functional Requirements

- Must be **free of cost**
- Must be **reasonably fast on consumer hardware**
- Must be **easy to rerun and update**
- Must be **robust to CricSheet JSON variations**
- Must be **easy to deploy locally**
- Must avoid storing large generated artifacts in Git

---

## 7. Data Source and Ingestion Strategy

### 7.1 Data Source

Source: CricSheet JSON files from:

`/Users/manojprabhakarm/Documents/work/personal/cricket-data/all_json`

Expected characteristics:

- thousands of JSON files
- each file represents one match
- metadata plus innings/ball-by-ball detail
- structure may vary slightly across formats and years

### 7.2 External Data Policy

The ingestion pipeline must read from the external dataset path and must not assume the data is inside the repo.

We should add:

- `.gitignore` entries for local artifacts
- `.env.example` with the dataset path variable name only
- startup validation that fails with a friendly message if the directory does not exist

### 7.3 File Tracking

To support updates efficiently, maintain an ingestion registry in SQLite with fields like:

- `match_id`
- `source_file_path`
- `file_hash`
- `last_indexed_at`
- `status`

This allows us to:

- skip unchanged files
- detect new files
- re-index only changed files

---

## 8. Parsing and Knowledge Construction

Raw CricSheet JSON is rich but not ideal for direct retrieval. We should convert each match into structured intermediate data first, then produce retrieval-friendly documents.

### 8.1 Structured Extraction Per Match

Extract:

- match ID
- date
- teams
- gender
- match type
- event / series / tournament
- venue
- city
- toss result
- outcome
- player of the match
- innings totals
- batting scorecards
- bowling figures
- wickets timeline
- notable partnerships
- notable events if derivable

### 8.2 Documents to Generate

Each match should generate multiple document types instead of one huge blob:

1. **Match summary document**
2. **Innings summary document** for each innings
3. **Player performance document** for each significant player in the match
4. **Event timeline document** for notable moments
5. Optional later: **ball-over summaries** instead of full ball-by-ball chunks

This is important because pure ball-by-ball ingestion for all matches may create too much noisy context and unnecessarily large storage.

### 8.3 Recommended Retrieval Granularity

For version 1:

- prioritize match summaries
- innings summaries
- player performance summaries

Avoid storing every ball as a standalone chunk initially. That would bloat the index and reduce retrieval quality for general questions.

If needed later, we can selectively support:

- over-level summaries
- death overs summaries
- wicket-event summaries

---

## 9. Chunking Strategy

Use chunking only after generating clean cricket-focused text documents.

Recommended strategy:

- chunk size around `600-900` characters or equivalent token-aware size
- overlap around `80-120` characters when needed
- preserve document boundaries
- include rich metadata on every chunk

Metadata per chunk should include:

- `match_id`
- `date`
- `teams`
- `players`
- `venue`
- `match_type`
- `event_name`
- `document_type`
- `source_file`

This metadata is critical for both filtering and source display in the UI.

---

## 10. Embedding and Vector Storage

### 10.1 Embedding Model

Recommended first choice:

- `sentence-transformers/all-MiniLM-L6-v2`

Reasons:

- free
- lightweight
- works on CPU
- simple to deploy

Possible upgrade later:

- `BAAI/bge-small-en-v1.5`

### 10.2 Vector Database

Recommended:

- **ChromaDB**

Reasons:

- free and local
- simple Python integration
- supports metadata storage
- good enough for this scale

Storage location should be outside Git, for example:

- `./storage/chroma` if you are okay with local generated artifacts in the repo folder but ignored
- or a separate local data directory such as `~/Library/Application Support/cricket-llm/`

For simplicity in development:

- use a repo-local `storage/` directory
- add it to `.gitignore`

---

## 11. Retrieval Pipeline

### 11.1 Base Retrieval Flow

1. User enters a question.
2. System normalizes the question.
3. System embeds the question.
4. Vector DB retrieves top candidate chunks.
5. Optional metadata filtering or reranking is applied.
6. Retrieved chunks are passed to the LLM.
7. Final answer is generated with cited sources.

### 11.2 Important Improvement: Hybrid Retrieval

Pure vector search may miss exact stat-style questions. A stronger free approach is:

- semantic search via embeddings
- keyword search via SQLite FTS or simple BM25
- merge results

This is strongly recommended because cricket questions often include:

- exact player names
- exact teams
- exact venues
- exact tournaments
- exact years/dates

So the ideal version 1.5 architecture is:

- **vector retrieval + metadata filtering + lightweight keyword retrieval**

### 11.3 Guardrails

The model should be instructed:

- answer only from provided context
- say when the answer is not supported by retrieved evidence
- avoid inventing statistics
- mention source match/date when relevant

---

## 12. LLM Strategy

Use a local Ollama-served model for answer generation.

Recommended starting models:

- `phi3:mini`
- `llama3.2:3b`

Selection criteria:

- acceptable CPU performance
- modest RAM usage
- decent instruction following

Given your machine has **18 GB RAM**, this is a comfortable fit for a local-first setup.
We should still start with a small reliable model for responsiveness, then evaluate a slightly stronger model later if needed.

Prompting should include:

- system role: cricket expert assistant grounded in retrieved context
- strict instruction not to hallucinate unsupported facts
- concise answer formatting
- source-aware answer style

Example answer style:

- direct answer first
- short explanation second
- source section last

---

## 13. Chat Interface

### 13.1 Recommended Version 1

Use **Streamlit** for the first release.

UI should include:

- chat history
- text input
- clear chat button
- expandable "Sources used" section
- optional debug view for retrieved chunks

### 13.2 Why Streamlit First

- lowest implementation complexity
- easy local deployment
- easy to demo
- fewer moving parts than React

Confirmed recommendation:

- version 1 should use **Streamlit**
- the backend logic should be written in reusable Python modules so a future shared UI or API can be added without rewriting the RAG pipeline

If the product later needs:

- authentication
- multi-user access
- richer UX

then we can move to:

- React frontend
- FastAPI backend

---

## 14. Update Strategy for New Data

The RAG system must support periodic updates without rebuilding everything from scratch.

### 14.1 Update Command

Provide a command such as:

`python -m app.ingest update`

It should:

- scan the external CricSheet folder
- detect new or changed JSON files
- parse only those files
- generate documents and chunks
- embed and append them to the vector store
- update the ingestion registry

### 14.2 Rebuild Command

Also provide a full rebuild command:

`python -m app.ingest rebuild`

This is useful if:

- schema changes
- embedding model changes
- vector DB corruption
- document generation strategy changes

### 14.3 Scheduling

For maintainability, start with a manual update flow.

Later, add optional monthly automation through OS schedulers if needed.

### 14.4 Recommended Update Path

Recommended for version 1:

- **manual updates**

Reason:

- simpler to build
- easier to debug
- safer while the ingestion pipeline is still evolving

Planned later version:

- **automated monthly updates**

This should be added only after the ingestion and indexing flow is stable.

---

## 15. Proposed Repository Structure

```text
cricket-llm/
  app/
    api/
    chat/
    ingest/
    rag/
    settings/
    ui/
  storage/
  tests/
  .env.example
  .gitignore
  README.md
  plan.md
```

Suggested module responsibilities:

- `app/settings/`: configuration loading
- `app/ingest/`: file discovery, parsing, document generation, indexing
- `app/rag/`: retrieval, prompting, LLM orchestration
- `app/ui/`: Streamlit app
- `app/api/`: optional FastAPI endpoints if needed
- `storage/`: ignored local artifacts like SQLite registry and Chroma data

---

## 16. Deployment Strategy

### 16.1 Primary Deployment Target

Local deployment on your own machine.

This is the most realistic fully free option because:

- the data is large
- the vector store can be large
- the LLM must run somewhere
- free hosted environments usually have low disk/RAM limits

Design constraint for future expansion:

- keep retrieval and generation logic separate from the UI
- make configuration environment-driven
- ensure the app can later expose API endpoints for remote/shared access

### 16.2 Local Run Model

Expected local services:

- Ollama running locally
- Streamlit app running locally
- Chroma persisted locally

### 16.3 Optional Future Remote Access

If you later want remote users, we should revisit the design because "fully free" remote hosting with local LLM inference is usually not sustainable at production quality.

---

## 17. Risks and Practical Limits

### 17.1 Large-Scale Ball-by-Ball Indexing

If every delivery from every match is embedded, storage and retrieval noise may become a problem.

Mitigation:

- start with summaries
- add deeper event indexing only where necessary

### 17.2 Local LLM Accuracy

Small local models may sometimes answer less cleanly than premium hosted models.

Mitigation:

- strong retrieval
- structured documents
- strict prompt grounding
- source display in UI

### 17.3 Stat Questions May Need More Than RAG

Some questions are better answered by computed statistics than by retrieval alone, such as:

- "Who scored the most ODI runs in 2019?"
- "Which bowler has the best economy at this venue?"

RAG alone is not the best engine for these.

Best long-term design:

- **RAG for narrative / context-heavy questions**
- **structured analytics layer** for aggregate statistical queries

This is an important architecture point. If we ignore it, some cricket questions will be weaker than they should be.

Confirmed product recommendation:

- version 1 should prioritize:
  - player/match lookup questions
  - narrative/match explanation questions
- later versions should add:
  - aggregate statistics across many matches
  - query routing between RAG and structured analytics

### 17.4 Recommended Design Extension

Alongside the vector DB, we should strongly consider building a small SQLite analytics database with normalized tables such as:

- matches
- innings
- batting_performances
- bowling_performances
- players

Then the app can later support:

- retrieval-based answers
- SQL/stat-based answers
- or a hybrid approach

This would make the system much more powerful for cricket expertise.

---

## 18. Recommended Build Phases

### Phase 1: Foundation

- create project structure
- add config management
- wire external dataset path
- set up `.gitignore`
- define storage paths

### Phase 2: Ingestion MVP

- parse CricSheet JSON safely
- generate match and player summary documents
- store ingestion registry in SQLite

### Phase 3: RAG MVP

- chunk documents
- embed documents
- build Chroma index
- implement retrieval pipeline

### Phase 4: Chat MVP

- connect retrieval to Ollama
- build Streamlit chat interface
- show sources

### Phase 5: Updates and Hardening

- incremental update command
- rebuild command
- logging
- tests
- better answer formatting
- manual monthly update workflow
- documentation for update usage

### Phase 6: Smart Cricket Q&A

- add structured stats database
- route aggregate/stat questions to analytics
- keep RAG for descriptive questions
- prepare API layer for future shared access

---

## 19. What I Would Recommend Building First

To give you the best chance of success, I recommend we build:

1. local-only MVP
2. external dataset path support
3. Chroma-based RAG over generated cricket summaries
4. Ollama-based answer generation
5. Streamlit chat UI
6. incremental update command

Then, after the MVP works:

7. add SQLite-based structured statistics for stronger cricket answers
8. add API and deployment support for shared access

This gives you something usable sooner without overengineering the first version.

---

## 20. Clarifications Still Needed Before Implementation

These were the key decisions needed before implementation:

1. **Local-only vs shared use**
   Decision: version 1 will be local-only, but the architecture should be extendable for shared access later.

2. **Mac machine capability**
   Decision: machine has 18 GB RAM, which is sufficient for a strong local-first MVP.

3. **Question type priority**
   Decision: version 1 should prioritize player/match lookup and narrative explanation questions.
   Aggregate cross-match statistics should be planned for later versions via a structured analytics layer.

4. **UI preference**
   Decision: use Streamlit for version 1.

5. **Update preference**
   Pending explanation and final selection:
   - manual updates first is recommended for version 1
   - automated updates can be added after the ingestion flow is stable

---

## 21. Final Recommendation

The best free and maintainable path is:

- keep CricSheet files outside Git
- build a Python-based ingestion pipeline
- generate cricket-aware summary documents
- store embeddings in Chroma
- use Ollama for a local free LLM
- expose the system through Streamlit
- support incremental updates with a tracked ingestion registry
- later add a structured stats database for stronger cricket analytics

This gives us a realistic path to a cricket expert assistant without paid infrastructure.
