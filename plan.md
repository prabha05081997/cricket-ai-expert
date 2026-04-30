# Project Plan: Local-First Cricket Expert Assistant

## 1. Project Goal

Build a chat application that feels like a real cricket expert, not a generic chatbot.

The assistant should:

- answer naturally in a conversational style
- understand cricket match questions from CricSheet data
- answer aggregate records and stat questions reliably
- explain cricket rules, formats, terminology, and context
- share useful facts, comparisons, and historical context when relevant
- stay grounded, transparent, and free to run locally

The final product should combine:

- **RAG** for match-specific and narrative questions
- **structured analytics** for cross-match statistics and records
- **domain knowledge** for rules, formats, and cricket facts
- **query routing** so each question is answered by the right subsystem

This is not a fine-tuning project at this stage. The practical path is:

- keep the base LLM unchanged
- build strong cricket-specific retrieval and analytics layers
- give the model better context and better tools
- shape the assistant with stronger prompting, routing, and UI behavior

---

## 2. Confirmed Constraint: Dataset Must Stay Outside the Repo

The CricSheet dataset is stored at:

`/Users/manojprabhakarm/Documents/work/personal/cricket-data/all_json`

This remains the correct setup because:

- the dataset is large (`~3.4 GB`)
- it should not be committed to Git
- it will keep changing over time

The app must use a configurable external data path through:

- `.env`
- environment variables
- or a small ignored local config

Recommended variable:

`CRICSHEET_DATA_DIR=/Users/manojprabhakarm/Documents/work/personal/cricket-data/all_json`

The repo should contain only:

- source code
- config templates
- docs
- tests
- small local metadata/state files

It should not contain:

- raw CricSheet JSON data
- large model downloads
- large vector or analytics artifacts that can be regenerated locally

---

## 3. Recommended Free Stack

### Backend

- **Python**
- **FastAPI**
- **Uvicorn**

### Retrieval / Indexing

- **custom CricSheet parser**
- **Sentence Transformers**
- **ChromaDB**
- **SQLite FTS / lightweight keyword retrieval**

### Structured Analytics

- **SQLite** for normalized cricket stats tables

### LLM Inference

- **Ollama**
- start with:
  - `llama3.2:3b`
  - `phi3:mini`
- evaluate stronger local models later if needed

### UI

- **Streamlit** for version 1 and version 1.5

### Optional Knowledge Assets

- curated local markdown / JSON knowledge packs for:
  - rules
  - formats
  - terminology
  - tournaments
  - iconic facts

---

## 4. Product Direction After MVP

The MVP proved that a simple RAG chat app is not enough for a strong cricket assistant.

What we learned:

- match-specific questions are a good fit for retrieval
- aggregate stat questions are a poor fit for plain RAG
- the assistant needs explicit question routing
- the assistant also needs non-CricSheet cricket knowledge
- response quality depends heavily on document quality, retrieval quality, and prompt discipline

So the new direction is:

1. keep the existing local-first architecture
2. strengthen retrieval and metadata filtering
3. add a structured analytics engine
4. add curated cricket knowledge beyond match files
5. add conversational polish so the assistant feels domain-native

---

## 5. Expanded Functional Requirements

The assistant should support these categories of questions:

### 5.1 Match Lookup Questions

Examples:

- "Who won the 2011 World Cup final?"
- "What happened in the last five overs of the match?"
- "How did Virat Kohli perform in that game?"

Best engine:

- RAG over CricSheet-derived documents

### 5.2 Aggregate Statistical Questions

Examples:

- "What is the highest individual score in ODI internationals?"
- "Who has the most wickets at Lord's?"
- "Which batter scored the most T20I runs in 2021?"

Best engine:

- structured analytics over normalized tables

### 5.3 Rules and Cricket Knowledge Questions

Examples:

- "What is the Duckworth-Lewis-Stern method?"
- "What is a powerplay in ODI cricket?"
- "What is the difference between a no-ball and a wide?"

Best engine:

- curated cricket knowledge base, optionally with lightweight RAG

### 5.4 Mixed Questions

Examples:

- "Why is Rohit Sharma's double century in ODI cricket special?"
- "How does a strike rate of 140 compare in T20 internationals?"

Best engine:

- query routing plus answer synthesis from multiple sources

### 5.5 Conversational Support

The assistant should also:

- ask clarifying questions when the user is ambiguous
- remember recent chat context within the current session
- adapt tone to the question type
- explain uncertainty honestly
- optionally add "interesting context" only when helpful

---

## 6. Non-Functional Requirements

- Must remain **free of cost**
- Must run **locally on consumer hardware**
- Must be **incrementally updateable**
- Must be **robust to CricSheet JSON variations**
- Must avoid **hallucinated stats**
- Must be **explainable** about where answers came from
- Must feel **fast enough for interactive use**
- Must feel **more like a cricket analyst than a generic assistant**

---

## 7. Data Sources

### 7.1 Primary Data Source

CricSheet JSON files from:

`/Users/manojprabhakarm/Documents/work/personal/cricket-data/all_json`

### 7.2 Secondary Knowledge Sources

To make the assistant more complete, we should add a small local knowledge layer for:

- laws and rules of cricket
- match formats and tournament structures
- common cricket terms and jargon
- special methods such as DLS
- major historical cricket facts and landmarks

Important note:

- CricSheet is excellent for match data
- CricSheet alone is not enough for full cricket knowledge

### 7.3 External Data Policy

The system should continue to support external dataset paths and keep all large source data outside Git.

---

## 8. Parsing, Normalization, and Knowledge Construction

### 8.1 Structured Match Extraction

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
- wicket events
- over-level summaries where useful

### 8.2 Edge Cases To Support

The parser should explicitly support:

- standard innings structures
- older wrapped innings structures
- forfeited matches
- no-result / abandoned matches
- super overs if present
- missing optional fields

### 8.3 Documents To Generate

Each match should generate multiple retrieval-friendly documents:

1. **match summary**
2. **innings summary**
3. **player performance summary**
4. **wicket timeline summary**
5. later: **death overs summary**
6. later: **over-level event summary**

### 8.4 Analytics Tables To Build

Alongside the RAG documents, build normalized SQLite tables such as:

- `matches`
- `innings`
- `batting_performances`
- `bowling_performances`
- `players`
- `venues`
- `teams`

Likely later additions:

- `partnerships`
- `dismissals`
- `overs`
- `tournaments`

### 8.5 Player Identity and Alias Resolution

The assistant should build a player identity layer so users do not need to type exact full names.

This layer should support:

- canonical player records
- full-name matching
- first-name-only matching where safe
- last-name-only matching where safe
- initials such as `V Kohli` or `MS Dhoni`
- common short references and spelling variants
- disambiguation when multiple players match the same reference

This is important for both:

- conversational questions such as "How did Kohli do?"
- analytics queries where player references must be resolved reliably

---

## 9. Retrieval and Query Routing Strategy

### 9.1 Retrieval Stack

Use:

- vector retrieval
- keyword retrieval
- metadata filtering
- reranking / scoring heuristics

### 9.2 Metadata Filtering

Important filters should include:

- match type
- gender
- event / tournament
- date / year
- venue
- team
- player
- international vs domestic signals where derivable

### 9.3 Query Routing

Before answering, classify the question into one of:

1. **match narrative**
2. **player-in-match**
3. **aggregate statistics**
4. **rules / knowledge**
5. **mixed / comparison**
6. **unsupported / ambiguous**

Recommended behavior:

- match narrative -> RAG
- aggregate stats -> analytics
- rules / knowledge -> curated knowledge
- mixed -> combine multiple sources

### 9.4 Guardrails

The assistant must:

- not invent unsupported records
- refuse unsupported stats cleanly
- expose supporting sources when possible
- prefer a short truthful answer over a confident wrong one

---

## 10. Conversational Design

The assistant should not sound like a generic chatbot.

It should behave like a cricket-savvy human expert who:

- answers directly first
- explains naturally
- knows when to add context
- avoids robotic repetition
- can switch between concise and detailed responses

Desired answer qualities:

- direct answer first
- natural explanatory style
- grounded evidence where relevant
- optional "why this matters" context
- optional "fun fact" or notable comparison when relevant and supported

The assistant should also support:

- short follow-up questions
- context carry-over inside a chat session
- conversational references like "that match", "that player", "last answer"

---

## 11. LLM Strategy

Use Ollama-served local models for answer generation.

### 11.1 Near-Term Model Strategy

Start with small reliable local models, but evaluate stronger options for better reasoning and style.

Candidate models:

- `llama3.2:3b`
- `phi3:mini`
- stronger local models later if your hardware supports them

### 11.2 Prompting Strategy

Prompts should vary by route:

- match-summary prompt
- stat-answer prompt
- rules-explainer prompt
- mixed-answer synthesis prompt

Each prompt should enforce:

- truthfulness
- route-specific formatting
- no invented stats
- concise direct answers first

### 11.3 Future Improvement Direction

To improve NLP quality without expensive fine-tuning:

- use better route-specific prompts
- improve retrieval precision
- improve document quality
- add clarifying-question behavior
- add conversation memory for the active session

---

## 12. UI and Experience Direction

### 12.1 Current UI

Continue with Streamlit for now.

### 12.2 Next UI Improvements

Add:

- visible answer type, such as `Match Answer`, `Stats Answer`, `Rules Answer`
- better source display
- retrieval debug mode
- follow-up question suggestions
- quick example prompts by category
- optional "why this answer" inspector

### 12.3 UX Goal

The UI should make it obvious that the assistant is:

- cricket-specific
- evidence-aware
- able to handle different question types differently

---

## 13. Update and Maintenance Strategy

### 13.1 Keep Existing Commands

- `python -m app.ingest update`
- `python -m app.ingest rebuild`

### 13.2 Improve Operational Visibility

Add:

- progress logging
- counts by indexed / skipped / failed
- failure-report command
- resumable behavior documentation

### 13.3 Add Analytics Rebuild Support

We will also need:

- analytics rebuild support
- analytics incremental update support

### 13.4 Deployment Modes

The project should explicitly support three deployment modes:

1. **local full version**
   - full dataset
   - full analytics
   - local Ollama
   - best quality and most realistic free mode

2. **lightweight public demo**
   - reduced dataset or curated subset
   - lighter model or remote inference alternative if ever allowed
   - simplified feature set
   - acceptable for demos, but not equivalent to the full assistant

3. **future shared hosted version**
   - intended for multi-user access
   - may require paid infrastructure
   - not a guaranteed zero-cost path

Important constraint:

- the refined plan is ideal for local-first use
- truly free long-term public hosting for the full assistant is unlikely without major tradeoffs

---

## 14. Recommended Repository Structure

```text
cricket-ai-expert/
  app/
    analytics/
    api/
    ingest/
    knowledge/
    rag/
    routing/
    settings/
    ui/
  storage/
  tests/
  .env.example
  .gitignore
  README.md
  plan.md
```

Suggested additional module responsibilities:

- `app/analytics/`: structured stats schema, loaders, query engine
- `app/knowledge/`: curated cricket rules, facts, and terminology assets
- `app/routing/`: classify question type and dispatch to the right subsystem

---

## 15. Risks and Practical Limits

### 15.1 RAG Alone Is Not Enough

This is now confirmed.

RAG is strong for:

- match lookup
- narrative explanations
- player-in-match summaries

RAG is weak for:

- global records
- top-N stats
- aggregate comparisons

### 15.2 Small Local Models Still Need Help

Even good local models can hallucinate when retrieval is weak or the task requires computation.

Mitigation:

- route better
- compute stats explicitly
- use stronger prompts
- expose evidence

### 15.3 Domain Knowledge Gaps

CricSheet does not contain everything needed for a full cricket expert persona.

Mitigation:

- add a small curated knowledge layer
- clearly separate data-derived facts from general cricket knowledge

### 15.4 Evaluation Is Essential

If we do not measure answer quality, the assistant will feel inconsistent.

Mitigation:

- build a question benchmark set
- test each route separately
- compare answer quality before and after changes

---

## 16. Recommended Build Phases Going Forward

### Phase 1: MVP Stabilization

- fix parser edge cases such as forfeited matches
- improve retrieval filters and ranking
- improve prompt discipline
- add progress and failure reporting

### Phase 2: Structured Analytics Layer

- build normalized SQLite analytics schema
- load batting and bowling performances
- implement aggregate stat queries
- support records by format, year, venue, player, and team

### Phase 3: Query Routing

- detect question type
- route to RAG, analytics, knowledge, or mixed mode
- return route-aware answer formats

### Phase 4: Cricket Knowledge Layer

- add curated rules / glossary / formats knowledge
- support rules questions and terminology explanations
- support lightweight fun facts and historical context

### Phase 5: Conversational Intelligence

- add session memory
- support follow-up references
- add clarification behavior for ambiguous questions
- improve natural answer style

### Phase 6: Quality and Evaluation

- create benchmark cricket questions by category
- measure route accuracy and failure modes
- tune prompts, retrieval, and analytics queries

### Phase 7: Product Polish

- better UI explanations
- richer source display
- answer route badges
- optional API improvements

---

## 17. Recommended Immediate Build Order

From here, the best sequence is:

1. **analytics layer**
2. **query routing**
3. **knowledge layer**
4. **conversation memory and follow-ups**
5. **evaluation harness**
6. **UI polish**

Reason:

- analytics fixes the biggest functional gap first
- routing makes the assistant more truthful
- knowledge layer makes it feel like a real cricket expert
- memory and follow-ups make it feel more human

---

## 18. Important Features Beyond the Original MVP

To make the assistant more effective and less generic, add these capabilities:

### 18.1 Must-Have

- structured stats engine
- query routing
- route-aware prompts
- curated cricket rules and terminology knowledge
- session memory for follow-ups
- player identity and alias resolution

### 18.2 High-Value Nice-To-Have

- interesting contextual add-ons such as:
  - "why this is notable"
  - "similar record"
  - "historical context"
- player/team disambiguation
- answer confidence / support level
- suggested follow-up questions

### 18.3 Later Ideas

- voice-friendly answer mode
- tournament summaries
- rivalry summaries
- richer player profile pages
- visual stat cards / charts

---

## 19. What Version 2 Should Feel Like

Version 2 should feel like:

- a cricket analyst
- a commentator when asked for narrative
- a stats desk when asked for records
- a friendly explainer when asked about rules or basics

It should not feel like:

- a generic chatbot with random cricket words
- an assistant that guesses stats from partial evidence
- a tool that answers everything the same way

---

## 20. Final Recommendation

The right path now is:

- keep the local-first architecture
- keep RAG for match and narrative questions
- add a structured analytics engine for record/stat questions
- add a curated cricket knowledge layer for rules and facts
- add query routing so the model stops using the wrong tool
- add conversational memory and stronger prompts so the assistant feels natural
- evaluate systematically so answer quality improves with each phase

This is the path that turns the current MVP into a true cricket expert assistant.
