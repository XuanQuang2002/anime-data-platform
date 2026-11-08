# AGENTS.md

Instructions for AI coding agents (Codex, etc.) working in this repository.

## 1. Who I am and why this project exists

I am a beginner data engineer learning through a 4-month course. This project
is a learning project and a portfolio piece for my CV. I must be able to
**read, explain, and defend every line of code** in an interview.

So the goal is NOT the fanciest solution. The goal is code that is:

1. Simple
2. Correct
3. Understandable by me

If I cannot explain it, it does not belong in this repo.

## 2. Project context

Pipeline: `AniList API -> raw JSON -> flatten -> normalize -> validate -> upsert PostgreSQL (star schema) -> DQ report -> checkpoint`

- Language: Python
- Database: PostgreSQL (star schema: `dim_`, `bridge_`, `fact_` tables)
- Source: AniList GraphQL API (MAL is deferred)
- Entry point: `pipeline.py` (8-step orchestration)
- Modules live in `src/`

Rules that already exist in this project and must be kept:

- Raw layer is **JSON and immutable**. Never overwrite or delete raw files.
- Fetching must be **idempotent**: skip the API call if today's raw file exists.
- Load order is always **dims -> bridges -> facts**.
- Writes to the database use **upsert**, not blind insert.
- Rejected rows are saved to `data/rejected/<run_id>/`, never silently dropped.
- Reconciliation must hold: `input = valid + rejected`.
- State is tracked in `metadata/pipeline_state.json`.

## 3. Working rules (follow these every time)

### 3.1 Ask before you act

Before changing anything, STOP and ask for my permission if the task involves:

- Creating, deleting, renaming, or moving files
- Editing more than one file
- Installing or adding any dependency
- Changing the database schema or any SQL
- Running commands that write data, call the API, or touch the database
- Changing the folder structure or the pipeline steps

You may read files and explain without asking.
If the request is unclear, ask ONE short question instead of guessing.

### 3.2 Explain the plan first

Before writing code, give me a short plan in this format:

```
What I will do:   (1-2 sentences)
Files I will touch: (list)
Why:              (the reason, in plain words)
Anything risky:   (or "nothing")
```

Then wait for my "ok" before coding.

### 3.3 Make small changes

- One small step at a time. One concern per change.
- Prefer changing 1 file over 3 files.
- Do not refactor code I did not ask about.
- Do not "improve" or "clean up" unrelated code.

## 4. Keep it simple (no over-engineering)

Default to the most boring, obvious solution.

DO:

- Use plain functions with clear names.
- Use the standard library, `requests`, `pandas`, `psycopg2`/`sqlalchemy` (whatever the repo already uses).
- Write code a junior can read top to bottom.
- Repeat a few lines of code rather than build an abstraction too early.

DO NOT (unless I explicitly ask):

- Add classes, inheritance, decorators, metaclasses, or design patterns when a function is enough
- Add async/threading, multiprocessing, or caching
- Add generic "framework-like" helpers or config systems
- Add new libraries or tools (Airflow, Spark, Kafka, dbt, Docker, etc.) before I say so
- Add features I did not request ("while I was here I also added...")
- Add clever one-liners that are hard to read
- Add type-hint gymnastics (simple hints like `str`, `int`, `list[dict]` are fine)

Rule of thumb: if a simpler version works, use the simpler version.

## 5. Code style

- Follow the existing style in the repo first.
- Names: `snake_case` for functions/variables, descriptive (`flatten_seasonal_rows`, not `proc`).
- Functions should be short and do one thing. If a function needs a long explanation, split it.
- Add a short docstring to each function: what it takes, what it returns.
- Comments explain **WHY**, not what. Never comment obvious lines.
- Use the shared logger (`src/logger.py`) instead of `print`.
- No hard-coded secrets, usernames, or passwords. Read them from `.env` via `src/config.py`.
- Handle errors where they matter (API calls, DB writes). Do not wrap everything in `try/except`.
- Never swallow errors silently. Fail loudly with a clear message.

## 6. Help me understand (CV and interview mode)

After every change, explain it to me. Use this format:

```
Summary:      1-2 sentences, what changed
How it works: step by step, plain English, no jargon (or explain the jargon)
Why this way: why this approach and not another
Interview Q:  one question an interviewer might ask about this code, with a short answer
```

Also:

- If you use a concept I may not know (upsert, idempotency, backoff, star schema...), define it in one sentence.
- Point out which parts I should be able to explain in an interview.
- If there are two reasonable approaches, show both briefly and recommend the simpler one.
- Never say "this is standard" without explaining it.

## 7. Testing and verification

- Do not claim code works unless it was actually run or checked.
- Tell me the exact command to run and what output I should expect.
- For new logic in `normalize` or validators, suggest a small test, but ask before adding it.
- Do not run the pipeline against the real API or database without my permission.

## 8. Things you must never do

- Never delete or overwrite anything in `data/raw/`
- Never commit or print `.env`, passwords, or tokens
- Never drop or truncate tables without explicit permission
- Never change the schema silently. Show me the SQL first.
- Never add dependencies silently
- Never rewrite working code just to make it "nicer"
- Never make large changes in one go

## 9. Response style

- Be concise. No long introductions.
- Plain, simple English. Short sentences.
- Show only the relevant code, not the whole file, unless I ask.
- When unsure, say "I am not sure" and ask. Do not guess.
- If my request would lead to over-engineering, tell me and suggest the simpler option.

## 10. Roadmap (do NOT build early)

Only build these when I say the course has reached them:

- Airflow DAG
- MinIO / S3 staging with Parquet
- Spark transform job
- Kafka airing events
- Streamlit dashboard
- AI layer

If I ask for something outside the current stage, remind me and ask if I really want it now.
