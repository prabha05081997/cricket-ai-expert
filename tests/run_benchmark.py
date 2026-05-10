"""Benchmark evaluation runner.

Usage:
    python tests/run_benchmark.py                    # run all questions
    python tests/run_benchmark.py --category easy    # filter by difficulty
    python tests/run_benchmark.py --id A001          # run single question
    python tests/run_benchmark.py --dry-run          # show questions without running

Outputs a summary table and saves results to tests/benchmark_results.json.
"""

from __future__ import annotations

import argparse
import json
import sys
import time
from pathlib import Path

# Add project root to path
sys.path.insert(0, str(Path(__file__).parent.parent))

from app.analytics.stats import AnalyticsQueryService
from app.knowledge.service import KnowledgeService
from app.rag.index import LocalIndex
from app.rag.intent import classify_intent
from app.rag.llm import OllamaClient
from app.rag.service import ChatService
from app.settings import get_settings


def load_questions(
    filter_difficulty: str | None = None,
    filter_id: str | None = None,
) -> list[dict]:
    path = Path(__file__).parent / "benchmark_questions.json"
    questions = json.loads(path.read_text())
    if filter_id:
        questions = [q for q in questions if q["id"] == filter_id]
    if filter_difficulty:
        questions = [q for q in questions if q["difficulty"] == filter_difficulty]
    return questions


def build_service() -> ChatService:
    settings = get_settings()
    index = LocalIndex(
        registry_db_path=settings.registry_db_path,
        chroma_dir=settings.chroma_dir,
        collection_name=settings.chroma_collection,
        embedding_model_name=settings.embedding_model,
    )
    llm = OllamaClient(settings.ollama_base_url, settings.ollama_model)
    analytics = AnalyticsQueryService(
        settings.registry_db_path,
        ollama_base_url=settings.ollama_base_url,
        ollama_model=settings.ollama_intent_model,
    )
    knowledge = KnowledgeService()
    return ChatService(
        index=index,
        llm_client=llm,
        analytics_service=analytics,
        knowledge_service=knowledge,
    )


def evaluate_question(svc: ChatService, q: dict) -> dict:
    """Run a single question and check if expected_contains strings appear in the answer."""
    category = q["category"]
    question_text = q["question"]

    # For conversational follow-up questions, split on [follow-up]
    if "[follow-up]" in question_text:
        parts = question_text.split("[follow-up]")
        q1 = parts[0].strip()
        q2 = parts[1].strip()

        t0 = time.monotonic()
        r1 = svc.answer(q1)
        state = r1.get("conversation_state", {})
        r2 = svc.answer(q2, conversation_state=state)
        elapsed = time.monotonic() - t0

        answer = r2["answer"]
        expected_contains = q.get("expected_contains_q2", q.get("expected_contains", []))
    else:
        t0 = time.monotonic()
        result = svc.answer(question_text)
        elapsed = time.monotonic() - t0
        answer = result["answer"]
        expected_contains = q.get("expected_contains", [])

    # Check if all expected strings appear in the answer (case-insensitive)
    answer_lower = answer.lower()
    checks = {
        term: term.lower() in answer_lower
        for term in expected_contains
    }
    passed = all(checks.values()) if checks else True  # no checks = manual review needed

    return {
        "id": q["id"],
        "difficulty": q["difficulty"],
        "category": category,
        "question": question_text,
        "answer": answer,
        "expected_contains": expected_contains,
        "checks": checks,
        "passed": passed,
        "elapsed_s": round(elapsed, 1),
        "notes": q.get("notes", ""),
    }


def print_result(r: dict, verbose: bool = False) -> None:
    icon = "✓" if r["passed"] else "✗"
    print(f"  {icon} [{r['id']}] {r['difficulty']:6s} | {r['elapsed_s']:4.1f}s | {r['question'][:60]}")
    if not r["passed"] or verbose:
        print(f"       A: {r['answer'][:120]}")
        if not r["passed"]:
            failed = [t for t, ok in r["checks"].items() if not ok]
            print(f"       MISSING: {failed}")


def main() -> None:
    parser = argparse.ArgumentParser(description="Run cricket assistant benchmark")
    parser.add_argument("--difficulty", choices=["easy", "medium", "hard"], help="Filter by difficulty")
    parser.add_argument("--category", help="Filter by category")
    parser.add_argument("--id", help="Run a single question by ID")
    parser.add_argument("--dry-run", action="store_true", help="Show questions without running")
    parser.add_argument("--verbose", action="store_true", help="Show all answers, not just failures")
    args = parser.parse_args()

    questions = load_questions(
        filter_difficulty=args.difficulty,
        filter_id=args.id,
    )
    if args.category:
        questions = [q for q in questions if q["category"] == args.category]

    if not questions:
        print("No questions matched the filters.")
        return

    if args.dry_run:
        print(f"Would run {len(questions)} questions:")
        for q in questions:
            print(f"  [{q['id']}] {q['difficulty']:6s} | {q['category']:25s} | {q['question'][:60]}")
        return

    print(f"Building service...")
    svc = build_service()
    print(f"Running {len(questions)} benchmark questions...\n")

    results = []
    passed = 0
    failed = 0
    total_time = 0.0

    # Group by category for cleaner output
    by_category: dict[str, list[dict]] = {}
    for q in questions:
        by_category.setdefault(q["category"], []).append(q)

    for category, qs in by_category.items():
        print(f"\n── {category.upper().replace('_', ' ')} ({len(qs)} questions) ──")
        for q in qs:
            r = evaluate_question(svc, q)
            results.append(r)
            total_time += r["elapsed_s"]
            if r["passed"]:
                passed += 1
            else:
                failed += 1
            print_result(r, verbose=args.verbose)

    # Summary
    total = len(results)
    pct = passed / total * 100 if total else 0
    print(f"\n{'='*60}")
    print(f"RESULTS: {passed}/{total} passed ({pct:.0f}%)  |  {total_time:.0f}s total")
    print(f"  Easy:   {sum(1 for r in results if r['difficulty']=='easy' and r['passed'])}/{sum(1 for r in results if r['difficulty']=='easy')}")
    print(f"  Medium: {sum(1 for r in results if r['difficulty']=='medium' and r['passed'])}/{sum(1 for r in results if r['difficulty']=='medium')}")
    print(f"  Hard:   {sum(1 for r in results if r['difficulty']=='hard' and r['passed'])}/{sum(1 for r in results if r['difficulty']=='hard')}")

    # Save results
    out_path = Path(__file__).parent / "benchmark_results.json"
    out_path.write_text(json.dumps(results, indent=2, ensure_ascii=False))
    print(f"\nFull results saved to {out_path}")


if __name__ == "__main__":
    main()
