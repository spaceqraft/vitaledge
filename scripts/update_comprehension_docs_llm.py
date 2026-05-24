#!/usr/bin/env python3
"""Regenerate COMPREHENSION docs from DESIGN/IMPLEMENTATION_PLAN using an LLM."""

from __future__ import annotations

import argparse
import json
import os
import re
import sys
import urllib.error
import urllib.request
from pathlib import Path


def read_text(path: Path) -> str:
    if not path.exists():
        raise FileNotFoundError(f"missing file: {path}")
    return path.read_text(encoding="utf-8")


def extract_outputs(raw: str) -> tuple[str, str]:
    marker_pattern = re.compile(
        r"===COMPREHENSION-Q\.md===\n(?P<q>.*?)\n===COMPREHENSION-A\.md===\n(?P<a>.*)",
        re.DOTALL,
    )
    match = marker_pattern.search(raw.strip())
    if match:
        return match.group("q").strip() + "\n", match.group("a").strip() + "\n"

    stripped = raw.strip()
    if stripped.startswith("{"):
        try:
            payload = json.loads(stripped)
            q_doc = payload["q_markdown"].strip() + "\n"
            a_doc = payload["a_markdown"].strip() + "\n"
            return q_doc, a_doc
        except (KeyError, json.JSONDecodeError):
            pass

    raise ValueError("could not parse model output into Q/A markdown files")


def validate_docs(q_doc: str, a_doc: str) -> None:
    q_ids = re.findall(r"^### Q-(\d{3}):", q_doc, flags=re.MULTILINE)
    a_ids = re.findall(r"^### A-(\d{3})$", a_doc, flags=re.MULTILINE)
    if not q_ids or not a_ids:
        raise ValueError("missing Q/A headings in generated docs")
    if q_ids != a_ids:
        raise ValueError(f"Q/A heading mismatch: Q={len(q_ids)} A={len(a_ids)}")

    for q_id in q_ids:
        if f"COMPREHENSION-A.md#a-{q_id}" not in q_doc:
            raise ValueError(f"missing answer link for Q-{q_id}")
        if f"COMPREHENSION-Q.md#q-{q_id}" not in a_doc:
            raise ValueError(f"missing back link for A-{q_id}")


def call_llm(api_url: str, api_key: str, model: str, prompt: str) -> str:
    payload = {
        "model": model,
        "temperature": 0.2,
        "input": [
            {
                "role": "system",
                "content": (
                    "You are generating architecture comprehension documentation. "
                    "Produce concise, high-signal markdown with stable anchors and links."
                ),
            },
            {"role": "user", "content": prompt},
        ],
    }

    req = urllib.request.Request(
        api_url,
        data=json.dumps(payload).encode("utf-8"),
        headers={
            "Content-Type": "application/json",
            "Authorization": f"Bearer {api_key}",
        },
        method="POST",
    )

    try:
        with urllib.request.urlopen(req, timeout=180) as resp:
            raw = resp.read().decode("utf-8")
    except urllib.error.HTTPError as err:
        body = err.read().decode("utf-8", errors="replace")
        raise RuntimeError(f"LLM API HTTP error {err.code}: {body}") from err
    except urllib.error.URLError as err:
        raise RuntimeError(f"LLM API connection error: {err}") from err

    parsed = json.loads(raw)
    if isinstance(parsed.get("output_text"), str) and parsed["output_text"].strip():
        return parsed["output_text"]

    chunks: list[str] = []
    for item in parsed.get("output", []):
        for content in item.get("content", []):
            text = content.get("text")
            if isinstance(text, str):
                chunks.append(text)
    if chunks:
        return "\n".join(chunks)

    raise RuntimeError("LLM API response contained no text output")


def build_prompt(design_md: str, plan_md: str, current_q: str, current_a: str) -> str:
    return f"""
Regenerate COMPREHENSION markdown docs based on the current DESIGN and IMPLEMENTATION_PLAN docs.

Goals:
1. Preserve high-quality, human-meaningful questions and answers (not generic section-title rewrites).
2. Re-evaluate design and implementation updates in the inputs.
3. Keep bidirectional links between each question and answer.
4. Keep stable heading IDs in the format q-001..q-XXX and a-001..a-XXX.
5. Keep markdown concise and developer-focused.

Output format requirements:
- Return ONLY two markdown documents in this exact wrapper format:
===COMPREHENSION-Q.md===
<full markdown for COMPREHENSION-Q.md>
===COMPREHENSION-A.md===
<full markdown for COMPREHENSION-A.md>

Current DESIGN.md:
---
{design_md}
---

Current IMPLEMENTATION_PLAN.md:
---
{plan_md}
---

Current COMPREHENSION-Q.md (style and baseline):
---
{current_q}
---

Current COMPREHENSION-A.md (style and baseline):
---
{current_a}
---
""".strip()


def main() -> int:
    parser = argparse.ArgumentParser(description="LLM-driven COMPREHENSION doc refresh")
    parser.add_argument("--design", default="DESIGN.md", help="Path to DESIGN markdown")
    parser.add_argument(
        "--plan", default="IMPLEMENTATION_PLAN.md", help="Path to implementation plan markdown"
    )
    parser.add_argument("--out-q", default="COMPREHENSION-Q.md", help="Output Q markdown path")
    parser.add_argument("--out-a", default="COMPREHENSION-A.md", help="Output A markdown path")
    parser.add_argument(
        "--api-url",
        default=os.getenv("OPENAI_API_URL", "https://api.openai.com/v1/responses"),
        help="LLM API URL",
    )
    parser.add_argument(
        "--model",
        default=os.getenv("COMPREHENSION_LLM_MODEL", "gpt-4.1"),
        help="LLM model name",
    )
    args = parser.parse_args()

    api_key = os.getenv("OPENAI_API_KEY", "").strip()
    if not api_key:
        print("ERROR: OPENAI_API_KEY is required", file=sys.stderr)
        return 2

    design_path = Path(args.design)
    plan_path = Path(args.plan)
    out_q = Path(args.out_q)
    out_a = Path(args.out_a)

    try:
        design_md = read_text(design_path)
        plan_md = read_text(plan_path)
        current_q = out_q.read_text(encoding="utf-8") if out_q.exists() else ""
        current_a = out_a.read_text(encoding="utf-8") if out_a.exists() else ""
    except FileNotFoundError as err:
        print(f"ERROR: {err}", file=sys.stderr)
        return 2

    prompt = build_prompt(design_md, plan_md, current_q, current_a)

    try:
        llm_output = call_llm(args.api_url, api_key, args.model, prompt)
        q_doc, a_doc = extract_outputs(llm_output)
        validate_docs(q_doc, a_doc)
    except (RuntimeError, ValueError) as err:
        print(f"ERROR: {err}", file=sys.stderr)
        return 1

    out_q.write_text(q_doc, encoding="utf-8")
    out_a.write_text(a_doc, encoding="utf-8")

    print(f"Updated {out_q} and {out_a} via model {args.model}")
    print(f"Validated Q/A pairs: {len(re.findall(r'^### Q-(\\d{{3}}):', q_doc, flags=re.MULTILINE))}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
