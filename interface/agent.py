"""Interface Agent - conversational gateway with the human specialist.

Responsible AI principles:
- Full pipeline transparency (every answer cites DB source)
- Human escalation recommended by default
- Every interaction is logged (chat_messages table)
- NEVER emits own diagnosis
"""
from __future__ import annotations

import time
import json
from openai import OpenAI

from config import (
    KAFKA_TOPIC_CHAT,
    LLM_BASE_URL,
    LLM_API_KEY,
    LLM_MODEL,
    LLM_TEMPERATURE,
    LLM_MAX_TOKENS,
)
from models import ChatMessage, ChatResponse
from roles import get_system_prompt
from storage import (
    get_logger, make_consumer,
    publish_with_trace, pg_execute, pg_fetchall,
)

log = get_logger("interface")

SYSTEM_PROMPT = get_system_prompt("interface")


# ============================================================
# Database context builders (IA Responsible: cite sources)
# ============================================================
def fetch_case_context(request_id: str) -> dict:
    """Retrieve full case state from Postgres."""
    ctx = {
        "request": None,
        "report": None,
        "confidences": [],
        "transactions": [],
        "history": [],
    }

    try:
        reqs = pg_fetchall("""
            SELECT request_id, user_id, study_uid, message_id,
                   status, pipeline_confidence, created_at
            FROM requests WHERE request_id = %s
        """, (request_id,))
        if reqs:
            ctx["request"] = dict(reqs[0])
    except Exception as e:
        log.warning(f"fetch request failed: {e}")

    try:
        reps = pg_fetchall("""
            SELECT primary_hypothesis, analysis, impression, differential,
                   confidence, audit_passed, audit_findings, evidence_trail,
                   confidence_score, final_markdown
            FROM agent_reports WHERE request_id = %s
            ORDER BY id DESC LIMIT 1
        """, (request_id,))
        if reps:
            ctx["report"] = dict(reps[0])
    except Exception as e:
        log.warning(f"fetch report failed: {e}")

    try:
        ctx["confidences"] = [dict(r) for r in pg_fetchall("""
            SELECT agent_name, confidence, latency_ms, model_used,
                   tokens_used, created_at
            FROM agent_confidence WHERE request_id = %s
            ORDER BY id
        """, (request_id,))]
    except Exception as e:
        log.warning(f"fetch confidences failed: {e}")

    try:
        ctx["transactions"] = [dict(r) for r in pg_fetchall("""
            SELECT from_agent, to_agent, from_topic, to_topic,
                   status, occurred_at
            FROM agent_transactions WHERE request_id = %s
            ORDER BY occurred_at
        """, (request_id,))]
    except Exception as e:
        log.warning(f"fetch transactions failed: {e}")

    try:
        ctx["history"] = [dict(r) for r in pg_fetchall("""
            SELECT role, content, created_at
            FROM chat_messages WHERE request_id = %s
            ORDER BY created_at DESC LIMIT 10
        """, (request_id,))]
        ctx["history"].reverse()
    except Exception as e:
        log.warning(f"fetch chat history failed: {e}")

    return ctx


def format_context(ctx: dict) -> str:
    """Format the case context for the LLM prompt."""
    parts = []

    if ctx["request"]:
        r = ctx["request"]
        parts.append(f"""## REQUEST
- request_id: {r['request_id']}
- study_uid: {r.get('study_uid', '?')}
- status: {r.get('status', '?')}
- pipeline_confidence: {r.get('pipeline_confidence', '?')}""")

    if ctx["report"]:
        rep = ctx["report"]
        parts.append(f"""## FINAL REPORT
- primary_hypothesis: {rep.get('primary_hypothesis', '?')}
- impression: {rep.get('impression', '?')}
- confidence: {rep.get('confidence', '?')}
- audit_passed: {rep.get('audit_passed', '?')}
- confidence_score: {rep.get('confidence_score', '?')}
- audit_findings: {json.dumps(rep.get('audit_findings', []))}""")

    if ctx["confidences"]:
        lines = [f"- {c['agent_name']}: conf={c['confidence']:.2f} "
                 f"latency={c['latency_ms']}ms model={c.get('model_used', 'none')}"
                 for c in ctx["confidences"]]
        parts.append("## AGENT CONFIDENCES\n" + "\n".join(lines))

    if ctx["transactions"]:
        lines = [f"- {t.get('from_agent', 'core')} -> {t['to_agent']} "
                 f"({t['status']})"
                 for t in ctx["transactions"]]
        parts.append("## PIPELINE TRANSACTIONS\n" + "\n".join(lines))

    return "\n\n".join(parts) if parts else "(no case context found)"


def save_message(request_id, user_id, session_id, role,
                 content, cited_sources=None, confidence=None,
                 tokens_used=None, model_used=None):
    """Persist a chat message for audit trail."""
    try:
        pg_execute("""
            INSERT INTO chat_messages
                (request_id, user_id, session_id, role, content,
                 cited_sources, confidence, tokens_used, model_used)
            VALUES (%s, %s, %s, %s, %s, %s::jsonb, %s, %s, %s)
        """, (
            request_id, user_id, session_id, role, content,
            json.dumps(cited_sources or []),
            confidence, tokens_used, model_used,
        ))
    except Exception as e:
        log.warning(f"save_message failed: {e}")


def answer_question(msg: ChatMessage, client: OpenAI) -> ChatResponse:
    """Generate a grounded answer citing the DB sources."""
    ctx = fetch_case_context(msg.request_id)
    context_txt = format_context(ctx)

    history_txt = ""
    if ctx["history"]:
        history_txt = "\n\n## RECENT CONVERSATION\n" + "\n".join(
            f"[{h['role']}] {h['content'][:300]}" for h in ctx["history"]
        )

    prompt = f"""## CASE CONTEXT (from database)
{context_txt}
{history_txt}

## SPECIALIST QUESTION
{msg.content}

Respond following your ROLE and PROHIBITIONS.
Cite the exact source of each fact (table.column or agent.name).
If you lack data, say so clearly.
Return JSON:
{{
  "answer": "your answer with citations",
  "cited_sources": ["table.column", "agent.name", ...],
  "confidence": 0.0,
  "escalation_recommended": false,
  "escalation_reason": ""
}}
"""

    resp = client.chat.completions.create(
        model=LLM_MODEL,
        messages=[
            {"role": "system", "content": SYSTEM_PROMPT},
            {"role": "user", "content": prompt},
        ],
        temperature=LLM_TEMPERATURE,
        max_tokens=LLM_MAX_TOKENS,
        response_format={"type": "json_object"},
    )

    parsed = json.loads(resp.choices[0].message.content)
    usage = getattr(resp, "usage", None)
    tokens = getattr(usage, "total_tokens", 0) if usage else 0

    return ChatResponse(
        request_id=msg.request_id,
        user_id=msg.user_id,
        answer=parsed.get("answer", ""),
        cited_sources=parsed.get("cited_sources", []),
        confidence=float(parsed.get("confidence", 0.0)),
        escalation_recommended=bool(parsed.get("escalation_recommended", True)),
        escalation_reason=parsed.get("escalation_reason", ""),
        tokens_used=tokens,
        model_used=LLM_MODEL,
    )


def run():
    log.info(f"Interface agent starting: {LLM_MODEL} @ {LLM_BASE_URL}")
    client = OpenAI(base_url=LLM_BASE_URL, api_key=LLM_API_KEY)
    consumer = make_consumer(KAFKA_TOPIC_CHAT, "interface")

    for raw in consumer:
        try:
            t0 = time.time()
            payload = raw.value

            # Incoming message from Streamlit or API
            incoming = ChatMessage(**{
                "request_id": payload["request_id"],
                "user_id": payload["user_id"],
                "role": "user",
                "content": payload["content"],
                "metadata": payload.get("metadata", {}),
            })

            session_id = incoming.metadata.get("session_id", "")
            log.info(f"Q req={incoming.request_id[:8]} user={incoming.user_id[:8]}")

            save_message(
                request_id=incoming.request_id,
                user_id=incoming.user_id,
                session_id=session_id,
                role="user",
                content=incoming.content,
            )

            resp = answer_question(incoming, client)
            latency_ms = int((time.time() - t0) * 1000)

            save_message(
                request_id=incoming.request_id,
                user_id=incoming.user_id,
                session_id=session_id,
                role="assistant",
                content=resp.answer,
                cited_sources=resp.cited_sources,
                confidence=resp.confidence,
                tokens_used=resp.tokens_used,
                model_used=resp.model_used,
            )

            publish_with_trace(
                topic=KAFKA_TOPIC_CHAT,
                payload=resp.model_dump(),
                from_agent="interface",
                to_agent="frontend",
                from_topic=KAFKA_TOPIC_CHAT,
            )

            log.info(
                f"  A req={incoming.request_id[:8]} conf={resp.confidence:.2f} "
                f"escalate={resp.escalation_recommended} ({latency_ms}ms)"
            )

        except Exception as e:
            log.exception(f"interface error: {e}")


if __name__ == "__main__":
    run()
