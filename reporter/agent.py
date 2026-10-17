"""Reporter - genera informe clinico markdown estructurado."""
from __future__ import annotations

import time
import json
from openai import OpenAI

from config import (
    KAFKA_TOPIC_ANALYZED,
    KAFKA_TOPIC_DRAFT,
    LLM_BASE_URL,
    LLM_API_KEY,
    LLM_MODEL,
    LLM_TEMPERATURE,
    LLM_MAX_TOKENS,
)
from models import DiagnosedReport, DraftReport
from storage import (
    get_logger, make_consumer,
    update_request_status, log_confidence, publish_with_trace,
)

log = get_logger("reporter")

SYSTEM_PROMPT = get_system_prompt("reporter")


def run():
    log.info("Reporter arrancando...")
    client = OpenAI(base_url=LLM_BASE_URL, api_key=LLM_API_KEY)
    consumer = make_consumer(KAFKA_TOPIC_ANALYZED, "reporter")

    for msg in consumer:
        try:
            t0 = time.time()
            d = DiagnosedReport(**msg.value)
            req_id = d.request_id

            specialist_json = json.dumps({
                "analysis": d.analysis,
                "impression": d.impression,
                "differential": d.differential,
                "confidence": d.confidence,
                "evidence": d.evidence,
            }, indent=2, ensure_ascii=False)

            user_prompt = f"""Analisis del especialista:
{specialist_json}

Metricas originales:
{d.retrieved.classified.features.model_dump_json(indent=2)}

Genera el informe markdown segun la estructura obligatoria."""

            resp = client.chat.completions.create(
                model=LLM_MODEL,
                messages=[
                    {"role": "system", "content": SYSTEM_PROMPT},
                    {"role": "user", "content": user_prompt},
                ],
                temperature=LLM_TEMPERATURE,
                max_tokens=LLM_MAX_TOKENS,
            )

            markdown = resp.choices[0].message.content.strip()
            latency_ms = int((time.time() - t0) * 1000)

            usage = getattr(resp, "usage", None)
            tokens = getattr(usage, "total_tokens", 0) if usage else 0

            # Confidence del reporter: hereda del diagnoser * factor de completitud
            # Si el informe tiene las 7 secciones requeridas = 1.0; menos = proporcional
            sections = ["## 1.", "## 2.", "## 3.", "## 4.", "## 5.", "## 6.", "## 7."]
            present = sum(1 for s in sections if s in markdown)
            completeness = present / len(sections)
            confidence = d.confidence * completeness

            flags = []
            if completeness < 0.85:
                flags.append(f"incomplete_report:{present}/7")

            log_confidence(
                request_id=req_id,
                agent_name="reporter",
                confidence=confidence,
                latency_ms=latency_ms,
                evidence=[f"draft_size:{len(markdown)}", f"sections:{present}/7"],
                flags=flags,
                model_used=LLM_MODEL,
                tokens_used=tokens,
            )

            out = DraftReport(
                request_id=req_id,
                user_id=d.user_id,
                message_id=d.message_id,
                study_uid=d.study_uid,
                diagnosed=d,
                markdown=markdown,
            )

            update_request_status(req_id, "drafted")

            publish_with_trace(
                topic=KAFKA_TOPIC_DRAFT,
                payload=out.model_dump(),
                from_agent="reporter",
                to_agent="auditor",
                from_topic=KAFKA_TOPIC_ANALYZED,
            )
            log.info(f"  req={req_id[:8]} mid={d.message_id} draft={len(markdown)} chars sections={present}/7 ({latency_ms}ms)")

        except Exception as e:
            log.exception(f"error en reporter: {e}")


if __name__ == "__main__":
    run()
