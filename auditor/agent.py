"""Auditor - verifica trazabilidad y calcula confidence score final."""
from __future__ import annotations

import time
import re
import json
from openai import OpenAI

from config import (
    KAFKA_TOPIC_DRAFT,
    KAFKA_TOPIC_FINAL,
    LLM_BASE_URL,
    LLM_API_KEY,
    LLM_MODEL,
)
from models import DraftReport, FinalReport
from storage import (
    get_logger, make_consumer,
    update_request_status, log_confidence, publish_with_trace,
    pg_execute, pg_fetchall,
)

log = get_logger("auditor")

SYSTEM_PROMPT = get_system_prompt("auditor")


def deterministic_checks(draft):
    findings = []
    markdown = draft.markdown

    # Numeros decimales en el markdown vs features
    nums_md = set(re.findall(r"\b\d+\.\d+\b", markdown))
    feat_json = draft.diagnosed.retrieved.classified.features.model_dump_json()
    nums_feat = set(re.findall(r"\b\d+\.\d+\b", feat_json))

    unverified = nums_md - nums_feat
    unverified = {n for n in unverified if not n.startswith(("0.", "1.")) and len(n) < 8}
    if unverified:
        findings.append(f"numeros sin traza: {list(unverified)[:5]}")

    # Citas [fuente::header]
    citations = re.findall(r"\[([^\]]+::[^\]]+)\]", markdown)
    rag_sources = {f"{d['source']}::{d['header']}"
                   for d in draft.diagnosed.retrieved.documents}
    bad_cites = [c for c in citations if c not in rag_sources]
    if bad_cites:
        findings.append(f"citas no encontradas: {bad_cites[:3]}")

    return findings


def run():
    log.info("Auditor arrancando...")
    client = OpenAI(base_url=LLM_BASE_URL, api_key=LLM_API_KEY)
    consumer = make_consumer(KAFKA_TOPIC_DRAFT, "auditor")

    for msg in consumer:
        try:
            t0 = time.time()
            draft = DraftReport(**msg.value)
            req_id = draft.request_id

            det_findings = deterministic_checks(draft)

            user_prompt = f"""## INFORME
{draft.markdown[:4000]}

## METRICAS
{draft.diagnosed.retrieved.classified.features.model_dump_json()}

## EVIDENCIA ESPECIALISTA
{json.dumps(draft.diagnosed.evidence, indent=2)}

## VERIFICACION DETERMINISTA PREVIA
{det_findings if det_findings else 'sin problemas detectados automaticamente'}

Audita el informe y responde en JSON."""

            resp = client.chat.completions.create(
                model=LLM_MODEL,
                messages=[
                    {"role": "system", "content": SYSTEM_PROMPT},
                    {"role": "user", "content": user_prompt},
                ],
                temperature=0.0,
                max_tokens=1024,
                response_format={"type": "json_object"},
            )

            parsed = json.loads(resp.choices[0].message.content)
            audit_findings = det_findings + parsed.get("audit_findings", [])
            audit_passed = parsed.get("audit_passed", False) and len(det_findings) == 0
            latency_ms = int((time.time() - t0) * 1000)

            usage = getattr(resp, "usage", None)
            tokens = getattr(usage, "total_tokens", 0) if usage else 0

            confidence = float(parsed.get("confidence_score", 0.0))

            log_confidence(
                request_id=req_id,
                agent_name="auditor",
                confidence=confidence,
                latency_ms=latency_ms,
                evidence=parsed.get("evidence_trail", []),
                flags=audit_findings,
                model_used=LLM_MODEL,
                tokens_used=tokens,
            )

            # Pipeline confidence = min de todos los agentes
            try:
                rows = pg_fetchall(
                    "SELECT MIN(confidence) AS mn FROM agent_confidence WHERE request_id = %s",
                    (req_id,)
                )
                pipeline_conf = float(rows[0]["mn"]) if rows and rows[0]["mn"] is not None else confidence
            except Exception:
                pipeline_conf = confidence

            # Persistir informe final
            try:
                pg_execute("""
                    INSERT INTO agent_reports
                    (request_id, user_id, message_id, study_uid,
                     primary_hypothesis, scores_json,
                     analysis, impression, differential, confidence,
                     draft_markdown, final_markdown,
                     audit_passed, audit_findings, evidence_trail,
                     confidence_score, processing_ms)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                            %s, %s, %s, %s, %s)
                    ON CONFLICT (message_id) DO UPDATE SET
                        request_id = EXCLUDED.request_id,
                        user_id = EXCLUDED.user_id,
                        audit_passed = EXCLUDED.audit_passed,
                        final_markdown = EXCLUDED.final_markdown,
                        confidence_score = EXCLUDED.confidence_score,
                        updated_at = NOW()
                """, (
                    req_id,
                    draft.user_id,
                    draft.message_id,
                    draft.study_uid,
                    draft.diagnosed.retrieved.classified.primary_hypothesis,
                    json.dumps(draft.diagnosed.retrieved.classified.scores),
                    draft.diagnosed.analysis,
                    draft.diagnosed.impression,
                    json.dumps(draft.diagnosed.differential),
                    draft.diagnosed.confidence,
                    draft.markdown,
                    draft.markdown,
                    audit_passed,
                    json.dumps(audit_findings),
                    json.dumps(parsed.get("evidence_trail", [])),
                    confidence,
                    draft.diagnosed.retrieved.classified.features.processing_ms,
                ))
            except Exception as e:
                log.warning(f"no se pudo persistir en Postgres: {e}")

            # Escribir el informe a disco
            try:
                from config import REPORTS_DIR
                import os as _os
                rp = _os.path.join(REPORTS_DIR, f"{req_id}.md")
                with open(rp, "w", encoding="utf-8") as fh:
                    fh.write(draft.markdown)
            except Exception as e:
                log.warning(f"no se pudo escribir report a disco: {e}")

            out = FinalReport(
                request_id=req_id,
                user_id=draft.user_id,
                message_id=draft.message_id,
                study_uid=draft.study_uid,
                draft=draft,
                audit_passed=audit_passed,
                audit_findings=audit_findings,
                confidence_score=confidence,
                evidence_trail=parsed.get("evidence_trail", []),
                final_markdown=draft.markdown,
            )

            update_request_status(req_id, "audited", pipeline_confidence=pipeline_conf)

            publish_with_trace(
                topic=KAFKA_TOPIC_FINAL,
                payload=out.model_dump(),
                from_agent="auditor",
                to_agent="sink",
                from_topic=KAFKA_TOPIC_DRAFT,
            )

            log.info(
                f"  req={req_id[:8]} mid={draft.message_id} "
                f"passed={audit_passed} conf={confidence:.2f} "
                f"pipeline={pipeline_conf:.2f} ({latency_ms}ms)"
            )

        except Exception as e:
            log.exception(f"error en auditor: {e}")


if __name__ == "__main__":
    run()
