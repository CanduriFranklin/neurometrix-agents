"""Specialist (MedGemma) - diagnostico asistido con IA responsable."""
from __future__ import annotations

import time
import json
from openai import OpenAI

from config import (
    KAFKA_TOPIC_RETRIEVED,
    KAFKA_TOPIC_ANALYZED,
    SPECIALIST_LLM_BASE_URL,
    SPECIALIST_LLM_API_KEY,
    SPECIALIST_LLM_MODEL,
    LLM_TEMPERATURE,
    LLM_MAX_TOKENS,
)
from models import RetrievedContext, DiagnosedReport
from storage import (
    get_logger, make_consumer,
    update_request_status, log_confidence, publish_with_trace,
)

log = get_logger("specialist")

# ============================================================
# IA RESPONSABLE - Prompt del especialista
# ============================================================
SPECIALIST_SYSTEM_PROMPT = get_system_prompt("specialist")


def format_features(c):
    f = c.classified.features
    lines = [
        "Modalidad: CT",
        f"Dimensiones: {f.height}x{f.width}x{f.depth} (T={f.timepoints})",
        f"Is 4D: {f.is_4d}",
        "",
        "=== HU Statistics ===",
        f"mean_hu={f.mean_hu:.1f} stddev_hu={f.stddev_hu:.1f}",
        f"min_hu={f.min_hu:.1f} max_hu={f.max_hu:.1f}",
        "",
        "=== Estructurales 3D ===",
        f"mean_entropy={f.mean_entropy:.2f}",
        f"peak_vesselness={f.peak_vesselness:.3f}",
        f"asymmetry_score={f.asymmetry_score:.2f}",
        f"total_volume_mm3={f.total_volume_mm3:.0f}",
    ]
    if f.is_4d:
        lines += [
            "",
            "=== Perfusion 4D ===",
            f"mean_cbf={f.mean_cbf:.2f} mL/100g/min",
            f"mean_cbv={f.mean_cbv:.2f} mL/100g",
            f"mean_mtt={f.mean_mtt:.2f} s",
            f"mean_ttp={f.mean_ttp:.2f} s",
            f"mean_tmax={f.mean_tmax:.2f} s",
            f"core_voxels={f.core_voxels} penumbra_voxels={f.penumbra_voxels}",
            f"mismatch_ratio={f.mismatch_ratio:.2f}",
        ]
    lines += [
        "",
        "=== Clasificador determinista (capa 1) ===",
        f"primary: {c.classified.primary_hypothesis}",
        f"flags: {c.classified.flags}",
        f"scores: {c.classified.scores}",
        f"reasoning: {c.classified.reasoning}",
    ]
    if c.classified.llm_reasoning:
        lines += [
            "",
            "=== Razonamiento LLM del clasificador (capa 2) ===",
            c.classified.llm_reasoning,
        ]
    return "\n".join(lines)


def format_docs(docs):
    parts = []
    for d in docs:
        parts.append(f"[{d['source']}::{d['header']}]\n{d['text']}")
    return "\n\n---\n\n".join(parts)


def run():
    log.info(f"Specialist (MedGemma) arrancando: {SPECIALIST_LLM_MODEL} @ {SPECIALIST_LLM_BASE_URL}")
    client = OpenAI(base_url=SPECIALIST_LLM_BASE_URL, api_key=SPECIALIST_LLM_API_KEY)
    consumer = make_consumer(KAFKA_TOPIC_RETRIEVED, "specialist")

    for msg in consumer:
        try:
            t0 = time.time()
            ctx = RetrievedContext(**msg.value)
            req_id = ctx.request_id

            user_prompt = f"""## METRICAS DEL VOLUMEN
{format_features(ctx)}

## DOCUMENTOS DE REFERENCIA (RAG)
{format_docs(ctx.documents)}

Genera el analisis en JSON siguiendo las reglas de IA responsable."""

            resp = client.chat.completions.create(
                model=SPECIALIST_LLM_MODEL,
                messages=[
                    {"role": "system", "content": SPECIALIST_SYSTEM_PROMPT},
                    {"role": "user", "content": user_prompt},
                ],
                temperature=LLM_TEMPERATURE,
                max_tokens=LLM_MAX_TOKENS,
                response_format={"type": "json_object"},
            )

            parsed = json.loads(resp.choices[0].message.content)
            latency_ms = int((time.time() - t0) * 1000)

            usage = getattr(resp, "usage", None)
            tokens = getattr(usage, "total_tokens", 0) if usage else 0

            confidence = float(parsed.get("confidence", 0.0))
            evidence = parsed.get("evidence", [])
            flags = parsed.get("uncertainty_flags", [])

            if parsed.get("refused", False):
                flags.append("refused")
                confidence = 0.0
            if confidence < 0.5:
                flags.append("low_confidence")
            if not evidence:
                flags.append("no_evidence")

            log_confidence(
                request_id=req_id,
                agent_name="specialist",
                confidence=confidence,
                latency_ms=latency_ms,
                evidence=evidence,
                flags=flags,
                model_used=SPECIALIST_LLM_MODEL,
                tokens_used=tokens,
            )

            out = DiagnosedReport(
                request_id=req_id,
                user_id=ctx.user_id,
                message_id=ctx.message_id,
                study_uid=ctx.study_uid,
                retrieved=ctx,
                analysis=parsed.get("analysis", ""),
                impression=parsed.get("impression", ""),
                differential=parsed.get("differential", []),
                confidence=confidence,
                evidence=evidence,
            )

            update_request_status(req_id, "analyzed")

            publish_with_trace(
                topic=KAFKA_TOPIC_ANALYZED,
                payload=out.model_dump(),
                from_agent="specialist",
                to_agent="reporter",
                from_topic=KAFKA_TOPIC_RETRIEVED,
            )
            log.info(
                f"  req={req_id[:8]} conf={confidence:.2f} "
                f"refused={parsed.get('refused', False)} tokens={tokens} ({latency_ms}ms)"
            )

        except Exception as e:
            log.exception(f"error en specialist: {e}")


if __name__ == "__main__":
    run()
