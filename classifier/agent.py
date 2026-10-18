"""Classifier â€” determinista. Features numericas -> scores + flags."""
from __future__ import annotations

import time
from typing import Dict, List, Tuple

from openai import OpenAI

from config import (
    KAFKA_TOPIC_RESULTS, KAFKA_TOPIC_CLASSIFIED,
    LLM_BASE_URL, LLM_API_KEY, LLM_MODEL,
    LLM_TEMPERATURE, LLM_MAX_TOKENS,
)
from models import ClassifiedFeatures, VolumeFeatures, new_request_id, new_user_id
from agent_roles import get_system_prompt
from storage import (
    get_logger, make_consumer,
    register_request, update_request_status,
    log_confidence, publish_with_trace,
)

log = get_logger("classifier")


# ============================================================
# Thresholds clinicos (alineados con knowledge/perfusion_thresholds.md)
# ============================================================
TH = {
    # Perfusion
    "cbf_low": 20.0,
    "cbf_normal_lo": 40.0,
    "cbf_normal_hi": 60.0,
    "cbv_core": 2.0,
    "cbv_normal_lo": 3.0,
    "cbv_normal_hi": 5.0,
    "mtt_prolonged": 6.5,
    "tmax_penumbra": 6.0,
    "tmax_severe": 10.0,
    # Findings 3D
    "vesselness_high": 0.15,
    "asymmetry_high": 15.0,
    "entropy_high": 3.0,
    # Hemorragia (HU)
    "hu_acute_blood_lo": 50.0,
    "hu_acute_blood_hi": 80.0,
    "hu_hyperacute_lo": 40.0,
    "hu_hyperacute_hi": 60.0,
}


def classify(f: VolumeFeatures) -> Tuple[Dict[str, float], List[str], str, str]:
    """Devuelve (scores, flags, primary_hypothesis, reasoning)."""
    scores: Dict[str, float] = {}
    flags: List[str] = []
    reasons: List[str] = []

    # ---------- 3D / estructurales ----------
    if f.peak_vesselness > TH["vesselness_high"]:
        scores["vascular_structure"] = min(1.0, f.peak_vesselness / 0.5)
        flags.append("vesselness_elevated")
        reasons.append(f"peak_vesselness={f.peak_vesselness:.3f}")

    if f.asymmetry_score > TH["asymmetry_high"]:
        scores["hemispheric_asymmetry"] = min(1.0, f.asymmetry_score / 80.0)
        flags.append("asymmetry_elevated")
        reasons.append(f"asymmetry={f.asymmetry_score:.2f}")

    if f.mean_entropy > TH["entropy_high"]:
        scores["tissue_heterogeneity"] = min(1.0, f.mean_entropy / 5.0)
        flags.append("entropy_high")
        reasons.append(f"entropy={f.mean_entropy:.2f}")

    # ---------- Hemorragia (si hay findings) ----------
    for fd in f.findings:
        ftype = str(fd.get("type", "")).lower()
        if "hemorrhage" in ftype or "blood" in ftype:
            conf = float(fd.get("confidence", 0.0))
            scores["hemorrhage_suspected"] = max(scores.get("hemorrhage_suspected", 0.0), conf)
            flags.append("hemorrhage_finding")
            reasons.append(f"{ftype} conf={conf:.2f}")

    # ---------- 4D perfusion ----------
    if f.is_4d:
        if f.mean_cbf < TH["cbf_low"]:
            scores["ischemia_core"] = 1.0 - (f.mean_cbf / TH["cbf_low"])
            flags.append("cbf_low")
            reasons.append(f"CBF={f.mean_cbf:.1f}")
        elif f.mean_cbf < TH["cbf_normal_lo"]:
            scores["ischemia_penumbra"] = 0.6
            flags.append("cbf_borderline")
            reasons.append(f"CBF={f.mean_cbf:.1f}")

        if f.mean_cbv < TH["cbv_core"]:
            scores["core_volume"] = 1.0 - (f.mean_cbv / TH["cbv_core"])
            flags.append("cbv_low")
            reasons.append(f"CBV={f.mean_cbv:.2f}")

        if f.mean_mtt > TH["mtt_prolonged"]:
            scores["delayed_transit"] = min(1.0, f.mean_mtt / 12.0)
            flags.append("mtt_prolonged")
            reasons.append(f"MTT={f.mean_mtt:.2f}")

        if f.mean_tmax > TH["tmax_severe"]:
            scores["severe_hypoperfusion"] = min(1.0, f.mean_tmax / 15.0)
            flags.append("tmax_severe")
            reasons.append(f"TMax={f.mean_tmax:.2f}")
        elif f.mean_tmax > TH["tmax_penumbra"]:
            scores["penumbra_risk"] = 0.7
            flags.append("tmax_penumbra")
            reasons.append(f"TMax={f.mean_tmax:.2f}")

        # Mismatch core/penumbra
        if f.core_voxels > 0 and f.penumbra_voxels > 0:
            ratio = f.mismatch_ratio
            if ratio > 1.8:
                scores["mismatch_favorable"] = min(1.0, ratio / 5.0)
                flags.append("mismatch_favorable")
                reasons.append(f"mismatch_ratio={ratio:.2f}")
            elif ratio < 1.0:
                flags.append("mismatch_unfavorable")
                reasons.append(f"mismatch_ratio={ratio:.2f}")

    # ---------- Hipotesis primaria ----------
    if not scores:
        primary = "no_acute_finding"
    else:
        primary = max(scores, key=lambda k: scores[k])

    reasoning = " | ".join(reasons) if reasons else "sin hallazgos relevantes"
    return scores, flags, primary, reasoning


# ============================================================
# Loop
# ============================================================
# ============================================================
# CAPA 2: Razonamiento LLM (IA Responsable)
# ============================================================
RESPONSIBLE_AI_PROMPT = get_system_prompt("classifier")


def classify_with_llm(features, scores, flags, primary):
    """Enriquece la salida determinista con razonamiento LLM explicativo."""
    result = {
        "reasoning": "",
        "differential": [],
        "uncertainty": 1.0,
        "refused": True,
        "tokens": 0,
    }

    if not scores:
        result["reasoning"] = "No se detectaron hallazgos que superen umbrales clinicos."
        return result

    try:
        from openai import OpenAI
        import json as _json
        from config import LLM_BASE_URL, LLM_API_KEY, LLM_MODEL, LLM_TEMPERATURE, LLM_MAX_TOKENS

        client = OpenAI(base_url=LLM_BASE_URL, api_key=LLM_API_KEY)

        scores_txt = "\n".join(f"- {k}: {v:.3f}" for k, v in scores.items())
        flags_txt = ", ".join(flags) if flags else "ninguno"
        f = features

        user_prompt = f"""## Features del volumen
Modalidad: CT
Dimensiones: {f.height}x{f.width}x{f.depth} (T={f.timepoints})
Is 4D: {f.is_4d}

HU: mean={f.mean_hu:.1f} std={f.stddev_hu:.1f} min={f.min_hu:.1f} max={f.max_hu:.1f}
Entropy: {f.mean_entropy:.2f}
Vesselness: {f.peak_vesselness:.3f}
Asymmetry: {f.asymmetry_score:.2f}
Total volume: {f.total_volume_mm3:.0f} mm3

Perfusion (si 4D):
CBF={f.mean_cbf:.2f} CBV={f.mean_cbv:.2f} MTT={f.mean_mtt:.2f}
TTP={f.mean_ttp:.2f} TMax={f.mean_tmax:.2f}
core={f.core_voxels} penumbra={f.penumbra_voxels} ratio={f.mismatch_ratio:.2f}

## Scores del clasificador determinista (NO modificar)
{scores_txt}

## Flags activados
{flags_txt}

## Hipotesis primaria
{primary}

Explica estos hallazgos en JSON siguiendo las reglas de IA responsable."""

        resp = client.chat.completions.create(
            model=LLM_MODEL,
            messages=[
                {"role": "system", "content": RESPONSIBLE_AI_PROMPT},
                {"role": "user", "content": user_prompt},
            ],
            temperature=LLM_TEMPERATURE,
            max_tokens=LLM_MAX_TOKENS,
            response_format={"type": "json_object"},
        )

        parsed = _json.loads(resp.choices[0].message.content)
        usage = getattr(resp, "usage", None)
        tokens = getattr(usage, "total_tokens", 0) if usage else 0

        result["reasoning"] = parsed.get("reasoning", "")
        result["differential"] = parsed.get("differential", [])
        result["uncertainty"] = float(parsed.get("uncertainty", 0.5))
        result["refused"] = bool(parsed.get("refused", False))
        result["tokens"] = tokens

    except Exception as e:
        log.warning(f"LLM reasoning fallo, usando fallback determinista: {e}")
        result["reasoning"] = f"[fallback] Reglas activadas: {', '.join(flags)}"

    return result


def run() -> None:
    log.info("Classifier starting...")
    consumer = make_consumer(KAFKA_TOPIC_RESULTS, "classifier")

    for msg in consumer:
        try:
            raw = msg.value
            t0 = time.time()

            # Trazabilidad: heredar o generar request_id/user_id
            req_id = raw.get("request_id") or new_request_id()
            usr_id = raw.get("user_id") or new_user_id()
            raw["request_id"] = req_id
            raw["user_id"] = usr_id

            features = VolumeFeatures(**raw)
            log.info(f"classifying req={req_id[:8]} mid={features.message_id} study={features.study_uid}")

            # Registrar request maestro
            register_request(
                request_id=req_id,
                user_id=usr_id,
                study_uid=features.study_uid,
                message_id=features.message_id,
                entry_point="kafka",
            )

            # CAPA 1: clasificacion deterministica (fuente de verdad)
            scores, flags, primary, reasoning = classify(features)

            # CAPA 2: razonamiento LLM explicativo (IA Responsable)
            llm = classify_with_llm(features, scores, flags, primary)
            latency_ms = int((time.time() - t0) * 1000)

            # Confidence final: min(score_max, 1 - uncertainty_llm)
            score_conf = max(scores.values()) if scores else 0.0
            llm_conf = 1.0 - llm["uncertainty"]
            confidence = min(score_conf, llm_conf) if not llm["refused"] else 0.0

            out = ClassifiedFeatures(
                request_id=req_id,
                user_id=usr_id,
                message_id=features.message_id,
                study_uid=features.study_uid,
                features=features,
                scores=scores,
                flags=flags,
                primary_hypothesis=primary,
                reasoning=reasoning,
                llm_reasoning=llm["reasoning"],
                llm_differential=llm["differential"],
                llm_uncertainty=llm["uncertainty"],
                llm_refused=llm["refused"],
                llm_tokens=llm["tokens"],
                llm_model=LLM_MODEL,
            )

            # Persistir confidence del classifier (reglas + LLM)
            evidence = (reasoning.split(" | ") if reasoning else []) + [llm["reasoning"]]
            log_confidence(
                request_id=req_id,
                agent_name="classifier",
                confidence=confidence,
                latency_ms=latency_ms,
                evidence=evidence,
                flags=flags + (["llm_refused"] if llm["refused"] else []),
                model_used=LLM_MODEL,
                tokens_used=llm["tokens"],
            )

            update_request_status(req_id, "classified")

            publish_with_trace(
                topic=KAFKA_TOPIC_CLASSIFIED,
                payload=out.model_dump(),
                from_agent="classifier",
                to_agent="rag",
                from_topic=KAFKA_TOPIC_RESULTS,
            )
            log.info(f"  -> {primary} flags={flags} conf={confidence:.2f} ({latency_ms}ms)")

        except Exception as e:
            log.exception(f"error procesando mensaje: {e}")


if __name__ == "__main__":
    run()

