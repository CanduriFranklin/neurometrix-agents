"""Specialist (MedGemma) - diagnostic assistance."""
import time, json, re
from openai import OpenAI
from config import KAFKA_TOPIC_RETRIEVED, KAFKA_TOPIC_ANALYZED, SPECIALIST_LLM_BASE_URL, SPECIALIST_LLM_API_KEY, SPECIALIST_LLM_MODEL, LLM_TEMPERATURE, LLM_MAX_TOKENS
from models import RetrievedContext, DiagnosedReport
from agent_roles import get_system_prompt
from storage import get_logger, make_consumer, update_request_status, log_confidence, publish_with_trace

log = get_logger("specialist")
SYSTEM_PROMPT = get_system_prompt("specialist")

def format_features(c):
    f = c.classified.features
    return f"HU: {f.mean_hu:.1f}, Entropy: {f.mean_entropy:.2f}, CBF: {f.mean_cbf:.1f}, CBV: {f.mean_cbv:.1f}, MTT: {f.mean_mtt:.1f}, TMax: {f.mean_tmax:.1f}"

def run():
    log.info(f"Specialist starting: {SPECIALIST_LLM_MODEL}")
    client = OpenAI(base_url=SPECIALIST_LLM_BASE_URL, api_key=SPECIALIST_LLM_API_KEY)
    consumer = make_consumer(KAFKA_TOPIC_RETRIEVED, "specialist")

    for msg in consumer:
        try:
            t0 = time.time()
            ctx = RetrievedContext(**msg.value)
            req_id = ctx.request_id

            user_prompt = f"Analyze these metrics: {format_features(ctx)}. Respond ONLY in JSON with keys: analysis, impression, differential (list of strings), confidence (number), evidence (list of strings)."

            resp = client.chat.completions.create(
                model=SPECIALIST_LLM_MODEL,
                messages=[{"role": "system", "content": SYSTEM_PROMPT}, {"role": "user", "content": user_prompt}],
                temperature=LLM_TEMPERATURE, max_tokens=LLM_MAX_TOKENS,
            )
            
            content = resp.choices[0].message.content.strip()
            match = re.search(r'\{.*\}', content, re.DOTALL)
            if match: content = match.group(0)
            
            try:
                parsed = json.loads(content)
            except Exception:
                parsed = {}

            # 1. Safe Confidence
            raw_conf = parsed.get("confidence", 0.5)
            try: confidence = float(raw_conf)
            except (ValueError, TypeError):
                raw_str = str(raw_conf).lower()
                if "high" in raw_str: confidence = 0.9
                elif "low" in raw_str: confidence = 0.3
                else: confidence = 0.5

            # 2. Safe Differential (Force list of strings)
            raw_diff = parsed.get("differential", [])
            diff_list = []
            if isinstance(raw_diff, list):
                for item in raw_diff:
                    if isinstance(item, dict):
                        # Extract hypothesis or rationale if it's a dict
                        hyp = item.get("hypothesis", "")
                        rat = item.get("rationale", "")
                        diff_list.append(f"{hyp}: {rat}" if hyp and rat else str(item))
                    else:
                        diff_list.append(str(item))
            else:
                diff_list = [str(raw_diff)]

            # 3. Safe Evidence (Force list of strings)
            raw_ev = parsed.get("evidence", [])
            ev_list = []
            if isinstance(raw_ev, list):
                for item in raw_ev:
                    if isinstance(item, dict):
                        ev_list.append(" | ".join(f"{k}: {v}" for k,v in item.items()))
                    else:
                        ev_list.append(str(item))
            elif isinstance(raw_ev, dict):
                ev_list = [f"{k}: {v}" for k, v in raw_ev.items()]
            else:
                ev_list = [str(raw_ev)]

            latency_ms = int((time.time() - t0) * 1000)
            
            log_confidence(req_id, "specialist", confidence, latency_ms, ev_list, [], SPECIALIST_LLM_MODEL)

            out = DiagnosedReport(
                request_id=req_id, user_id=ctx.user_id, message_id=ctx.message_id, study_uid=ctx.study_uid,
                retrieved=ctx, 
                analysis=str(parsed.get("analysis", "No analysis provided.")), 
                impression=str(parsed.get("impression", "No impression provided.")),
                differential=diff_list, 
                confidence=confidence, 
                evidence=ev_list
            )
            
            update_request_status(req_id, "analyzed")
            publish_with_trace(KAFKA_TOPIC_ANALYZED, out.model_dump(), "specialist", "reporter", KAFKA_TOPIC_RETRIEVED)
            log.info(f"  req={req_id[:8]} conf={confidence:.2f} ({latency_ms}ms)")
            
        except Exception as e:
            log.exception(f"error: {e}")

if __name__ == "__main__":
    run()

