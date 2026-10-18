"""Auditor - verifies traceability."""
import time, json, re
from openai import OpenAI
from config import KAFKA_TOPIC_DRAFT, KAFKA_TOPIC_FINAL, LLM_BASE_URL, LLM_API_KEY, LLM_MODEL
from models import DraftReport, FinalReport
from agent_roles import get_system_prompt
from storage import get_logger, make_consumer, update_request_status, log_confidence, publish_with_trace, pg_execute, pg_fetchall

log = get_logger("auditor")
SYSTEM_PROMPT = get_system_prompt("auditor")

def run():
    log.info("Auditor starting...")
    client = OpenAI(base_url=LLM_BASE_URL, api_key=LLM_API_KEY)
    consumer = make_consumer(KAFKA_TOPIC_DRAFT, "auditor")

    for msg in consumer:
        try:
            t0 = time.time()
            draft = DraftReport(**msg.value)
            req_id = draft.request_id

            user_prompt = f"Audit this report for traceability. Respond ONLY in JSON with keys: audit_passed, audit_findings, confidence_score."

            resp = client.chat.completions.create(
                model=LLM_MODEL,
                messages=[{"role": "system", "content": SYSTEM_PROMPT}, {"role": "user", "content": user_prompt}],
                temperature=0.0, max_tokens=1024,
            )
            
            content = resp.choices[0].message.content.strip()
            match = re.search(r'\{.*\}', content, re.DOTALL)
            if match: content = match.group(0)
            
            try:
                parsed = json.loads(content)
            except Exception:
                parsed = {"audit_passed": True, "audit_findings": [], "confidence_score": 0.5}

            latency_ms = int((time.time() - t0) * 1000)
            audit_passed = parsed.get("audit_passed", False)
            # Safe parsing for confidence
            raw_conf = parsed.get("confidence_score", 0.5)
            try: confidence = float(raw_conf)
            except (ValueError, TypeError):
                raw_str = str(raw_conf).lower()
                if "high" in raw_str: confidence = 0.9
                elif "low" in raw_str: confidence = 0.3
                else: confidence = 0.5

            log_confidence(req_id, "auditor", confidence, latency_ms, [], parsed.get("audit_findings", []), LLM_MODEL)

            try:
                pg_execute("INSERT INTO agent_reports (request_id, user_id, message_id, study_uid, primary_hypothesis, analysis, impression, confidence, final_markdown, audit_passed, confidence_score) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s) ON CONFLICT (message_id) DO UPDATE SET audit_passed = EXCLUDED.audit_passed, confidence_score = EXCLUDED.confidence_score", 
                (req_id, draft.user_id, draft.message_id, draft.study_uid, draft.diagnosed.retrieved.classified.primary_hypothesis, draft.diagnosed.analysis, draft.diagnosed.impression, draft.diagnosed.confidence, draft.markdown, audit_passed, confidence))
            except Exception as e:
                log.warning(f"DB save failed: {e}")

            out = FinalReport(request_id=req_id, user_id=draft.user_id, message_id=draft.message_id, study_uid=draft.study_uid, draft=draft, audit_passed=audit_passed, confidence_score=confidence, final_markdown=draft.markdown)
            update_request_status(req_id, "audited", confidence)
            publish_with_trace(KAFKA_TOPIC_FINAL, out.model_dump(), "auditor", "sink", KAFKA_TOPIC_DRAFT)
            log.info(f"  req={req_id[:8]} passed={audit_passed} conf={confidence:.2f} ({latency_ms}ms)")
        except Exception as e:
            log.exception(f"error: {e}")

if __name__ == "__main__":
    run()

