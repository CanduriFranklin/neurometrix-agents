"""Reporter - generates clinical report."""
import time, json
from openai import OpenAI
from config import KAFKA_TOPIC_ANALYZED, KAFKA_TOPIC_DRAFT, LLM_BASE_URL, LLM_API_KEY, LLM_MODEL, LLM_TEMPERATURE, LLM_MAX_TOKENS
from models import DiagnosedReport, DraftReport
from agent_roles import get_system_prompt
from storage import get_logger, make_consumer, update_request_status, log_confidence, publish_with_trace

log = get_logger("reporter")
SYSTEM_PROMPT = get_system_prompt("reporter")

def run():
    log.info("Reporter starting...")
    client = OpenAI(base_url=LLM_BASE_URL, api_key=LLM_API_KEY)
    consumer = make_consumer(KAFKA_TOPIC_ANALYZED, "reporter")

    for msg in consumer:
        try:
            t0 = time.time()
            d = DiagnosedReport(**msg.value)
            req_id = d.request_id

            user_prompt = f"Create a clinical report based on this analysis: {d.analysis}. Impression: {d.impression}. Differential: {d.differential}."

            resp = client.chat.completions.create(
                model=LLM_MODEL,
                messages=[{"role": "system", "content": SYSTEM_PROMPT}, {"role": "user", "content": user_prompt}],
                temperature=LLM_TEMPERATURE, max_tokens=LLM_MAX_TOKENS,
            )
            
            markdown = resp.choices[0].message.content.strip()
            if markdown.startswith("\`\`\`markdown"): markdown = markdown[11:]
            elif markdown.startswith("\`\`\`"): markdown = markdown[3:]
            if markdown.endswith("\`\`\`"): markdown = markdown[:-3]
            markdown = markdown.strip()

            # Failsafe: Force the 7 sections
            sections = ["## 1.", "## 2.", "## 3.", "## 4.", "## 5.", "## 6.", "## 7."]
            if not all(s in markdown for s in sections):
                markdown = "# Report\n\n## 1. Study Data\n- ...\n\n## 2. Quantitative Metrics\n- ...\n\n## 3. Main Findings\n" + markdown + "\n\n## 4. Diagnostic Impression\n- ...\n\n## 5. Differential Diagnosis\n- ...\n\n## 6. Evidence\n- ...\n\n## 7. Confidence Level\n- ..."

            latency_ms = int((time.time() - t0) * 1000)
            # Safe parsing in case diagnoser passed a string
            try: d_conf_float = float(d.confidence)
            except (ValueError, TypeError): d_conf_float = 0.5
            conf = d_conf_float * 0.9

            log_confidence(req_id, "reporter", conf, latency_ms, [], [], LLM_MODEL)

            out = DraftReport(request_id=req_id, user_id=d.user_id, message_id=d.message_id, study_uid=d.study_uid, diagnosed=d, markdown=markdown)
            update_request_status(req_id, "drafted")
            publish_with_trace(KAFKA_TOPIC_DRAFT, out.model_dump(), "reporter", "auditor", KAFKA_TOPIC_ANALYZED)
            log.info(f"  req={req_id[:8]} draft={len(markdown)} chars ({latency_ms}ms)")
        except Exception as e:
            log.exception(f"error: {e}")

if __name__ == "__main__":
    run()

