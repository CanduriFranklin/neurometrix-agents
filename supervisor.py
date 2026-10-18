
import logging
import threading
import time
from orchestrator_agent import run as orchestrator_run
from data_engineer.agent import run as etl_run
from classifier.agent import run as classifier_run
from rag.agent import run as rag_run
from diagnoser.agent import run as diagnoser_run
from reporter.agent import run as reporter_run
from auditor.agent import run as auditor_run
from interface.agent import run as interface_run

logging.basicConfig(level=logging.INFO, format="[%(asctime)s] [%(name)s] [%(levelname)s] %(message)s")
log = logging.getLogger("supervisor")

agents = [
    ("orchestrator", orchestrator_run),
    ("data_engineer", etl_run),
    ("classifier", classifier_run),
    ("rag", rag_run),
    ("specialist", diagnoser_run),
    ("reporter", reporter_run),
    ("auditor", auditor_run),
    ("interface", interface_run),
]

def start_agent(name, fn):
    try:
        log.info(f"Starting agent: {name}")
        fn()
    except Exception as e:
        log.error(f"Agent {name} crashed: {e}")

if __name__ == "__main__":
    log.info("="*50)
    log.info("  Neurometrix Supervisor & Agents")
    log.info("="*50)
    
    threads = []
    for name, fn in agents:
        t = threading.Thread(target=start_agent, args=(name, fn), name=name, daemon=True)
        t.start()
        threads.append(t)
        time.sleep(0.5)

    log.info("All 8 services are running. Ctrl+C to stop.")
    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        log.info("Shutting down...")
