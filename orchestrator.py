"""Orquestador - arranca los 5 agentes en threads paralelos."""
from __future__ import annotations

import signal
import sys
import time
import threading

from storage import get_logger

log = get_logger("orchestrator")

_stop = threading.Event()


def _safe_run(name, fn):
    try:
        fn()
    except Exception as e:
        log.exception(f"[{name}] cayo: {e}")


def main():
    from classifier.agent import run as classifier_run
    from rag.agent import run as rag_run
    from diagnoser.agent import run as diagnoser_run
    from reporter.agent import run as reporter_run
    from auditor.agent import run as auditor_run
    from interface.agent import run as interface_run

    agents = [
        ("classifier", classifier_run),
        ("rag",        rag_run),
        ("specialist", diagnoser_run),
        ("reporter",   reporter_run),
        ("auditor",    auditor_run),
        ("interface",  interface_run),
    ]

    log.info("=" * 60)
    log.info("  Orquestador de agentes Neurometrix")
    log.info("=" * 60)

    for name, fn in agents:
        t = threading.Thread(target=_safe_run, args=(name, fn), daemon=True, name=name)
        t.start()
        log.info(f"  -> {name} iniciado")
        time.sleep(0.5)

    def shutdown(sig, frame):
        log.info("shutdown solicitado")
        _stop.set()
        sys.exit(0)

    signal.signal(signal.SIGINT, shutdown)
    signal.signal(signal.SIGTERM, shutdown)

    log.info("All 6 agents are running. Ctrl+C to stop.")
    while not _stop.is_set():
        time.sleep(1)


if __name__ == "__main__":
    main()
