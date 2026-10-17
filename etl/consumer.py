"""ETL - puente entre core-4d.results y features normalizadas.

En esta version, el mensaje ya viene serializado como JSON desde el consumer
original del core. Si tu Kafka no tiene ese traductor, este script debe
hacerse cargo del parsing binario.
"""
from __future__ import annotations

import json

from config import KAFKA_TOPIC_RESULTS
from storage import get_logger, make_consumer

log = get_logger("etl")

# NOTA: los mensajes de core-4d.results vienen del C++ en formato binario
# [8B mid LE][8B ts LE][4B uid_len][uid][4B path_len][path].
# El classifier espera JSON. Esta capa debe hacer el bridge.


def run():
    log.info("ETL bridge arrancando (no-op si el core ya emite JSON)...")
    consumer = make_consumer(KAFKA_TOPIC_RESULTS, "etl-probe")
    for msg in consumer:
        v = msg.value
        if isinstance(v, dict):
            log.debug(f"JSON OK mid={v.get('message_id')}")
        else:
            log.warning(f"mensaje no-JSON recibido (len={len(str(v))}), requiere bridge")


if __name__ == "__main__":
    run()
