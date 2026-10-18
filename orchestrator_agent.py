
import os
import json
import logging
from kafka import KafkaConsumer, KafkaProducer

log = logging.getLogger("orchestrator_agent")
BOOTSTRAP_SERVERS = "localhost:9092"

def run():
    log.info("Orchestrator Agent starting...")
    consumer = KafkaConsumer(
        "orchestrator.requests",
        bootstrap_servers=BOOTSTRAP_SERVERS,
        group_id="orchestrator-cg",
        value_deserializer=lambda m: json.loads(m.decode('utf-8'))
    )
    producer = KafkaProducer(
        bootstrap_servers=BOOTSTRAP_SERVERS,
        value_serializer=lambda v: json.dumps(v).encode('utf-8')
    )

    for msg in consumer:
        data = msg.value
        req_id = data.get("request_id")
        log.info(f"[ORCHESTRATOR] Received new study: {req_id}")
        
        # Forward to ETL Agent
        producer.send("etl.requests", value=data)
        producer.flush()
        log.info(f"[ORCHESTRATOR] Dispatched {req_id} to ETL Agent.")
