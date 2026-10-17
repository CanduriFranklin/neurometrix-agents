"""Persistencia Postgres + Kafka helpers comunes a todos los agentes."""
from __future__ import annotations

import json
import logging
from contextlib import contextmanager
from typing import Any, Dict, Optional

import psycopg2
import psycopg2.extras
from kafka import KafkaConsumer, KafkaProducer

from config import (
    KAFKA_BOOTSTRAP_SERVERS,
    KAFKA_AUTO_OFFSET_RESET,
    DATABASE_URL,
    LOG_LEVEL,
)

# ---- Logging ----
logging.basicConfig(
    level=getattr(logging, LOG_LEVEL, logging.INFO),
    format="[%(asctime)s] [%(name)s] [%(levelname)s] %(message)s",
)


def get_logger(name: str) -> logging.Logger:
    return logging.getLogger(name)


# ---- Postgres ----
@contextmanager
def pg_conn():
    conn = psycopg2.connect(DATABASE_URL)
    try:
        yield conn
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


def pg_execute(sql: str, params: Optional[tuple] = None) -> None:
    with pg_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(sql, params or ())


def pg_fetchall(sql: str, params: Optional[tuple] = None) -> list:
    with pg_conn() as conn:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute(sql, params or ())
            return cur.fetchall()


# ---- Kafka Producer ----
_producer: Optional[KafkaProducer] = None


def get_producer() -> KafkaProducer:
    global _producer
    if _producer is None:
        _producer = KafkaProducer(
            bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
            value_serializer=lambda v: json.dumps(v, default=str).encode("utf-8"),
            acks="all",
        )
    return _producer


def publish(topic: str, payload: Dict[str, Any]) -> None:
    p = get_producer()
    p.send(topic, payload)
    p.flush()


# ---- Kafka Consumer ----
def make_consumer(topic: str, group_suffix: str) -> KafkaConsumer:
    from config import KAFKA_CONSUMER_GROUP
    return KafkaConsumer(
        topic,
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        group_id=f"{KAFKA_CONSUMER_GROUP}-{group_suffix}",
        auto_offset_reset=KAFKA_AUTO_OFFSET_RESET,
        value_deserializer=lambda b: json.loads(b.decode("utf-8")),
        enable_auto_commit=True,
    )


# ============================================================
# Helpers de trazabilidad (T1)
# ============================================================
def register_request(request_id, user_id,
                     study_uid="", message_id=0,
                     entry_point="kafka",
                     metadata=None):
    """Crea o actualiza la fila maestra en requests."""
    import json as _json
    pg_execute("""
        INSERT INTO requests
            (request_id, user_id, study_uid, message_id, entry_point, metadata, status)
        VALUES (%s, %s, %s, %s, %s, %s, 'received')
        ON CONFLICT (request_id) DO UPDATE SET
            updated_at = NOW()
    """, (
        request_id, user_id,
        study_uid, message_id,
        entry_point,
        _json.dumps(metadata or {}),
    ))


def update_request_status(request_id, status, pipeline_confidence=None):
    """Avanza el estado del request en el pipeline."""
    if pipeline_confidence is None:
        pg_execute("""
            UPDATE requests SET status = %s, updated_at = NOW()
            WHERE request_id = %s
        """, (status, request_id))
    else:
        pg_execute("""
            UPDATE requests
            SET status = %s, pipeline_confidence = %s, updated_at = NOW()
            WHERE request_id = %s
        """, (status, pipeline_confidence, request_id))


def log_confidence(request_id, agent_name, confidence, latency_ms=0,
                   evidence=None, flags=None,
                   model_used=None, tokens_used=0):
    """Registra el confidence score de un agente."""
    import json as _json
    try:
        pg_execute("""
            INSERT INTO agent_confidence
                (request_id, agent_name, confidence, latency_ms,
                 evidence, flags, model_used, tokens_used)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (request_id, agent_name) DO UPDATE SET
                confidence = EXCLUDED.confidence,
                latency_ms = EXCLUDED.latency_ms,
                evidence = EXCLUDED.evidence,
                flags = EXCLUDED.flags,
                model_used = EXCLUDED.model_used,
                tokens_used = EXCLUDED.tokens_used
        """, (
            request_id, agent_name, confidence, latency_ms,
            _json.dumps(evidence or []),
            _json.dumps(flags or []),
            model_used, tokens_used,
        ))
    except Exception as e:
        logging.getLogger("storage").warning(f"log_confidence fallo: {e}")


def log_transaction(request_id, from_agent, to_agent,
                    from_topic, to_topic,
                    payload_size=0, status="ok", error_detail=None):
    """Registra el salto entre dos agentes."""
    try:
        pg_execute("""
            INSERT INTO agent_transactions
                (request_id, from_agent, to_agent, from_topic, to_topic,
                 payload_size, status, error_detail)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
        """, (
            request_id, from_agent, to_agent,
            from_topic, to_topic,
            payload_size, status, error_detail,
        ))
    except Exception as e:
        logging.getLogger("storage").warning(f"log_transaction fallo: {e}")


def publish_with_trace(topic, payload,
                       from_agent=None, to_agent="unknown",
                       from_topic=None):
    """Publica en Kafka + registra la transacción con trazabilidad."""
    import json as _json
    publish(topic, payload)

    request_id = payload.get("request_id")
    if request_id:
        try:
            size = len(_json.dumps(payload, default=str).encode("utf-8"))
        except Exception:
            size = 0
        log_transaction(
            request_id=request_id,
            from_agent=from_agent,
            to_agent=to_agent,
            from_topic=from_topic,
            to_topic=topic,
            payload_size=size,
            status="ok",
        )
