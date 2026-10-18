"""Configuración común para los 5 agentes. Lee de .env."""
from __future__ import annotations

import os
from pathlib import Path
from dotenv import load_dotenv

HERE = Path(__file__).parent
load_dotenv(HERE / ".env")


def _get(key: str, default: str = "") -> str:
    return os.getenv(key, default)


def _get_int(key: str, default: int = 0) -> int:
    try:
        return int(os.getenv(key, str(default)))
    except ValueError:
        return default


def _get_float(key: str, default: float = 0.0) -> float:
    try:
        return float(os.getenv(key, str(default)))
    except ValueError:
        return default


# ---- Kafka ----
KAFKA_BOOTSTRAP_SERVERS = _get("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
KAFKA_TOPIC_RESULTS       = _get("KAFKA_TOPIC_RESULTS", "core-4d.results")
KAFKA_TOPIC_CLASSIFIED    = _get("KAFKA_TOPIC_CLASSIFIED", "agents.classified")
KAFKA_TOPIC_RETRIEVED     = _get("KAFKA_TOPIC_RETRIEVED", "agents.retrieved")
KAFKA_TOPIC_ANALYZED      = _get("KAFKA_TOPIC_ANALYZED", "agents.analyzed")
KAFKA_TOPIC_DRAFT         = _get("KAFKA_TOPIC_DRAFT", "agents.reports.draft")
KAFKA_TOPIC_FINAL         = _get("KAFKA_TOPIC_FINAL", "agents.reports.final")
KAFKA_CONSUMER_GROUP      = _get("KAFKA_CONSUMER_GROUP", "agents-cg")
KAFKA_AUTO_OFFSET_RESET   = _get("KAFKA_AUTO_OFFSET_RESET", "latest")

# ---- Postgres ----
DATABASE_URL = _get("DATABASE_URL", "")

# ---- LLM ----
LLM_BASE_URL    = _get("LLM_BASE_URL", "http://localhost:8000/v1")
LLM_API_KEY     = _get("LLM_API_KEY", "not-needed")
LLM_MODEL       = _get("LLM_MODEL", "Qwen/Qwen2.5-14B-Instruct")
LLM_TEMPERATURE = _get_float("LLM_TEMPERATURE", 0.1)
LLM_MAX_TOKENS  = _get_int("LLM_MAX_TOKENS", 1024)

# ---- LLM especialista (MedGemma 1.5 4B en puerto 8001) ----
SPECIALIST_LLM_BASE_URL = _get("SPECIALIST_LLM_BASE_URL", LLM_BASE_URL)
SPECIALIST_LLM_API_KEY  = _get("SPECIALIST_LLM_API_KEY", LLM_API_KEY)
SPECIALIST_LLM_MODEL    = _get("SPECIALIST_LLM_MODEL", "google/medgemma-1.5-4b-it")

# ---- Kafka topic para chat ----
KAFKA_TOPIC_CHAT = _get("KAFKA_TOPIC_CHAT", "agents.chat")

# ---- RAG ----
CHROMA_PATH   = str(HERE / _get("CHROMA_PATH", "chroma_db"))
EMBED_MODEL   = _get("EMBED_MODEL", "sentence-transformers/all-MiniLM-L6-v2")
KNOWLEDGE_DIR = str(HERE / _get("KNOWLEDGE_DIR", "knowledge"))

# ---- RAG avanzado ----
RAG_RERANK_TOP_K     = _get_int("RAG_RERANK_TOP_K", 5)
RAG_INITIAL_RETRIEVE = _get_int("RAG_INITIAL_RETRIEVE", 15)

# ---- Runtime ----
LOG_LEVEL    = _get("LOG_LEVEL", "INFO").upper()
REPORTS_DIR  = str(HERE / _get("REPORTS_DIR", "reports"))
LOGS_DIR     = str(HERE / "logs")

for p in (LOGS_DIR, REPORTS_DIR, CHROMA_PATH):
    os.makedirs(p, exist_ok=True)
