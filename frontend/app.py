"""Neurometrix Frontend - Streamlit dashboard for clinical specialists."""
from __future__ import annotations

import os
import sys
import uuid
import json
import time
from datetime import datetime

import streamlit as st

# Add agents/ to sys.path so we can import shared modules
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from kafka import KafkaProducer
from storage import pg_fetchall, pg_execute

# ============================================================
# Config
# ============================================================
KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
TOPIC_REQUESTS  = os.getenv("KAFKA_TOPIC_RESULTS", "core-4d.results")
TOPIC_CHAT      = os.getenv("KAFKA_TOPIC_CHAT", "agents.chat")

st.set_page_config(
    page_title="Neurometrix - Clinical AI Assistant",
    page_icon="🧠",
    layout="wide",
)

# ============================================================
# Helpers
# ============================================================
def publish(topic: str, payload: dict):
    p = KafkaProducer(
        bootstrap_servers=KAFKA_BOOTSTRAP,
        value_serializer=lambda v: json.dumps(v, default=str).encode("utf-8"),
        acks="all",
    )
    p.send(topic, payload)
    p.flush()
    p.close()


def list_requests(limit: int = 20):
    try:
        return pg_fetchall("""
            SELECT request_id, user_id, study_uid, status,
                   pipeline_confidence, created_at
            FROM requests
            ORDER BY created_at DESC
            LIMIT %s
        """, (limit,))
    except Exception as e:
        st.error(f"DB error: {e}")
        return []


def get_report(request_id: str):
    try:
        rows = pg_fetchall("""
            SELECT primary_hypothesis, analysis, impression, differential,
                   confidence, audit_passed, audit_findings, confidence_score,
                   final_markdown, created_at
            FROM agent_reports WHERE request_id = %s
            ORDER BY id DESC LIMIT 1
        """, (request_id,))
        return dict(rows[0]) if rows else None
    except Exception as e:
        st.error(f"DB error: {e}")
        return None


def get_confidences(request_id: str):
    try:
        return pg_fetchall("""
            SELECT agent_name, confidence, latency_ms, model_used, tokens_used
            FROM agent_confidence WHERE request_id = %s
            ORDER BY id
        """, (request_id,))
    except Exception:
        return []


def get_chat_history(request_id: str):
    try:
        return pg_fetchall("""
            SELECT role, content, cited_sources, confidence, created_at
            FROM chat_messages WHERE request_id = %s
            ORDER BY created_at ASC
        """, (request_id,))
    except Exception:
        return []


# ============================================================
# Header
# ============================================================
st.title("🧠 Neurometrix")
st.caption("Clinical AI Assistant for Neurological Imaging — Powered by AMD ROCm")

tab_upload, tab_cases, tab_chat = st.tabs(["📤 Upload Study", "📋 Case Review", "💬 Chat with AI"])

# ============================================================
# Tab 1 — Upload
# ============================================================
with tab_upload:
    st.subheader("Upload a new imaging study")
    st.markdown("Supported: CT/MR in DICOM format (2D, 3D, or 4D).")

    col1, col2 = st.columns([2, 1])
    with col1:
        uploaded = st.file_uploader(
            "Select DICOM files",
            type=["dcm", "DCM", "zip"],
            accept_multiple_files=True,
        )
    with col2:
        user_id = st.text_input("Your user ID", value="physician@clinic.com")
        study_uid = st.text_input("Study UID (optional)", value="")

    if st.button("🚀 Submit Study", type="primary", use_container_width=True):
        if not uploaded:
            st.warning("Please upload at least one file.")
        else:
            request_id = str(uuid.uuid4())
            uid = study_uid or f"auto-{request_id[:8]}"

            upload_dir = os.path.join(
                os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
                "data", "uploads", request_id,
            )
            os.makedirs(upload_dir, exist_ok=True)
            for f in uploaded:
                with open(os.path.join(upload_dir, f.name), "wb") as out:
                    out.write(f.read())

            payload = {
                "request_id": request_id,
                "user_id": user_id,
                "message_id": int(time.time() * 1000),
                "study_uid": uid,
                "dicom_path": upload_dir.replace("\\", "/"),
                "timestamp_utc": int(time.time() * 1000),
            }
            publish(TOPIC_REQUESTS, payload)

            st.success(f"Study submitted. Request ID: `{request_id}`")
            st.info(f"Track progress in the **Case Review** tab.")

# ============================================================
# Tab 2 — Case Review
# ============================================================
with tab_cases:
    st.subheader("Recent requests")

    if st.button("🔄 Refresh", use_container_width=False):
        st.rerun()

    reqs = list_requests()
    if not reqs:
        st.info("No requests yet. Upload a study to get started.")
    else:
        options = {f"{r['request_id'][:8]}… | {r['status']} | {r['study_uid'][:20]}": r['request_id']
                   for r in reqs}
        selected = st.selectbox("Select a request", list(options.keys()))

        if selected:
            rid = options[selected]
            report = get_report(rid)
            confs = get_confidences(rid)

            st.markdown(f"**Request ID:** `{rid}`")

            # Confidence summary
            if confs:
                st.markdown("### Agent Confidence Scores")
                cols = st.columns(len(confs))
                for col, c in zip(cols, confs):
                    col.metric(
                        c["agent_name"].title(),
                        f"{c['confidence']:.2f}",
                        delta=f"{c['latency_ms']}ms",
                    )

            # Report
            if report:
                st.markdown("### 📄 Final Report")
                if report["audit_passed"]:
                    st.success(f"✅ Audit passed (confidence: {report['confidence_score']:.2f})")
                else:
                    st.warning(f"⚠️ Audit flagged issues: {report['audit_findings']}")

                st.markdown(report["final_markdown"] or "_No report content_")

                st.download_button(
                    "📥 Download Report (Markdown)",
                    data=report["final_markdown"] or "",
                    file_name=f"report_{rid[:8]}.md",
                    mime="text/markdown",
                )
            else:
                st.info("Report not ready yet. Pipeline may still be processing.")

# ============================================================
# Tab 3 — Chat
# ============================================================
with tab_chat:
    st.subheader("💬 Ask the AI about a case")

    reqs = list_requests()
    if not reqs:
        st.info("No cases available. Upload a study first.")
    else:
        options = {f"{r['request_id'][:8]}… | {r['study_uid'][:20]}": r['request_id'] for r in reqs}
        selected = st.selectbox("Case", list(options.keys()), key="chat_case")

        if selected:
            rid = options[selected]

            # Show history
            history = get_chat_history(rid)
            for h in history:
                with st.chat_message(h["role"]):
                    st.markdown(h["content"])
                    if h.get("cited_sources"):
                        st.caption(f"Sources: {', '.join(h['cited_sources'])}")

            # Input
            question = st.chat_input("Ask about findings, confidence, evidence…")
            if question:
                with st.chat_message("user"):
                    st.markdown(question)

                payload = {
                    "request_id": rid,
                    "user_id": "physician@clinic.com",
                    "content": question,
                    "metadata": {"session_id": st.session_state.get("session_id", str(uuid.uuid4()))},
                }
                publish(TOPIC_CHAT, payload)

                st.info("Question sent. Wait for AI response…")
                time.sleep(3)
                st.rerun()
