
import os
import json
import logging
import psycopg2
from kafka import KafkaConsumer
from openai import OpenAI

log = logging.getLogger("interface")

# Configuration
KAFKA_SERVERS = "localhost:9092"
DB_URL = "postgresql://neurometrix:Enginer012%25@localhost:5432/neurometrix_core_4d"
from config import LLM_BASE_URL as AMD_API_BASE
from config import LLM_API_KEY as AMD_API_KEY

def get_db_connection():
    return psycopg2.connect(DB_URL)

def get_latest_report_context():
    """Fetches the most recent clinical report to give context to the LLM."""
    try:
        conn = get_db_connection()
        cur = conn.cursor()
        cur.execute("SELECT final_markdown FROM agent_reports ORDER BY created_at DESC LIMIT 1;")
        row = cur.fetchone()
        cur.close()
        conn.close()
        return row[0] if row else "No previous clinical reports found in the database."
    except Exception as e:
        log.error(f"DB Error fetching context: {e}")
        return "Database context unavailable."

def save_response_to_db(response_text):
    """Saves the AI response to PostgreSQL so the Streamlit UI can read it."""
    try:
        conn = get_db_connection()
        cur = conn.cursor()
        cur.execute("INSERT INTO chat_messages (role, content) VALUES ('assistant', %s);", (response_text,))
        conn.commit()
        cur.close()
        conn.close()
    except Exception as e:
        log.error(f"DB Error saving response: {e}")

def generate_llm_response(user_message, context):
    """Calls AMD API (Gemma) with a graceful fallback."""
    try:
        client = OpenAI(base_url=AMD_API_BASE, api_key=AMD_API_KEY)
        response = client.chat.completions.create(
            model="Gemma-4-31B-IT",
            messages=[
                {"role": "system", "content": f"You are the Neurometrix Clinical AI. Answer the doctor's question based strictly on this medical report context:\n\n{context}"},
                {"role": "user", "content": user_message}
            ],
            max_tokens=400,
            timeout=10
        )
        return response.choices[0].message.content
    except Exception as e:
        log.warning(f"LLM API Error (using fallback): {e}")
        # Fallback if API key fails or AMD service is down
        snippet = context[:600] + "...\n\n[AI Note: This is a local summary. Please review the full report in the Case Review tab.]"
        return f"Based on the latest clinical report:\n\n{snippet}"

def run():
    log.info("Interface Agent (Chat) starting...")
    consumer = KafkaConsumer(
        "agents.chat",
        bootstrap_servers=KAFKA_SERVERS,
        group_id="agents-cg-interface",
        value_deserializer=lambda m: json.loads(m.decode('utf-8'))
    )

    for msg in consumer:
        data = msg.value
        user_message = data.get("message")
        log.info(f"Received chat from UI: {user_message}")
        
        # 1. Get Context
        context = get_latest_report_context()
        
        # 2. Generate Response
        response_text = generate_llm_response(user_message, context)
        
        # 3. Save to DB for UI polling
        save_response_to_db(response_text)
        log.info("Response generated and saved to database.")
