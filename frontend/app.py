
import streamlit as st
import os
import sys
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from config import LLM_BASE_URL, LLM_API_KEY

import uuid
import time
import json
import psycopg2
from kafka import KafkaProducer
from openai import OpenAI

DB_URL = "postgresql://neurometrix:Enginer012%25@localhost:5432/neurometrix_core_4d"
KAFKA_SERVERS = "localhost:9092"
RAW_DIR = r"C:\Users\candu\source\repos\Mathematical_Neurometrix\data\raw"

st.set_page_config(page_title="Neurometrix | AMD ROCm", page_icon="🧠", layout="wide")

# --- UI HEADER ---
logo_path = os.path.join(os.path.dirname(__file__), "logo.png")
if os.path.exists(logo_path):
    st.image(logo_path, width=250)

st.markdown("""
    <div style="text-align: center; margin-bottom: 20px;">
        <h1 style="color: #E31837; font-weight: 800; font-size: 3.5rem; margin-bottom: 0;">NEUROMETRIX</h1>
        <p style="color: #333; font-size: 1.2rem;">Powered by <b>AMD ROCm</b> & <b>Google Gemma</b></p>
    </div>
""", unsafe_allow_html=True)

# Estado global
if "active_case_id" not in st.session_state:
    st.session_state.active_case_id = None

tab_upload, tab_cases, tab_chat, tab_status = st.tabs(["Upload Study", "Case Review", "Chat with AI", "System Status"])

def get_db_connection():
    return psycopg2.connect(DB_URL)

def get_all_cases():
    try:
        conn = get_db_connection(); cur = conn.cursor()
        # Buscamos los casos. En un sistema real, filtraríamos por user_id
        cur.execute("SELECT request_id, confidence FROM agent_reports ORDER BY created_at DESC LIMIT 20;")
        reports = cur.fetchall(); cur.close(); conn.close()
        return reports
    except: return []

# =====================================================================
# TAB 1: UPLOAD
# =====================================================================
with tab_upload:
    st.subheader("Upload a new imaging study")
    uploaded_files = st.file_uploader("Select DICOM files", accept_multiple_files=True)
    user_id = st.text_input("User ID", value="physician@clinic.com")

    if st.button("Submit Study", type="primary"):
        if uploaded_files:
            request_id = str(uuid.uuid4())
            upload_dir = os.path.join(RAW_DIR, request_id)
            os.makedirs(upload_dir, exist_ok=True)
            for f in uploaded_files:
                with open(os.path.join(upload_dir, f.name), "wb") as out:
                    out.write(f.getbuffer())
            with open(os.path.join(upload_dir, "metadata.json"), "w") as meta:
                json.dump({"request_id": request_id, "user_id": user_id, "timestamp": time.time()}, meta)
            try:
                producer = KafkaProducer(bootstrap_servers=KAFKA_SERVERS, value_serializer=lambda v: json.dumps(v).encode('utf-8'))
                producer.send('orchestrator.requests', value={'request_id': request_id, 'raw_path': upload_dir.replace("\\", "/"), 'user_id': user_id})
                producer.flush(); producer.close()
                st.success(f"Study submitted! Request ID: `{request_id}`")
            except Exception as e:
                st.error(f"Kafka Error: {e}")

# =====================================================================
# TAB 2: CASE REVIEW
# =====================================================================
with tab_cases:
    st.subheader("Clinical Case Review")
    reports = get_all_cases()
    if not reports:
        st.info("No cases available.")
    else:
        options = [f"{r[0][:8]}... | Conf: {r[1]:.2f}" for r in reports]
        selected = st.selectbox("Select a case:", options)
        if selected:
            st.session_state.active_case_id = reports[options.index(selected)][0]
            try:
                conn = get_db_connection(); cur = conn.cursor()
                cur.execute("SELECT final_markdown FROM agent_reports WHERE request_id = %s", (st.session_state.active_case_id,))
                report_text = cur.fetchone()[0]; cur.close(); conn.close()
                st.markdown(report_text)
            except: st.error("Report not found.")
            
            st.markdown("---")
            st.subheader("Medical Board Decision")
            decision = st.radio("Decision:", ["Approve", "Reject", "Request Support"])
            description = st.text_area("Justification (Required):")
            if st.button("Submit Decision"):
                if description.strip():
                    try:
                        conn = get_db_connection(); cur = conn.cursor()
                        status_map = {"Approve": "APPROVED", "Reject": "REJECTED", "Request Support": "TECHNICAL_SUPPORT"}
                        cur.execute("INSERT INTO medical_reviews (request_id, decision, justification) VALUES (%s, %s, %s);", (st.session_state.active_case_id, status_map[decision], description))
                        cur.execute("UPDATE agent_reports SET status = %s WHERE request_id = %s;", (status_map[decision], st.session_state.active_case_id))
                        conn.commit(); cur.close(); conn.close()
                        st.success("Decision recorded successfully!")
                    except Exception as e: st.error(f"DB Error: {e}")

# =====================================================================
# TAB 3: CHAT WITH AI (Con selector de contexto integrado)
# =====================================================================
with tab_chat:
    
    st.subheader("Consult the Neurometrix AI")
    
    # Botón para limpiar la memoria y que no salgan respuestas viejas
    if st.button("Reset AI Context"):
        st.session_state.chat_history = [{"role": "assistant", "content": "Context loaded. How can I assist you with this patient?"}]
        st.rerun()
    
    
    # --- Selector de contexto dentro del mismo Chat ---
    reports = get_all_cases()
    if not reports:
        st.warning("No clinical cases found in the database. Please upload a study first.")
    else:
        options = [f"Case {r[0][:8]}... | Conf: {r[1]:.2f}" for r in reports]
        # Buscamos el índice del caso activo actual para que el dropdown lo muestre seleccionado
        default_index = 0
        if st.session_state.active_case_id:
            for i, r in enumerate(reports):
                if r[0] == st.session_state.active_case_id:
                    default_index = i
                    break
                    
        selected_case = st.selectbox("Select patient context for this chat:", options, index=default_index)
        st.session_state.active_case_id = reports[options.index(selected_case)][0]
        st.caption(f"Active Context: Analyzing Request ID `{st.session_state.active_case_id}`")
    
    st.markdown("---")

    # --- Historial de chat ---
    if "chat_history" not in st.session_state:
        st.session_state.chat_history = [{"role": "assistant", "content": "Context loaded. How can I assist you with this patient?"}]
    
    # Si cambiamos de caso, reiniciamos el chat
    if "last_case" not in st.session_state:
        st.session_state.last_case = st.session_state.active_case_id
    elif st.session_state.last_case != st.session_state.active_case_id:
        st.session_state.chat_history = [{"role": "assistant", "content": "Context changed. I have loaded the new patient's report. How can I help?"}]
        st.session_state.last_case = st.session_state.active_case_id

    for msg in st.session_state.chat_history:
        st.chat_message(msg["role"]).write(msg["content"])
        
    if prompt := st.chat_input("Ask about this patient..."):
        st.session_state.chat_history.append({"role": "user", "content": prompt})
        st.chat_message("user").write(prompt)
        
        # 1. Obtener contexto del caso activo
        context = "No clinical report found for the active case."
        if st.session_state.active_case_id:
            try:
                conn = get_db_connection(); cur = conn.cursor()
                cur.execute("SELECT final_markdown FROM agent_reports WHERE request_id = %s;", (st.session_state.active_case_id,))
                row = cur.fetchone()
                if row: context = row[0]
                cur.close(); conn.close()
            except: pass

        # 2. Streaming
        with st.chat_message("assistant"):
            try:
                client = OpenAI(base_url=LLM_BASE_URL, api_key=LLM_API_KEY)
                stream = client.chat.completions.create(
                    model="Gemma-4-31B-IT",
                    messages=[{"role": "system", "content": f"You are a Neurologist AI. Answer based on this report: {context}"}, {"role": "user", "content": prompt}],
                    stream=True
                )
                
                # Generador robusto que ignora cortes de red al final
                def stream_generator():
                    try:
                        for chunk in stream:
                            if chunk.choices and chunk.choices[0].delta and chunk.choices[0].delta.content:
                                yield chunk.choices[0].delta.content
                    except Exception as e:
                        print(f"Stream finished with minor network error: {e}")

                response_text = st.write_stream(stream_generator())
                
            except Exception as e:
                # Solo muestra error si falló el inicio de la conexión
                response_text = "I apologize, Doctor. There was an issue connecting to the AMD Gemma API. Please try again."
                st.error(response_text)

        st.session_state.chat_history.append({"role": "assistant", "content": response_text})
        
        # 3. Guardar en DB
        try:
            conn = get_db_connection(); cur = conn.cursor()
            cur.execute("INSERT INTO chat_messages (request_id, role, content) VALUES (%s, 'assistant', %s);", (st.session_state.active_case_id, response_text))
            conn.commit(); cur.close(); conn.close()
        except: pass

# =====================================================================
# TAB 4: STATUS
# =====================================================================
with tab_status:
    st.subheader("System Telemetry")
    st.info("All microservices running.")
