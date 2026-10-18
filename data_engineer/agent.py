
import os
import time
import json
import shutil
import logging
import struct
import psycopg2
from kafka import KafkaConsumer, KafkaProducer

log = logging.getLogger("data_engineer")

BASE_DIR = r"C:\\Users\\candu\\source\\repos\\Mathematical_Neurometrix"
RAW_DIR = os.path.join(BASE_DIR, "data", "raw")
WORK_DIR = os.path.join(BASE_DIR, "data", "workspace")
CURATED_DIR = os.path.join(BASE_DIR, "data", "curated")
BOOTSTRAP_SERVERS = "localhost:9092"
DB_URL = "postgresql://neurometrix:Enginer012%25@localhost:5432/neurometrix_core_4d"

def run():
    log.info("Data Engineer Agent (ETL) starting...")
    consumer = KafkaConsumer(
        "etl.requests",
        bootstrap_servers=BOOTSTRAP_SERVERS,
        group_id="etl-cg",
        value_deserializer=lambda m: json.loads(m.decode('utf-8'))
    )
    
    # IMPORTANT: The C++ Core expects raw bytes, so NO JSON serializer here.
    producer = KafkaProducer(bootstrap_servers=BOOTSTRAP_SERVERS)

    for msg in consumer:
        data = msg.value
        req_id = data.get("request_id")
        raw_path = data.get("raw_path")
        user_id = data.get("user_id", "physician@clinic.com")

        if not os.path.exists(raw_path):
            log.error(f"[ETL] Path not found: {raw_path}")
            continue

        log.info(f"[ETL] Processing study: {req_id}")

        # 1. Move to Workspace
        ws_path = os.path.join(WORK_DIR, req_id)
        if not os.path.exists(ws_path):
            shutil.copytree(raw_path, ws_path)
            log.info(f"[ETL] Copied to Workspace: {ws_path}")

        time.sleep(1) 

        # 2. Move to Curated
        cur_path = os.path.join(CURATED_DIR, req_id)
        if not os.path.exists(cur_path):
            shutil.copytree(ws_path, cur_path)
            log.info(f"[ETL] Saved to Curated: {cur_path}")

        # 3. Save to DB
        try:
            conn = psycopg2.connect(DB_URL)
            cur = conn.cursor()
            cur.execute(
                "INSERT INTO ingestion_jobs (request_id, raw_path, workspace_path, curated_path, status) VALUES (%s, %s, %s, %s, 'CURATED') ON CONFLICT (request_id) DO UPDATE SET status = 'CURATED';",
                (req_id, raw_path.replace('\\', '/'), ws_path.replace('\\', '/'), cur_path.replace('\\', '/'))
            )
            conn.commit()
            cur.close()
            conn.close()
        except Exception:
            pass

        # 4. Send BINARY payload directly to C++ Core (neuro-4d.exe)
        clean_cur_path = cur_path.replace("\\", "/")
        mid = int(time.time() * 1000) % 1000000
        uid = f"auto-{req_id}"

        ub = uid.encode('utf-8')
        pb = clean_cur_path.encode('utf-8')

        # This is the exact struct the C++ Core expects
        payload = struct.pack('<Q', mid) + struct.pack('<Q', int(time.time()*1000)) + struct.pack('<I', len(ub)) + ub + struct.pack('<I', len(pb)) + pb

        producer.send("core-4d.requests", value=payload)
        producer.flush()
        log.info(f"[CORE] Dispatched {req_id} to C++ Core as BINARY STRUCT.")
