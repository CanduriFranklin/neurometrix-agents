"""End-to-end test - publish a synthetic volume to Kafka and verify the pipeline."""
import json
import time
import uuid
import sys
import os

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from kafka import KafkaProducer

TOPIC = os.getenv("KAFKA_TOPIC_RESULTS", "core-4d.results")
KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")

request_id = str(uuid.uuid4())
user_id = "e2e-test-user"

payload = {
    "request_id": request_id,
    "user_id": user_id,
    "message_id": int(time.time() * 1000),
    "study_uid": "1.2.826.0.1.3680043.10.999.E2E.TEST",
    "status": "processed",
    "height": 512, "width": 512, "depth": 48, "timepoints": 20,
    "is_4d": True,
    "mean_hu": 42.5, "stddev_hu": 18.3, "min_hu": -1000.0, "max_hu": 800.0,
    "mean_entropy": 3.8, "peak_vesselness": 0.42, "asymmetry_score": 28.5,
    "total_volume_mm3": 12582912.0,
    "mean_cbf": 22.5, "mean_cbv": 2.1, "mean_mtt": 8.3,
    "mean_ttp": 12.1, "mean_tmax": 9.2,
    "core_voxels": 450, "penumbra_voxels": 1200, "mismatch_ratio": 2.67,
    "findings": [
        {"type": "acute_hemorrhage", "confidence": 0.85, "mean_hu": 65.0},
        {"type": "calcification", "confidence": 1.0, "mean_hu": 250.0},
    ],
    "processing_ms": 448,
    "gpu_used": False,
    "error_detail": "",
}

p = KafkaProducer(
    bootstrap_servers=KAFKA_BOOTSTRAP,
    value_serializer=lambda v: json.dumps(v).encode("utf-8"),
    acks="all",
)
p.send(TOPIC, payload).get(timeout=10)
p.flush()
p.close()

print(f"Published to {TOPIC}")
print(f"  request_id: {request_id}")
print(f"  user_id: {user_id}")
print()
print("Pipeline should complete in ~30-60 seconds:")
print("  classifier -> rag -> specialist -> reporter -> auditor")
print()
print("Verify with:")
print(f"  psql -c \"SELECT request_id, status, pipeline_confidence FROM requests WHERE request_id='{request_id}';\"")
