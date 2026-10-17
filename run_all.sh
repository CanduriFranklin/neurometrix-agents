#!/bin/bash
# agents/run_all.sh - arranca el orquestador
set -e

source .env 2>/dev/null || true
mkdir -p logs reports

echo "=== Pre-flight checks ==="

curl -sf http://localhost:8000/v1/models > /dev/null && echo "  vLLM OK" || (echo "  ERROR: vLLM no responde" && exit 1)

python -c "from storage import get_producer; get_producer(); print('  Kafka OK')" || (echo "  ERROR: Kafka no responde" && exit 1)

python -c "import os, psycopg2; c = psycopg2.connect(os.environ['DATABASE_URL']); c.close(); print('  Postgres OK')" || echo "  WARN: Postgres no responde"

echo ""
echo "  Arrancando orquestador (5 agentes)"
echo ""

python orchestrator.py 2>&1 | tee logs/orchestrator.log