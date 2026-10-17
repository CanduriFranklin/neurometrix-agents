#!/bin/bash
# agents/bootstrap.sh - AMD Notebook setup
set -e

echo "=== [1/6] Instalando dependencias Python ==="
pip install -q -r requirements.txt

echo "=== [2/6] Descargando Qwen2.5-14B-Instruct ==="
mkdir -p /tmp/models logs
if [ ! -d "/tmp/models/qwen2.5-14b" ]; then
    huggingface-cli download Qwen/Qwen2.5-14B-Instruct --local-dir /tmp/models/qwen2.5-14b --exclude "*.bin" "*.h5" "*.msgpack" "*.ot" > logs/hf_download.log 2>&1
fi

echo "=== [3/6] Levantando vLLM (OpenAI-compatible) ==="
if [ -f logs/vllm.pid ] && kill -0 $(cat logs/vllm.pid) 2>/dev/null; then
    echo "  vLLM ya corriendo (pid $(cat logs/vllm.pid))"
else
    nohup python -m vllm.entrypoints.openai.api_server --model /tmp/models/qwen2.5-14b --port 8000 --host 0.0.0.0 --max-model-len 8192 --gpu-memory-utilization 0.90 > logs/vllm.log 2>&1 &
    echo $! > logs/vllm.pid
fi

echo "=== [4/6] Esperando a vLLM ==="
for i in $(seq 1 90); do
    if curl -sf http://localhost:8000/v1/models > /dev/null 2>&1; then
        echo "  vLLM listo en intento $i"
        break
    fi
    sleep 5
done

echo "=== [5/6] Construyendo indice RAG ==="
python -m rag.indexer

echo "=== [6/6] Verificando Kafka + Postgres ==="
python -c "from storage import get_producer, pg_conn; get_producer(); c=pg_conn(); c.__enter__(); print('  Kafka + Postgres OK')" || echo "  WARN: Kafka o Postgres no accesible"

echo ""
echo "  LISTO. Siguiente: bash run_all.sh"