#!/bin/bash
# ============================================================
# agents/bootstrap_amd.sh
# AMD Developer Cloud - Full stack bootstrap
# ============================================================
set -e

echo "============================================================"
echo "  Neurometrix - AMD Notebook Bootstrap"
echo "============================================================"

mkdir -p /tmp/models logs reports

# ============================================================
# [1/5] Python dependencies
# ============================================================
echo ""
echo "[1/5] Installing Python dependencies..."
pip install -q -r requirements.txt

# ============================================================
# [2/5] Download models
# ============================================================
echo ""
echo "[2/5] Downloading Gemma 4 31B..."
if [ ! -d "/tmp/models/gemma-4-31b" ]; then
    huggingface-cli download google/gemma-4-31B-it \
        --local-dir /tmp/models/gemma-4-31b \
        > logs/hf_gemma4.log 2>&1
fi

echo ""
echo "[2/5] Downloading MedGemma 1.5 4B..."
if [ ! -d "/tmp/models/medgemma" ]; then
    huggingface-cli download google/medgemma-1.5-4b-it \
        --local-dir /tmp/models/medgemma \
        > logs/hf_medgemma.log 2>&1
fi

# ============================================================
# [3/5] Start vLLM servers
# ============================================================
echo ""
echo "[3/5] Starting vLLM Gemma 4 (port 8000)..."
if [ -f logs/vllm_gemma4.pid ] && kill -0 $(cat logs/vllm_gemma4.pid) 2>/dev/null; then
    echo "  already running (pid $(cat logs/vllm_gemma4.pid))"
else
    nohup python -m vllm.entrypoints.openai.api_server \
        --model /tmp/models/gemma-4-31b \
        --port 8000 --host 0.0.0.0 \
        --max-model-len 32768 \
        --gpu-memory-utilization 0.55 \
        --attention-backend TRITON_ATTN \
        > logs/vllm_gemma4.log 2>&1 &
    echo $! > logs/vllm_gemma4.pid
fi

echo ""
echo "[3/5] Starting vLLM MedGemma (port 8001)..."
if [ -f logs/vllm_medgemma.pid ] && kill -0 $(cat logs/vllm_medgemma.pid) 2>/dev/null; then
    echo "  already running (pid $(cat logs/vllm_medgemma.pid))"
else
    nohup python -m vllm.entrypoints.openai.api_server \
        --model /tmp/models/medgemma \
        --port 8001 --host 0.0.0.0 \
        --max-model-len 16384 \
        --gpu-memory-utilization 0.30 \
        --attention-backend TRITON_ATTN \
        > logs/vllm_medgemma.log 2>&1 &
    echo $! > logs/vllm_medgemma.pid
fi

# ============================================================
# [4/5] Wait for both vLLMs
# ============================================================
echo ""
echo "[4/5] Waiting for vLLM endpoints..."
for port in 8000 8001; do
    for i in $(seq 1 120); do
        if curl -sf http://localhost:$port/v1/models > /dev/null 2>&1; then
            echo "  port $port ready (attempt $i)"
            break
        fi
        sleep 5
    done
done

# ============================================================
# [5/5] Cloudflare tunnels
# ============================================================
echo ""
echo "[5/5] Starting Cloudflare tunnels..."
if [ ! -f /tmp/cloudflared ]; then
    wget -q https://github.com/cloudflare/cloudflared/releases/latest/download/cloudflared-linux-amd64 -O /tmp/cloudflared
    chmod +x /tmp/cloudflared
fi

nohup /tmp/cloudflared tunnel --url http://localhost:8000 > logs/tunnel_gemma4.log 2>&1 &
nohup /tmp/cloudflared tunnel --url http://localhost:8001 > logs/tunnel_medgemma.log 2>&1 &
sleep 15

echo ""
echo "============================================================"
echo "  Gemma 4 31B URL:"
grep -o 'https://[a-z0-9-]*\.trycloudflare\.com' logs/tunnel_gemma4.log | head -1
echo ""
echo "  MedGemma URL:"
grep -o 'https://[a-z0-9-]*\.trycloudflare\.com' logs/tunnel_medgemma.log | head -1
echo "============================================================"
echo ""
echo "Next: build RAG index and start agents"
echo "  python -m rag.indexer"
echo "  python orchestrator.py"
echo ""