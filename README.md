# Neurometrix Agents

**Microservice for clinical AI orchestration on AMD ROCm.**

Standalone, event-driven Python microservice that consumes numeric features from the Neurometrix math cores (via Kafka), runs a **6-agent pipeline** with **grounded generation**, and produces auditable clinical reports.

---

## Architecture

Decoupled via Apache Kafka. Each agent is an independent consumer/producer. The pipeline is a directed acyclic graph where every hop is audited.

```text
core-X.results ──► [classifier] ──► agents.classified
                                        │
                                        ▼
                                    [rag] ──► agents.retrieved
                                                  │
                                                  ▼
                                           [specialist] ──► agents.analyzed
                                                              │
                                                              ▼
                                                        [reporter] ──► agents.reports.draft
                                                                        │
                                                                        ▼
                                                                  [auditor] ──► agents.reports.final
                                                                                  │
                                                                                  ▼
                                                                            PostgreSQL
                                                                            (agent_reports)

Frontend (Streamlit) / API ◄──► [interface] ◄──► agents.chat
The 6 Agents
#	Agent	Model	Role	Responsible AI
1	classifier	Gemma 4 31B	Deterministic scoring + LLM explanation	Never decides, only explains
2	rag	Gemma 4 31B + pgvector	Retrieval + rerank + query expansion	Every doc cites source
3	specialist	MedGemma 1.5 4B	Assisted diagnosis with differential	Never definitive, always differential
4	reporter	Gemma 4 31B	Structured clinical report	Zero hallucination, mandatory notice
5	auditor	Gemma 4 31B	Traceability + coherence verification	Final gate before human
6	interface	Gemma 4 31B	Chat with human specialist	Full transparency, escalate by default
Responsible AI
Every agent has:

Formal role (defined in roles.py): purpose, permissions, prohibitions, system prompt.

Confidence score persisted per request (agent_confidence table).

Evidence trail (numerical features, RAG docs, rules triggered).

Transaction log (agent_transactions table) — every Kafka hop is audited.

The auditor rejects reports that contain:

Numbers not present in the original features.

Citations not present in the RAG knowledge base.

Assertive language in diagnosis.

Event Contracts
Input Topics (from math cores)
core-4d.results — VolumeFeatures (HU stats, perfusion, entropy, vesselness, asymmetry).

Internal Topics (agent pipeline)
agents.classified — ClassifiedFeatures

agents.retrieved — RetrievedContext

agents.analyzed — DiagnosedReport

agents.reports.draft — DraftReport

agents.reports.final — FinalReport

agents.chat — Bidirectional chat messages

Database Schema (PostgreSQL 18 + pgvector)
Tables (all owned by this microservice):

requests — Master record per request_id

agent_reports — Final clinical reports

agent_confidence — Per-agent confidence + evidence

agent_transactions — Event audit trail

chat_messages — Interface conversations

knowledge_chunks — RAG vectors (pgvector, 384-dim)

Views:

v_agent_reports_final

v_request_confidence

v_request_trace

v_chat_recent

Migrate: psql -f sql/V1__agent_reports.sql (and V2, V3, V4).

Quickstart (Local)
bash
# 1. Install dependencies
pip install -r requirements.txt

# 2. Configure environment
cp .env.example .env
# Edit .env with Kafka, Postgres, and LLM endpoints

# 3. Migrate database
psql "$DATABASE_URL" -f sql/V1__agent_reports.sql
psql "$DATABASE_URL" -f sql/V2__agent_tracing.sql
psql "$DATABASE_URL" -f sql/V3__knowledge_pgvector.sql
psql "$DATABASE_URL" -f sql/V4__chat.sql

# 4. Build RAG index
python -m rag.indexer

# 5. Start the 6 agents
python orchestrator.py

# 6. (Optional) Start frontend
streamlit run frontend/app.py
Quickstart (AMD Developer Cloud)
bash
# On the AMD Jupyter Notebook:
unzip agents.zip -d /tmp/agents
cd /tmp/agents
bash bootstrap_amd.sh
The bootstrap script downloads Gemma 4 31B and MedGemma 1.5 4B, starts two vLLM servers on AMD ROCm (--attention-backend TRITON_ATTN), and opens two Cloudflare tunnels. Point your local .env to the printed URLs:

env
LLM_BASE_URL=https://xxx.trycloudflare.com/v1
SPECIALIST_LLM_BASE_URL=https://yyy.trycloudflare.com/v1
Environment Variables
See .env.example. Key variables:

Variable	Purpose
KAFKA_BOOTSTRAP_SERVERS	Kafka broker address
DATABASE_URL	PostgreSQL + pgvector connection string
LLM_BASE_URL	Primary LLM endpoint (Gemma 4 31B)
SPECIALIST_LLM_BASE_URL	Secondary LLM endpoint (MedGemma 1.5 4B)
RAG_INITIAL_RETRIEVE	Candidates before rerank (default 15)
RAG_RERANK_TOP_K	Final docs after rerank (default 5)
Stack
Runtime: Python 3.13

Event Bus: Apache Kafka (kafka-python)

Database: PostgreSQL 18 + pgvector 0.8.5

Inference: vLLM on AMD ROCm (MI300X)

Models: Gemma 4 31B (5 agents) + MedGemma 1.5 4B (specialist)

UI: Streamlit

Embeddings: sentence-transformers (all-MiniLM-L6-v2, 384-dim)

Powered by AMD
AMD Developer Cloud (MI300X, 192 GB VRAM)

ROCm 6.x

vLLM with --attention-backend TRITON_ATTN

Both Gemma 4 31B and MedGemma 1.5 4B served simultaneously (~70 GB VRAM)

Responsible AI Disclaimer
This is a clinical decision support tool, not a diagnostic device. Every report requires review by a licensed medical specialist. The system never emits definitive diagnoses; it provides probabilistic hypotheses with full traceability.

License
MIT