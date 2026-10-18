# AI Architecture & Design Patterns

## 1. Event-Driven Microservices
The agents communicate asynchronously via Kafka. This decouples the heavy LLM inference from the C++ math processing.

## 2. The Swarm Topology
- Classifier: Uses Gemma to classify the pathology based on metrics.
- RAG (Retrieval-Augmented Generation): Uses pgvector to query medical literature based on findings.
- Specialist: The main diagnostic brain. Uses Google Gemma-4-31B-IT.
- Auditor: Double-checks the Specialist's report for logical inconsistencies.

## 3. Anti-Hallucination Pattern (Data Constraint)
The prompts injected into Gemma use a strict JSON schema:
System: You are a neurologist. The patient has CBF={cbf}, CBV={cbv}. Diagnose. Do not invent numbers.

## 4. Human-in-the-Loop (HITLL)
No agent has final authority. The Streamlit UI requires a physician's digital signature (saved in PostgreSUL) to approve the report.
