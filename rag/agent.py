"""RAG agent (pgvector + rerank LLM) - IA Responsable."""
from __future__ import annotations

import time
import json

from config import (
    KAFKA_TOPIC_CLASSIFIED,
    KAFKA_TOPIC_RETRIEVED,
    EMBED_MODEL,
    LLM_BASE_URL, LLM_API_KEY, LLM_MODEL, LLM_TEMPERATURE,
    RAG_RERANK_TOP_K, RAG_INITIAL_RETRIEVE,
)
from models import ClassifiedFeatures, RetrievedContext
from storage import (
    get_logger, make_consumer,
    update_request_status, log_confidence, publish_with_trace,
    pg_fetchall,
)

log = get_logger("rag.agent")

_model = None
_llm_client = None


def get_embedder():
    global _model
    if _model is None:
        from sentence_transformers import SentenceTransformer
        _model = SentenceTransformer(EMBED_MODEL)
        log.info(f"Embedder cargado: {EMBED_MODEL}")
    return _model


def get_llm():
    global _llm_client
    if _llm_client is None:
        from openai import OpenAI
        _llm_client = OpenAI(base_url=LLM_BASE_URL, api_key=LLM_API_KEY)
    return _llm_client


def build_query(c):
    """Query base para retrieval."""
    parts = [c.primary_hypothesis.replace("_", " "), " ".join(c.flags)]
    f = c.features
    if f.is_4d:
        parts.append(f"CBF {f.mean_cbf:.1f} CBV {f.mean_cbv:.2f} MTT {f.mean_mtt:.2f} TMax {f.mean_tmax:.2f}")
    if f.peak_vesselness > 0.15:
        parts.append("vascular calcification vesselness")
    if f.asymmetry_score > 15:
        parts.append("hemispheric asymmetry hemorrhage")
    return " ".join(parts)


def expand_query(c, base_query):
    """LLM expande la query para mejor retrieval semantico."""
    try:
        client = get_llm()
        prompt = f"""Reescribe esta consulta clinica para busqueda semantica en una base
de conocimiento medico. Solo agrega terminos tecnicos equivalentes, no cambies el sentido.

Consulta original: {base_query}

Responde SOLO con la consulta expandida (una linea)."""
        resp = client.chat.completions.create(
            model=LLM_MODEL,
            messages=[{"role": "user", "content": prompt}],
            temperature=0.0,
            max_tokens=150,
        )
        expanded = resp.choices[0].message.content.strip()
        return expanded if expanded else base_query
    except Exception as e:
        log.warning(f"query expansion fallo: {e}")
        return base_query


def retrieve(query, k):
    """Busqueda por similitud coseno en pgvector."""
    model = get_embedder()
    emb = model.encode(query, normalize_embeddings=True)
    vec_str = "[" + ",".join(f"{x:.6f}" for x in emb.tolist()) + "]"
    rows = pg_fetchall("""
        SELECT id, doc_source, doc_header, chunk_text,
               1 - (embedding <=> %s::vector) AS similarity
        FROM knowledge_chunks
        WHERE embedding IS NOT NULL
        ORDER BY embedding <=> %s::vector
        LIMIT %s
    """, (vec_str, vec_str, k))
    return [
        {"id": str(r["id"]), "text": r["chunk_text"],
         "source": r["doc_source"], "header": r["doc_header"],
         "similarity": float(r["similarity"]) if r["similarity"] is not None else 0.0}
        for r in rows
    ]


def rerank_with_llm(query, docs, top_k):
    """LLM rerank con IA responsable."""
    if len(docs) <= top_k:
        return docs

    try:
        client = get_llm()
        docs_txt = "\n\n".join(
            f"[{i}] ({d['source']}::{d['header']})\n{d['text'][:300]}"
            for i, d in enumerate(docs)
        )
        prompt = f"""Consulta clinica: {query}

Documentos recuperados:
{docs_txt}

Selecciona los {top_k} documentos MAS relevantes para responder la consulta.
Responde SOLO con JSON: {{"selected": [0, 2, 5], "reasoning": "una linea"}}"""
        resp = client.chat.completions.create(
            model=LLM_MODEL,
            messages=[{"role": "user", "content": prompt}],
            temperature=0.0,
            max_tokens=200,
            response_format={"type": "json_object"},
        )
        parsed = json.loads(resp.choices[0].message.content)
        selected = parsed.get("selected", [])
        reranked = [docs[i] for i in selected if 0 <= i < len(docs)]
        return reranked[:top_k] if reranked else docs[:top_k]
    except Exception as e:
        log.warning(f"rerank fallo, usando orden original: {e}")
        return docs[:top_k]


def run():
    log.info("RAG agent (pgvector + rerank LLM) arrancando...")
    consumer = make_consumer(KAFKA_TOPIC_CLASSIFIED, "rag")

    get_embedder()
    get_llm()
    try:
        rows = pg_fetchall("SELECT COUNT(*) AS c FROM knowledge_chunks;")
        log.info(f"knowledge_chunks: {rows[0]['c']} chunks")
    except Exception as e:
        log.warning(f"knowledge_chunks no accesible: {e}")

    for msg in consumer:
        try:
            t0 = time.time()
            c = ClassifiedFeatures(**msg.value)
            req_id = c.request_id

            base_q = build_query(c)
            expanded_q = expand_query(c, base_q)

            docs_initial = retrieve(expanded_q, RAG_INITIAL_RETRIEVE)
            docs = rerank_with_llm(expanded_q, docs_initial, RAG_RERANK_TOP_K)

            latency_ms = int((time.time() - t0) * 1000)

            confidence = 1.0 if len(docs) >= 3 else (0.5 if len(docs) >= 1 else 0.0)

            log_confidence(
                request_id=req_id,
                agent_name="rag",
                confidence=confidence,
                latency_ms=latency_ms,
                evidence=[
                    f"query_expanded: {expanded_q[:80]}",
                    *[f"{d['source']}::{d['header']} (sim={d['similarity']:.2f})" for d in docs]
                ],
                flags=[] if docs else ["no_docs_retrieved"],
                model_used=LLM_MODEL,
            )

            out = RetrievedContext(
                request_id=req_id,
                user_id=c.user_id,
                message_id=c.message_id,
                study_uid=c.study_uid,
                classified=c,
                documents=[{"id": d["id"], "text": d["text"],
                            "source": d["source"], "header": d["header"]} for d in docs],
            )

            update_request_status(req_id, "retrieved")

            publish_with_trace(
                topic=KAFKA_TOPIC_RETRIEVED,
                payload=out.model_dump(),
                from_agent="rag",
                to_agent="specialist",
                from_topic=KAFKA_TOPIC_CLASSIFIED,
            )
            log.info(
                f"  req={req_id[:8]} initial={len(docs_initial)} "
                f"reranked={len(docs)} conf={confidence:.2f} ({latency_ms}ms)"
            )

        except Exception as e:
            log.exception(f"error en RAG: {e}")


if __name__ == "__main__":
    run()
