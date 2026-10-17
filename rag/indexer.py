"""Construye el indice vectorial en Postgres (pgvector) desde knowledge/*.md."""
from __future__ import annotations

import os
import glob

from config import KNOWLEDGE_DIR, EMBED_MODEL
from storage import get_logger, pg_execute

log = get_logger("rag.indexer")


def chunk_markdown(path):
    with open(path, "r", encoding="utf-8") as f:
        text = f.read()

    basename = os.path.basename(path)
    chunks = []
    current_header = basename
    current_body = []

    for line in text.split("\n"):
        if line.startswith("## "):
            if current_body:
                chunks.append({
                    "header": current_header,
                    "text": "\n".join(current_body).strip(),
                    "source": basename,
                })
            current_header = line[3:].strip()
            current_body = [line]
        else:
            current_body.append(line)

    if current_body:
        chunks.append({
            "header": current_header,
            "text": "\n".join(current_body).strip(),
            "source": basename,
        })

    return [c for c in chunks if len(c["text"]) > 30]


def build_index():
    from sentence_transformers import SentenceTransformer

    log.info(f"cargando embedder {EMBED_MODEL}...")
    model = SentenceTransformer(EMBED_MODEL)

    pg_execute("TRUNCATE knowledge_chunks RESTART IDENTITY;")

    md_files = glob.glob(os.path.join(KNOWLEDGE_DIR, "*.md"))
    log.info(f"indexando {len(md_files)} archivos...")

    total = 0
    for path in md_files:
        for chunk in chunk_markdown(path):
            emb = model.encode(chunk["text"], normalize_embeddings=True)
            vec_str = "[" + ",".join(f"{x:.6f}" for x in emb.tolist()) + "]"

            pg_execute("""
                INSERT INTO knowledge_chunks
                    (doc_source, doc_header, chunk_text, embedding)
                VALUES (%s, %s, %s, %s::vector)
                ON CONFLICT (doc_source, doc_header, chunk_text) DO NOTHING
            """, (chunk["source"], chunk["header"], chunk["text"], vec_str))
            total += 1

    log.info(f"indexados {total} chunks en pgvector")
    return total


if __name__ == "__main__":
    n = build_index()
    print(f"OK: {n} chunks indexados")
