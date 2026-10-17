-- ============================================================
-- V3 - Knowledge base vectorial en pgvector
-- ============================================================

CREATE TABLE IF NOT EXISTS knowledge_chunks (
    id          BIGSERIAL PRIMARY KEY,
    doc_source  TEXT NOT NULL,
    doc_header  TEXT NOT NULL,
    chunk_text  TEXT NOT NULL,
    embedding   vector(384),
    created_at  TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE (doc_source, doc_header, chunk_text)
);

CREATE INDEX IF NOT EXISTS idx_knowledge_source
    ON knowledge_chunks(doc_source);

CREATE INDEX IF NOT EXISTS idx_knowledge_embedding
    ON knowledge_chunks
    USING ivfflat (embedding vector_cosine_ops)
    WITH (lists = 10);