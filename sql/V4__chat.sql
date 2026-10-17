-- ============================================================
-- V4 - Chat messages (interface agent with human specialist)
-- ============================================================
CREATE TABLE IF NOT EXISTS chat_messages (
    id              BIGSERIAL PRIMARY KEY,
    request_id      TEXT NOT NULL,
    user_id         TEXT NOT NULL,
    session_id      TEXT,
    role            TEXT NOT NULL,     -- 'user' | 'assistant' | 'system'
    content         TEXT NOT NULL,
    cited_sources   JSONB,
    confidence      DOUBLE PRECISION,
    tokens_used     INTEGER,
    model_used      TEXT,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_chat_request   ON chat_messages(request_id);
CREATE INDEX IF NOT EXISTS idx_chat_user      ON chat_messages(user_id);
CREATE INDEX IF NOT EXISTS idx_chat_session   ON chat_messages(session_id);
CREATE INDEX IF NOT EXISTS idx_chat_created   ON chat_messages(created_at DESC);

CREATE OR REPLACE VIEW v_chat_recent AS
SELECT request_id, user_id, session_id, role,
       LEFT(content, 200) AS content_preview,
       confidence, created_at
FROM chat_messages
ORDER BY created_at DESC;