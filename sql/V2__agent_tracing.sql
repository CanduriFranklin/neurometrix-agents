-- ============================================================
-- V2 - Tracing + Confidence por agente + Transacciones
-- ============================================================

CREATE TABLE IF NOT EXISTS requests (
    request_id      TEXT PRIMARY KEY,
    user_id         TEXT NOT NULL,
    study_uid       TEXT,
    message_id      BIGINT,
    status          TEXT NOT NULL DEFAULT 'received',
    pipeline_confidence DOUBLE PRECISION,
    entry_point     TEXT,
    metadata        JSONB,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at      TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_requests_user       ON requests(user_id);
CREATE INDEX IF NOT EXISTS idx_requests_study      ON requests(study_uid);
CREATE INDEX IF NOT EXISTS idx_requests_status     ON requests(status);
CREATE INDEX IF NOT EXISTS idx_requests_created    ON requests(created_at DESC);

CREATE TABLE IF NOT EXISTS agent_confidence (
    id              BIGSERIAL PRIMARY KEY,
    request_id      TEXT NOT NULL REFERENCES requests(request_id) ON DELETE CASCADE,
    agent_name      TEXT NOT NULL,
    confidence      DOUBLE PRECISION NOT NULL,
    latency_ms      INTEGER,
    evidence        JSONB,
    flags           JSONB,
    model_used      TEXT,
    tokens_used     INTEGER,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE (request_id, agent_name)
);

CREATE INDEX IF NOT EXISTS idx_agent_conf_request  ON agent_confidence(request_id);
CREATE INDEX IF NOT EXISTS idx_agent_conf_agent    ON agent_confidence(agent_name);

CREATE TABLE IF NOT EXISTS agent_transactions (
    id              BIGSERIAL PRIMARY KEY,
    request_id      TEXT NOT NULL REFERENCES requests(request_id) ON DELETE CASCADE,
    from_agent      TEXT,
    to_agent        TEXT NOT NULL,
    from_topic      TEXT,
    to_topic        TEXT NOT NULL,
    payload_size    INTEGER,
    status          TEXT NOT NULL,
    error_detail    TEXT,
    occurred_at     TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_trans_request    ON agent_transactions(request_id);
CREATE INDEX IF NOT EXISTS idx_trans_occurred   ON agent_transactions(occurred_at DESC);

CREATE OR REPLACE VIEW v_request_confidence AS
SELECT
    r.request_id, r.user_id, r.study_uid, r.status, r.pipeline_confidence,
    MIN(ac.confidence) AS min_confidence,
    AVG(ac.confidence) AS avg_confidence,
    MAX(ac.confidence) AS max_confidence,
    COUNT(DISTINCT ac.agent_name) AS agents_completed,
    MAX(ac.created_at) - MIN(ac.created_at) AS total_latency,
    r.created_at
FROM requests r
LEFT JOIN agent_confidence ac ON ac.request_id = r.request_id
GROUP BY r.request_id
ORDER BY r.created_at DESC;

CREATE OR REPLACE VIEW v_request_trace AS
SELECT
    t.request_id, t.from_agent, t.to_agent,
    t.from_topic, t.to_topic, t.status, t.occurred_at,
    EXTRACT(EPOCH FROM (
        t.occurred_at - LAG(t.occurred_at) OVER (PARTITION BY t.request_id ORDER BY t.occurred_at)
    )) * 1000 AS delta_ms
FROM agent_transactions t
ORDER BY t.request_id, t.occurred_at;

ALTER TABLE agent_reports
    ADD COLUMN IF NOT EXISTS request_id TEXT,
    ADD COLUMN IF NOT EXISTS user_id    TEXT;

CREATE INDEX IF NOT EXISTS idx_agent_reports_request ON agent_reports(request_id);
CREATE INDEX IF NOT EXISTS idx_agent_reports_user    ON agent_reports(user_id);