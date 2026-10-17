-- ============================================================
-- Agent Reports schema
-- Persiste los informes generados por el pipeline de 5 agentes.
-- ============================================================

CREATE TABLE IF NOT EXISTS agent_reports (
    id              BIGSERIAL PRIMARY KEY,
    message_id      BIGINT NOT NULL,
    study_uid       TEXT   NOT NULL,

    -- Scores del clasificador
    primary_hypothesis  TEXT,
    scores_json         JSONB,

    -- Razonamiento del especialista
    analysis        TEXT,
    impression      TEXT,
    differential    JSONB,
    confidence      DOUBLE PRECISION,

    -- Informe final
    draft_markdown  TEXT,
    final_markdown  TEXT,

    -- Auditoría
    audit_passed    BOOLEAN DEFAULT FALSE,
    audit_findings  JSONB,
    evidence_trail  JSONB,
    confidence_score DOUBLE PRECISION,

    -- Metadata
    processing_ms   BIGINT,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),

    UNIQUE (message_id)
);

CREATE INDEX IF NOT EXISTS idx_agent_reports_study    ON agent_reports(study_uid);
CREATE INDEX IF NOT EXISTS idx_agent_reports_created  ON agent_reports(created_at DESC);
CREATE INDEX IF NOT EXISTS idx_agent_reports_passed   ON agent_reports(audit_passed);

-- ============================================================
-- Vista de conveniencia: últimos informes finales
-- ============================================================
CREATE OR REPLACE VIEW v_agent_reports_final AS
SELECT
    id, message_id, study_uid,
    primary_hypothesis,
    confidence,
    confidence_score,
    audit_passed,
    created_at
FROM agent_reports
WHERE audit_passed = TRUE
ORDER BY created_at DESC;
