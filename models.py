"""Modelos Pydantic compartidos entre los agentes."""
from __future__ import annotations

import uuid
from typing import Any, Dict, List, Optional
from pydantic import BaseModel, Field


def new_request_id() -> str:
    return str(uuid.uuid4())


def new_user_id() -> str:
    return str(uuid.uuid4())


# ============================================================
# Input: core-4d.results (producido por neuro-4d)
# ============================================================
class VolumeFeatures(BaseModel):
    request_id: str = Field(default_factory=new_request_id)
    user_id: str = Field(default_factory=new_user_id)
    message_id: int
    study_uid: str
    status: str = "processed"

    height: int = 0
    width: int = 0
    depth: int = 0
    timepoints: int = 0
    is_4d: bool = False

    mean_hu: float = 0.0
    stddev_hu: float = 0.0
    min_hu: float = 0.0
    max_hu: float = 0.0

    mean_entropy: float = 0.0
    peak_vesselness: float = 0.0
    asymmetry_score: float = 0.0
    total_volume_mm3: float = 0.0

    # 4D perfusion summary (only if is_4d)
    mean_cbf: float = 0.0
    mean_cbv: float = 0.0
    mean_mtt: float = 0.0
    mean_ttp: float = 0.0
    mean_tmax: float = 0.0

    core_voxels: int = 0
    penumbra_voxels: int = 0
    mismatch_ratio: float = 0.0

    findings: List[Dict[str, Any]] = Field(default_factory=list)

    processing_ms: int = 0
    gpu_used: bool = False
    error_detail: str = ""


# ============================================================
# Etapa 1: classifier -> agents.classified
# ============================================================
class ClassifiedFeatures(BaseModel):
    request_id: str
    user_id: str
    message_id: int
    study_uid: str
    features: VolumeFeatures

    # === CAPA 1: Reglas deterministas (fuente de verdad, auditable) ===
    scores: Dict[str, float] = Field(default_factory=dict)
    flags: List[str] = Field(default_factory=list)
    primary_hypothesis: str = ""
    reasoning: str = ""

    # === CAPA 2: Razonamiento LLM (explicativo, NO vinculante) ===
    # IAA RESPONSABLE: el LLM explica, no decide.
    llm_reasoning: str = ""
    llm_differential: List[Dict[str, Any]] = Field(default_factory=list)
    llm_uncertainty: float = 0.0
    llm_refused: bool = False
    llm_tokens: int = 0

    # Metadata de trazabilidad del modelo usado
    llm_model: str = ""


# ============================================================
# Etapa 2: rag -> agents.retrieved
# ============================================================
class RetrievedContext(BaseModel):
    request_id: str
    user_id: str
    message_id: int
    study_uid: str
    classified: ClassifiedFeatures
    documents: List[Dict[str, str]] = Field(default_factory=list)


# ============================================================
# Etapa 3: diagnoser -> agents.analyzed
# ============================================================
class DiagnosedReport(BaseModel):
    request_id: str
    user_id: str
    message_id: int
    study_uid: str
    retrieved: RetrievedContext
    analysis: str = ""
    impression: str = ""
    differential: List[str] = Field(default_factory=list)
    confidence: float = 0.0
    evidence: List[str] = Field(default_factory=list)


# ============================================================
# Etapa 4: reporter -> agents.reports.draft
# ============================================================
class DraftReport(BaseModel):
    request_id: str
    user_id: str
    message_id: int
    study_uid: str
    diagnosed: DiagnosedReport
    markdown: str = ""


# ============================================================
# Etapa 5: auditor -> agents.reports.final
# ============================================================
class FinalReport(BaseModel):
    request_id: str
    user_id: str
    message_id: int
    study_uid: str
    draft: DraftReport
    audit_passed: bool = False
    audit_findings: List[str] = Field(default_factory=list)
    confidence_score: float = 0.0
    evidence_trail: List[str] = Field(default_factory=list)
    final_markdown: str = ""


# ============================================================
# Trazabilidad: confidence + transacciones
# ============================================================
class AgentConfidence(BaseModel):
    """Confidence score de un agente sobre una respuesta."""
    request_id: str
    agent_name: str          # classifier | rag | diagnoser | reporter | auditor
    confidence: float = Field(ge=0.0, le=1.0)
    latency_ms: int = 0
    evidence: List[str] = Field(default_factory=list)
    flags: List[str] = Field(default_factory=list)
    model_used: Optional[str] = None
    tokens_used: int = 0


class AgentTransaction(BaseModel):
    """Registro de cada salto entre agentes."""
    request_id: str
    from_agent: Optional[str] = None
    to_agent: str
    from_topic: Optional[str] = None
    to_topic: str
    payload_size: int = 0
    status: str = "ok"
    error_detail: Optional[str] = None


# ============================================================
# Modelos del agente INTERFACE (chat con el especialista)
# ============================================================
class ChatMessage(BaseModel):
    request_id: str
    user_id: str
    role: str          # "user" | "assistant" | "system"
    content: str
    metadata: Dict[str, Any] = Field(default_factory=dict)


class ChatResponse(BaseModel):
    request_id: str
    user_id: str
    answer: str
    cited_sources: List[str] = Field(default_factory=list)
    confidence: float = 0.0
    escalation_recommended: bool = False
    escalation_reason: str = ""
    tokens_used: int = 0
    model_used: str = ""
