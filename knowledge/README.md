# Knowledge Base — Neurometrix Agents

This folder contains the medical documents that `rag/` indexes into pgvector.
Each `.md` is a citable source: the auditor verifies that every statement
in the report traces back to one of these documents.

## Documents
- `perfusion_thresholds.md` — TMax/CBF/CBV/MTT thresholds (DEFUSE, EXTEND-IA)
- `hemorrhage.md` — Intracranial hemorrhage classification
- `ischemia.md` — Ischemic stroke, penumbra, core
- `hounsfield_reference.md` — HU ranges per tissue

## Format
Each document has short `##` sections to improve semantic retrieval.
