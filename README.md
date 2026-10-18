
<div align="center">
  <img src="logo.png" alt="Neurometrix Logo" width="300"/>
  
  # NEUROMETRIX
  
  **ADVANCED MATHEMATICAL NEUROIMAGING SYSTEM**  
  *QUANTITATIVE STRUCTURAL MEASUREMENT & AGENTIAL INFERENCE ENGINE*
  
  <br/>
  <img src="hackathon.png" alt="AMD Developer Hackathon ACT III" width="600"/>
</div>

---


[![AMD ROCm](https://img.shields.io/badge/Powered%20by-AMD%20ROCm-E31837?style=for-the-badge&amp;logo=amd)](https://www.amd.com/en/graphics/servers-solutions-rocm)
![Google Gemma](https://img.shields.io/badge/Model-Google%20Gemma-4285F4?style=for-the-badge&amp;logo=google)(https://ai.google.dev/gemma)
![Python 3.13](https://img.shields.io/badge/Python-3.13-blue?style=for-the-badge&amp;logo=python)()

## Project Overview
This repository contains the Cognitive Layer of Neurometrix. It is a multi-agent AI swarm built on Python and Apache Kafka. It consumes pure numerical data extracted by the C++ Math Cores and uses Google Gemma (hosted on AMD ROCm) to generate expert-level radiological reports.

## The Zero-Hallucination Philosophy
Traditional medical AI tries to look at images and guess. We do not.
1. The C++ Core calculates precise metrics (CBF, CBV).
2. The AI Agents receive these hard numbers.
3. The LLM (Gemma) is mathematically constrained to base its diagnosis only on this verified data.

## Value Proposition (For SMBs & Clinics)
- Democratizing Tier-1 Tools: Small clinics and private practices cannot afford $500k proprietary perfusion software. This open, microservice-based swarm provides hospital-grade analysis at a fraction of the cost.
- ROI: Reduces radiologist analysis time from 45 minutes to 2 minutes per 4D study.
- Liability Reduction: By eliminating AI hallucinations, it protects clinics from misdiagnosis lawsuits.

## Architecture
This module uses an Event-Driven Architecture:
- Orchestrator: Routes incoming DICOMs.
- Data Engineer (ETL): Curates files (Raw -> Workspace -> Curated) and triggers the C++ Core.
- Classifier, RAG, Specialist, Reporter, Auditor: The AI swarm.
- Interface: Real-time streaming chat.

## Setup & Run
```bash
python -m venv venv
venv\Scripts\activate
pip install -r requirements.txt
python supervisor.py
```
