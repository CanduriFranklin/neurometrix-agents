# Neurometrix Agents - Capa de razonamiento clinico

Pipeline de 5 agentes que consumen features numericas de los cores
matematicos y producen informes clinicos grounded.

## Arquitectura de eventos (Kafka)

neuro-4d (C++) --> core-4d.results
                       |
                       v
                 [classifier] --> agents.classified
                                       |
                                       v
                                  [rag] --> agents.retrieved
                                              |
                                              v
                                        [diagnoser] --> agents.analyzed
                                                          |
                                                          v
                                                    [reporter] --> agents.reports.draft
                                                                    |
                                                                    v
                                                              [auditor] --> agents.reports.final
                                                                              |
                                                                              v
                                                                        Postgres agent_reports

## Los 5 agentes

| Agente | Rol | Usa LLM |
|---|---|---|
| classifier | Scoring determinista sobre features | No |
| rag | Retrieval de knowledge base medica | No |
| diagnoser | Razonamiento sobre features + RAG | Si |
| reporter | Genera informe markdown estructurado | Si |
| auditor | Verifica trazabilidad + confidence | Si |

## Principio

Los agentes no inventan. El auditor rechaza informes donde:
- Aparecen numeros que no estan en las metricas
- Aparecen citas [fuente] que no existen en el RAG
- El LLM escribe sin respaldo

## Uso en AMD Notebook

bash bootstrap.sh
bash run_all.sh

## Uso local (Windows)

cd agents
pip install -r requirements.txt
python -m rag.indexer
python orchestrator.py
