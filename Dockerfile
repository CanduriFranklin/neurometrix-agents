FROM python:3.13-slim

WORKDIR /app

# System deps for psycopg2 + build tools
RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    libpq-dev \
    && rm -rf /var/lib/apt/lists/*

# Python deps
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# App code
COPY . .

# No secrets, no runtime artifacts (excluded via .dockerignore)
ENV PYTHONUNBUFFERED=1
ENV PYTHONDONTWRITEBYTECODE=1

# Run all 6 agents
CMD ["python", "orchestrator.py"]
