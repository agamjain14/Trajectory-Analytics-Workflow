FROM python:3.11-slim

WORKDIR /app

# Install system deps for psutil and pyspark
RUN apt-get update && apt-get install -y --no-install-recommends \
    openjdk-17-jre-headless \
    curl \
    && rm -rf /var/lib/apt/lists/*

# Set JAVA_HOME for PySpark
ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64

# Install Python dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy application code
COPY src/ src/
COPY static/ static/

# Copy pre-collected Delta Lake data (if available, else empty dir)
COPY data/ data/

# Copy entrypoint
COPY entrypoint.sh .
RUN chmod +x entrypoint.sh

# Env defaults (overridden by docker-compose or Azure)
ENV LLM_BACKEND=ollama
ENV DEPLOY_MODE=local
ENV PORT=8000

EXPOSE 8000

ENTRYPOINT ["./entrypoint.sh"]
