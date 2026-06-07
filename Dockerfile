FROM python:3.11-slim

WORKDIR /app

# Build arg: set to "streaming" to include PySpark + JDK (~650MB extra)
ARG DEPLOY_MODE=local

# Install system deps — JDK only for streaming mode
RUN apt-get update && apt-get install -y --no-install-recommends curl \
    && if [ "$DEPLOY_MODE" = "streaming" ]; then \
         apt-get install -y --no-install-recommends openjdk-21-jre-headless; \
       fi \
    && rm -rf /var/lib/apt/lists/*

ENV JAVA_HOME=/usr/lib/jvm/java-21-openjdk-arm64

# Install core Python dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Install streaming extras only when needed
COPY requirements-streaming.txt .
RUN if [ "$DEPLOY_MODE" = "streaming" ]; then \
      pip install --no-cache-dir -r requirements-streaming.txt; \
    fi

# Copy application code
COPY src/ src/
COPY static/ static/

# Copy pre-seeded data directory (Delta tables populated on first run by live_metrics)
COPY data/ data/

# Copy entrypoint
COPY entrypoint.sh .
RUN chmod +x entrypoint.sh

# Env defaults (overridden by docker-compose or deploy scripts)
ENV LLM_BACKEND=ollama
ENV DEPLOY_MODE=local
ENV PORT=8000

EXPOSE 8000

CMD ["./entrypoint.sh"]
