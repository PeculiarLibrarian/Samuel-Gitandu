# =====================================================
# PADI EXECUTOR v5.3 — Dockerfile
# =====================================================
#
# Multi-stage build for optimized production image
#
# Version: 5.3
# Node: Nairobi-01
# Timestamp: 2026-03-26 [EAT]

# =====================================================
# Stage 1: Base Image
# =====================================================
FROM python:3.11-slim AS base

# Set environment variables
ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    PIP_NO_CACHE_DIR=1 \
    PIP_DISABLE_PIP_VERSION_CHECK=1

# Install system dependencies
RUN apt-get update && apt-get install -y \
    curl \
    gcc \
    g++ \
    git \
    && rm -rf /var/lib/apt/lists/*

# Set working directory
WORKDIR /app

# =====================================================
# Stage 2: Dependencies
# =====================================================
FROM base AS dependencies

# Copy requirements file
COPY requirements.txt .

# Install Python dependencies
RUN pip install --no-cache-dir -r requirements.txt

# Install additional monitoring dependencies
RUN pip install --no-cache-dir prometheus-client

# =====================================================
# Stage 3: Production Image
# =====================================================
FROM base AS production

# Copy Python dependencies
COPY --from=dependencies /usr/local/lib/python3.11/site-packages /usr/local/lib/python3.11/site-packages
COPY --from=dependencies /usr/local/bin /usr/local/bin

# Copy application code
COPY executor.py .
COPY executor_resilience.py .
COPY executor_receipt_tracker.py .
COPY executor_rdf_manager.py .
COPY metrics_exporter.py .
COPY test_deployment.py .

# Create necessary directories
RUN mkdir -p /app/logs \
    /app/logs/json_exports \
    /app/logs/rdf_exports \
    /app/logs/csv_exports \
    /app/logs/diagnostics \
    /app/persist

# Set ownership
RUN useradd -m -u 1000 padi && \
    chown -R padi:padi /app

# Switch to non-root user
USER padi

# Set working directory
WORKDIR /app

# Health check
HEALTHCHECK --interval=30s --timeout=10s --retries=3 \
    CMD python -c "from executor import Executor; Executor(simulation_mode=True).health_check()" || exit 1

# Default command (will be overridden in docker-compose)
CMD ["python", "executor.py"]
