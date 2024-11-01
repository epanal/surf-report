# Use the official Airflow image as base
FROM apache/airflow:2.6.2-python3.8

# Switch to root to install system dependencies
USER root

# Install system dependencies
RUN apt-get update && apt-get install -y \
    build-essential \
    git \
    curl \
    && rm -rf /var/lib/apt/lists/*

# Switch back to airflow user
USER airflow

# Create a directory for requirements
WORKDIR /opt/airflow

# Create requirements.txt file
COPY --chown=airflow:root requirements.txt .

# Install Python dependencies
RUN pip install --no-cache-dir \
    -r requirements.txt \
    && pip cache purge

# Copy your DAGs and other necessary files
COPY --chown=airflow:root dags/ ./dags/
COPY --chown=airflow:root plugins/ ./plugins/

# Optional: Set environment variables if needed
ENV PYTHONPATH=/opt/airflow