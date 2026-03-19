# Airflow Dockerfile
# Base image: official Apache Airflow 2.8.1
# Kailangan i-install ang docker.io para ma-execute ng BashOperator
# ang `docker exec` commands papunta sa ibang containers (pandas-worker, dbt-model)
# gamit ang mounted Docker socket

FROM apache/airflow:2.8.1

# Switch to root para ma-install ang docker.io via apt
USER root

# Install Docker CLI from official Docker repo
# Kailangan ng newer version kasi ang docker.io sa apt ay may lumang API version
# na hindi compatible sa host Docker daemon (minimum API v1.44)
RUN apt-get update \
    && apt-get install -y --no-install-recommends ca-certificates curl gnupg \
    && install -m 0755 -d /etc/apt/keyrings \
    && curl -fsSL https://download.docker.com/linux/debian/gpg | gpg --dearmor -o /etc/apt/keyrings/docker.gpg \
    && chmod a+r /etc/apt/keyrings/docker.gpg \
    && echo "deb [arch=$(dpkg --print-architecture) signed-by=/etc/apt/keyrings/docker.gpg] https://download.docker.com/linux/debian $(. /etc/os-release && echo "$VERSION_CODENAME") stable" > /etc/apt/sources.list.d/docker.list \
    && apt-get update \
    && apt-get install -y --no-install-recommends docker-ce-cli \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

# Balik sa airflow user para sa security
USER airflow
