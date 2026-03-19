# dbt Model Dockerfile
# Ito ang container para sa Gold layer transformations gamit ang dbt
# Gagamitin ng Airflow via `docker exec` — walang sariling CMD/ENTRYPOINT dito,
# ang docker-compose ang mag-se-set ng `tail -f /dev/null` para manatiling buhay

FROM python:3.11-slim

# Working directory sa loob ng container
WORKDIR /usr/app

# I-install ang git kasi required dependency ng dbt
RUN apt-get update \
    && apt-get install -y --no-install-recommends git \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

# I-install ang dbt-postgres, loguru, at pinned protobuf
# Kailangan i-pin ang protobuf<5 dahil may known bug sa dbt 1.7.4
# kung newer protobuf ang naka-install (MessageToJson error)
RUN pip install --no-cache-dir dbt-postgres==1.7.4 loguru==0.7.2 "protobuf<5"
