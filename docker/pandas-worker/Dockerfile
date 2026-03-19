# Pandas Worker Dockerfile
# Ito ang container na magpo-process ng Bronze at Silver layer transformations
# Gagamitin ng Airflow via `docker exec` — walang sariling CMD/ENTRYPOINT dito,
# ang docker-compose ang mag-se-set ng `tail -f /dev/null` para manatiling buhay

FROM python:3.11-slim

# Working directory sa loob ng container
WORKDIR /app

# I-copy at i-install ang Python dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
