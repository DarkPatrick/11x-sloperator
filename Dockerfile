FROM python:3.13-slim

WORKDIR /app

RUN apt-get update && apt-get install -y --no-install-recommends \
    openvpn \
    iproute2 \
    iputils-ping \
    ca-certificates \
    bash \
    && rm -rf /var/lib/apt/lists/*

COPY main.py /app/main.py
COPY clickhouse_worker.py /app/clickhouse_worker.py
COPY vpn_supervisor.py /app/vpn_supervisor.py
COPY clickhouse_supervisor.py /app/clickhouse_supervisor.py
COPY slack_worker.py /app/slack_worker.py
COPY stats.py /app/stats.py
COPY chatgpt_agent_worker.py /app/chatgpt_agent_worker.py
COPY docker /app/docker
COPY queries /app/queries
COPY metrics.yaml /app/metrics.yaml
COPY vpn /app/vpn
COPY requirements.txt /app/requirements.txt

RUN pip install --no-cache-dir -r requirements.txt

ENV PYTHONPATH=/app

RUN chmod +x /app/docker/entrypoint.sh /app/docker/start_vpn.sh

ENTRYPOINT ["bash", "/app/docker/entrypoint.sh"]
