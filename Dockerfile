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
COPY docker /app/docker
COPY vpn /app/vpn
COPY requirements.txt /app/requirements.txt

RUN pip install --no-cache-dir -r requirements.txt

ENV PYTHONPATH=/app

RUN chmod +x /app/docker/entrypoint.sh /app/docker/start_vpn.sh

ENTRYPOINT ["bash", "/app/docker/entrypoint.sh"]
