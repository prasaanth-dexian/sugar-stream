# Copyright (c) 2026 Dexian India Technologies Private Limited.
# All rights reserved.
#
# This file is proprietary and confidential. Unauthorised copying,
# distribution or use of this file, via any medium, is strictly
# prohibited without the prior written permission of Dexian India
# Technologies Private Limited.

# THIN PI edge ingest gateway. Build context: thin-pi/edge-gateway
FROM python:3.12-slim
RUN useradd --system --uid 10001 gateway && mkdir -p /data && chown gateway /data
WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY gateway.py ledger.py bootstrap.py ./
USER gateway
VOLUME ["/data"]
EXPOSE 8092
HEALTHCHECK --interval=30s --timeout=5s CMD python -c "import urllib.request;urllib.request.urlopen('http://127.0.0.1:8092/health',timeout=4)"
CMD ["python", "-u", "gateway.py"]
