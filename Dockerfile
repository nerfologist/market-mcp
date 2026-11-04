FROM python:3.12-slim
WORKDIR /app
RUN pip install --no-cache-dir "fastmcp==4.1.6" "yfinance==1.8.0" "uvicorn==0.54.0"
COPY server.py eodhd.py ecb.py bdi.py store.py ./
EXPOSE 8765
HEALTHCHECK --interval=30s --timeout=5s --start-period=10s \
  CMD python -c "import urllib.request;urllib.request.urlopen('http://127.0.0.1:8765/healthz')"
CMD ["python", "server.py"]
