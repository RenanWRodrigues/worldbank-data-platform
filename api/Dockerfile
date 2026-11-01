# FastAPI de serving: lê dw e rt com o usuário somente leitura api_ro
FROM python:3.12.12-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1

WORKDIR /srv
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY app/ app/

RUN useradd --uid 10001 --no-create-home api
USER api
EXPOSE 8000

# Sem curl na imagem slim: o healthcheck usa a biblioteca padrão
HEALTHCHECK --interval=15s --timeout=5s --start-period=10s --retries=5 \
  CMD python -c "import urllib.request; urllib.request.urlopen('http://localhost:8000/health', timeout=4)"

# Um worker: o SSE é assíncrono e o pool de conexões fica num processo só
CMD ["uvicorn", "app.main:app", "--host", "0.0.0.0", "--port", "8000", "--workers", "1", "--proxy-headers"]
