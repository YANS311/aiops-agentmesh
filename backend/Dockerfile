FROM python:3.12-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1 \
    PIP_INDEX_URL=https://mirrors.aliyun.com/pypi/simple

WORKDIR /app

COPY pyproject.toml ./
COPY app ./app
COPY skills ./skills
COPY alembic.ini ./
COPY alembic ./alembic

RUN pip install --upgrade pip && pip install .

EXPOSE 8016

HEALTHCHECK --interval=5s --timeout=3s --retries=12 \
  CMD python -c "import urllib.request; urllib.request.urlopen('http://127.0.0.1:8016/api/v1/health')" || exit 1

CMD ["sh", "-c", "alembic upgrade head && exec uvicorn app.main:app --host 0.0.0.0 --port 8016"]
