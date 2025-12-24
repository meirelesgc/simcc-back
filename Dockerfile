FROM python:3.12.10-alpine3.20 AS builder

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    POETRY_NO_INTERACTION=1 \
    POETRY_VIRTUALENVS_CREATE=false \
    POETRY_HOME="/opt/poetry"

ENV PATH="$POETRY_HOME/bin:$PATH"

RUN apk add --no-cache gcc musl-dev postgresql-dev libffi-dev

WORKDIR /app

RUN pip install --no-cache-dir poetry

COPY pyproject.toml poetry.lock ./

RUN poetry install --no-root --only main --no-ansi

FROM python:3.12.10-alpine3.20 AS runtime

LABEL maintainer="geu_costa@outlook.com"

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PATH="/home/python/.local/bin:$PATH" \
    NLTK_DATA="/app/nltk_data"

RUN apk add --no-cache libpq

RUN addgroup -S python && adduser -S python -G python

WORKDIR /app

COPY --from=builder /usr/local/lib/python3.12/site-packages /usr/local/lib/python3.12/site-packages
COPY --from=builder /usr/local/bin /usr/local/bin

COPY --chown=python:python . .

RUN python -m nltk.downloader -d /app/nltk_data stopwords

USER python

EXPOSE 8000

# CMD ["gunicorn", "--bind", "0.0.0.0:8000", "--workers", "4", "--threads", "2", "--access-logfile", "-", "server:app"]
CMD ["gunicorn", "--bind", "0.0.0.0:8000", "--access-logfile", "-", "server:app"]