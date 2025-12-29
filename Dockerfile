FROM python:3.12.10-alpine3.20 AS builder

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    POETRY_NO_INTERACTION=1 \
    POETRY_HOME="/opt/poetry" \
    POETRY_VIRTUALENVS_IN_PROJECT=true

ENV PATH="$POETRY_HOME/bin:$PATH"

RUN apk add --no-cache gcc musl-dev postgresql-dev libffi-dev libxml2-dev libxslt-dev

WORKDIR /app

RUN pip install --no-cache-dir poetry

COPY pyproject.toml poetry.lock ./

RUN poetry install --no-root --no-ansi

FROM python:3.12.10-alpine3.20 AS runtime

LABEL maintainer="geu_costa@outlook.com"

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PATH="/app/.venv/bin:$PATH" \
    PYTHONPATH="/app" \
    NLTK_DATA="/app/nltk_data"

RUN apk add --no-cache libpq libxml2 libxslt

RUN addgroup -S python && adduser -S python -G python

WORKDIR /app

COPY --from=builder /app/.venv /app/.venv
COPY --chown=python:python . .

RUN python -m nltk.downloader -d /app/nltk_data stopwords

USER python

EXPOSE 8000

CMD ["uvicorn", "--host", "0.0.0.0", "simcc.app:app"]