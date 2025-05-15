FROM python:3.12.10-alpine3.20

ENV POETRY_VIRTUALENVS_CREATE=false

RUN apk update && apk upgrade && apk add --no-cache postgresql-dev gcc python3-dev musl-dev linux-headers


WORKDIR /app
COPY . .

RUN pip install poetry
RUN poetry config installer.max-workers 10
RUN poetry install --no-interaction --no-ansi
RUN poetry run python -m nltk.downloader stopwords

EXPOSE 8000

CMD ["poetry", "run", "uvicorn", "--host", "0.0.0.0", "simcc:app", "--log-level", "info", "--log-config", "/app/logging.yaml"]