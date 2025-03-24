FROM python:3.12-slim

ENV POETRY_VIRTUALENVS_CREATE=false

WORKDIR /app

COPY . .

RUN pip install poetry && \
    poetry config installer.max-workers 10 && \
    poetry install --no-interaction --no-ansi && \
    poetry run python -m nltk.downloader stopwords

EXPOSE 8080

CMD ["gunicorn", "-b", "0.0.0.0:8000", "server:app", "--reload", "--log-level", "info", "--access-logfile", "Files/log/access.log", "--error-logfile", "Files/log/error.log"]
