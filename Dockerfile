FROM python:3.12.10-alpine3.20

ENV POETRY_VIRTUALENVS_CREATE=false

WORKDIR /app

COPY . .

RUN pip install poetry && \
    poetry config installer.max-workers 10 && \
    poetry install --no-interaction --no-ansi && \
    poetry run python -m nltk.downloader stopwords

EXPOSE 8000

CMD ["poetry", "run", "gunicorn", "-b", "0.0.0.0:8000", "server:app", "--reload"]
