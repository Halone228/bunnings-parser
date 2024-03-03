FROM chetan1111/botasaurus:latest
ENV PYTHONUNBUFFERED=1
WORKDIR /app
RUN pip install poetry
COPY pyproject.toml .
COPY main.py .
COPY payload.mako .
COPY test.json .
RUN poetry config virtualenvs.create false
RUN poetry install
VOLUME /app/data
ENTRYPOINT ["poetry", "run", "start"]
