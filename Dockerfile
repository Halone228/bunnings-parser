FROM chetan1111/botasaurus:latest
ENV PYTHONUNBUFFERED=1
WORKDIR /app
RUN pip install poetry
COPY pyproject.toml .
COPY main.py .
COPY parser_bb ./parser_bb
COPY payload.mako .
COPY test.json .
RUN poetry config virtualenvs.create false
RUN poetry install
VOLUME /app/results
ENTRYPOINT ["poetry", "run", "start"]
