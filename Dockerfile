FROM ghcr.io/astral-sh/uv:python3.12-bookworm-slim AS builder

WORKDIR /build

COPY pyproject.toml .
COPY llm_broker/main.py ./llm_broker/

RUN uv build --wheel --out-dir /dist


FROM python:3.12-slim AS runtime

WORKDIR /app

COPY --from=builder /dist/*.whl .

RUN pip install --no-cache-dir *.whl && rm *.whl

CMD ["python", "-m", "llm_broker.main"]