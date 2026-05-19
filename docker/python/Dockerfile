FROM python:3.13-slim

ARG INSTALL_DEV=false

ENV PYTHONPYCACHEPREFIX=/tmp/pycache \
    PYTHONUNBUFFERED=1 \
    UV_PROJECT_ENVIRONMENT=/opt/venv \
    UV_CACHE_DIR=/tmp/uv-cache \
    VIRTUAL_ENV=/opt/venv \
    PATH="/opt/venv/bin:/usr/local/bin:$PATH" \
    LANG=C.UTF-8 \
    LC_ALL=C.UTF-8

RUN apt-get update && apt-get install -y --no-install-recommends \
    bash \
    bash-completion \
    curl \
    git \
    less \
    postgresql-client \
    vim \
 && rm -rf /var/lib/apt/lists/*

COPY --from=ghcr.io/astral-sh/uv:latest /uv /usr/local/bin/uv

RUN useradd -m -s /bin/bash omop

WORKDIR /opt/omop-alchemy

COPY LICENSE README.md pyproject.toml uv.lock ./
COPY omop_alchemy ./omop_alchemy

RUN if [ "$INSTALL_DEV" = "true" ]; then \
        uv sync --frozen --extra postgres --extra dev; \
    else \
        uv sync --frozen --extra postgres; \
    fi \
 && chown -R omop:omop /opt/omop-alchemy /opt/venv

RUN printf '\nif [ -f /opt/venv/bin/activate ] && [ -z "$VIRTUAL_ENV" ]; then\n  . /opt/venv/bin/activate\nfi\n' >> /home/omop/.bashrc \
 && chown omop:omop /home/omop/.bashrc

WORKDIR /workspace
USER omop

CMD ["sleep", "infinity"]
