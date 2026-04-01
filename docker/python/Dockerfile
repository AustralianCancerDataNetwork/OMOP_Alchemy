# ---- Stage 1: postgres tools ----
FROM postgres:18 AS pgtools

# ---- Stage 2: python ----
FROM python:3.13

ENV PYTHONPYCACHEPREFIX=/tmp/pycache \
    PYTHONUNBUFFERED=1 \
    UV_PROJECT_ENVIRONMENT=/home/vscode/.venv \
    UV_CACHE_DIR=/home/vscode/.cache/uv \
    PATH="/usr/local/bin:/home/vscode/.venv/bin:$PATH" \
    LANG=C.UTF-8 \
    LC_ALL=C.UTF-8

# system deps
RUN apt-get update && apt-get install -y --no-install-recommends \
    git \
    curl \
    bash \
    bash-completion \
    less \
    vim \
 && rm -rf /var/lib/apt/lists/*

# copy binaries from pgtools stage
COPY --from=pgtools /usr/lib/postgresql /usr/lib/postgresql
COPY --from=pgtools /usr/lib/aarch64-linux-gnu/libpq* /usr/lib/aarch64-linux-gnu/

RUN ln -s /usr/lib/postgresql/18/bin/psql /usr/local/bin/psql \
 && ln -s /usr/lib/postgresql/18/bin/pg_dump /usr/local/bin/pg_dump \
 && ln -s /usr/lib/postgresql/18/bin/pg_restore /usr/local/bin/pg_restore
     
# ---- uv install ----
COPY --from=ghcr.io/astral-sh/uv:latest /uv /usr/local/bin/uv

# ---- User setup ----
RUN useradd -m -s /bin/bash vscode

WORKDIR /workspace

# ---- Auto-activate venv ----
RUN printf '\nif [ -f /home/vscode/.venv/bin/activate ] && [ -z "$VIRTUAL_ENV" ]; then\n  . /home/vscode/.venv/bin/activate\nfi\n' >> /home/vscode/.bashrc \
 && chown vscode:vscode /home/vscode/.bashrc

USER vscode

CMD ["sleep", "infinity"]