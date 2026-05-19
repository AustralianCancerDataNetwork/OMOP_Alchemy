FROM quay.io/jupyter/minimal-notebook:python-3.13

USER root

ENV UV_PROJECT_ENVIRONMENT=/opt/venv \
    UV_CACHE_DIR=/tmp/uv-cache \
    VIRTUAL_ENV=/opt/venv \
    PATH="/opt/venv/bin:/usr/local/bin:${PATH}"

COPY --from=ghcr.io/astral-sh/uv:latest /uv /usr/local/bin/uv

WORKDIR /opt/omop-alchemy

COPY LICENSE README.md pyproject.toml uv.lock ./
COPY omop_alchemy ./omop_alchemy

RUN uv sync --frozen --extra postgres \
 && /opt/venv/bin/python -m pip install ipykernel \
 && /opt/venv/bin/python -m ipykernel install \
        --name uv-venv \
        --display-name "Python (uv venv)" \
 && chown -R jovyan:users /opt/omop-alchemy /opt/venv

USER jovyan
ENV HOME=/home/jovyan
WORKDIR /home/jovyan/work
