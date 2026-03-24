FROM python:3.13
ENV PYTHONPYCACHEPREFIX=/tmp/pycache
ENV PYTHONUNBUFFERED=1
RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    git \
    curl \
    libpq-dev \
    bash \
    bash-completion \
    libreadline-dev \
    less \
    vim \
 && rm -rf /var/lib/apt/lists/*
ENV TERM=xterm-256color
RUN curl -Ls https://astral.sh/uv/install.sh | sh \
    && mv /root/.local/bin/uv /usr/local/bin/uv
ENV PATH="/usr/local/bin:$PATH"
RUN useradd -m -s /bin/bash vscode
WORKDIR /workspace
RUN chown -R vscode:vscode /workspace
RUN printf '\n# Auto-activate project virtualenv when opening a Bash shell\nif [ -f /workspace/.venv/bin/activate ] && [ -z "$VIRTUAL_ENV" ]; then\n  . /workspace/.venv/bin/activate\nfi\n' >> /home/vscode/.bashrc \
 && chown vscode:vscode /home/vscode/.bashrc
ENV PATH="/workspace/.venv/bin:$PATH"
USER vscode
ENV UV_CACHE_DIR=/home/vscode/.cache/uv
CMD ["sleep", "infinity"]