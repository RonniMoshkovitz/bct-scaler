FROM python:3.12-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1 \
    PIP_DISABLE_PIP_VERSION_CHECK=1 \
    PIP_ROOT_USER_ACTION=ignore

WORKDIR /app

# Pinned dependencies first, in their own layer (a source change doesn't reinstall them).
COPY requirements.txt ./
RUN pip install -r requirements.txt

COPY pyproject.toml ./
COPY src ./src
# pip check fails the build if the pins don't satisfy pyproject.toml's dependency ranges.
RUN pip install --no-deps . && pip check

# Run as an unprivileged user; a non-interactive job needs no home directory or login shell.
RUN useradd --system appuser
USER appuser

ENTRYPOINT ["autoscaler"]
