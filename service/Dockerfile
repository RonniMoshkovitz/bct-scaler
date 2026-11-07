FROM python:3.12-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1

RUN useradd --system --no-create-home appuser

WORKDIR /app

# Copy project definition and source together
COPY pyproject.toml ./
COPY src ./src

# Install package and generate the `autoscaler` CLI executable
RUN pip install --no-cache-dir .

USER appuser

ENTRYPOINT ["autoscaler"]