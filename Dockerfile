################################################################################
# Base image
FROM python:3.13.3-alpine as python

ENV PYTHONUNBUFFERED=true

ENV POETRY_HOME=/opt/poetry
ENV POETRY_VIRTUALENVS_IN_PROJECT=true
ENV PATH="$POETRY_HOME/bin:$PATH"

ENV PATH="/app/.venv/bin:$PATH"

WORKDIR /app

################################################################################
# Install poetry
FROM python as pypoetry
RUN apk update && apk add --no-cache cmake build-base curl

# Create Poetry directory and explicitly set permissions
RUN mkdir -p $POETRY_HOME
RUN curl -sSL https://install.python-poetry.org | POETRY_HOME=$POETRY_HOME python3 -

# Verify Poetry installation
RUN poetry --version

################################################################################
# Install runner dependencies
FROM pypoetry as runner-deps

# Install system dependencies for compiling Python packages
RUN apk add --no-cache gcc python3-dev musl-dev linux-headers

COPY ./poetry.lock ./pyproject.toml ./README.md ./

RUN poetry install --without dev --no-interaction --no-ansi -vvv

################################################################################
# Copy the dependencies to the final image
FROM python as runner

ENV PYTHONPATH="/app"

COPY --from=runner-deps /app /app
COPY ./ ./

CMD ["python", "main.py"]
