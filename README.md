# GreedyBnbUsdt

## Core Functions

- Automated grid trading for BNB/USDT spot market.
- S1 strategy for position control based on 52-day high/low.
- Advanced risk management with configurable position ratio limits.
- Web dashboard for real-time monitoring, logs, and system status.
- Automatic fund transfer between spot and flexible savings (Simple Earn).

## Prerequest

- Python 3.12+ (recommended)
- Binance account with API key/secret (spot trading enabled)
- [ccxt](https://github.com/ccxt/ccxt) (async version)
- Poetry (recommended) or pip for dependency management
- Set up `.env` file with required environment variables:
  - `EXCHANGE_CLIENT_BINANCE_API_KEY`
  - `EXCHANGE_CLIENT_BINANCE_API_SECRET`
  - (Optional) `EXCHANGE_CLIENT_HTTP_PROXY`
  - (Optional) `LOG_DEBUG` for debug logging

## Installation

### Using Poetry (Recommended)

First, [install Poetry](https://python-poetry.org/docs/#installation) if you haven't already:

```bash
curl -sSL https://install.python-poetry.org | python3 -
```

Then install the project dependencies:

```bash
git clone https://github.com/WangWilly/GreedyBnbUsdt.git
cd GreedyBnbUsdt
# Install all dependencies defined in pyproject.toml
poetry install
# Prepare your .env file in the project root
cp .env.example .env
# Edit .env with your Binance API credentials
```

To activate the Poetry virtual environment:

```bash
poetry env activate
```

## Running the Application

To start the development server:

```bash
./scripts/dev.sh
```

## Running with Docker

You can also run the application using Docker:

### Building the Docker Image

```bash
./scripts/build.sh
```

This will build the Docker image tagged as `greedybnbusdt/backend:latest`.

### Running with Docker Compose

1. Configure your environment variables in `deployments/docker-compose.yml` or use environment files:

```bash
# Edit the docker-compose.yml file to include your API credentials
nano deployments/docker-compose.yml
```

2. Start the service:

```bash
./scripts/start.sh
```

This will start the application in detached mode, with the web interface accessible at `http://localhost:8080`.

### Accessing Logs

Logs are mounted to `./deployments/logs` on your host machine.

## Caution

- Ensure your Binance API key has only the required permissions.
- Use at your own risk. Test thoroughly with small amounts or in sandbox before live trading.
- The authors are not responsible for any financial loss or account issues.
- Always keep your API keys secure and never share them.
