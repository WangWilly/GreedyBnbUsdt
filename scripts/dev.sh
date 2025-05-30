#/bin/bash

# load the environment variables from .env file
if [ -f .env ]; then
  export $(cat .env | xargs)
else
  echo ".env file not found. Please create one with the necessary environment variables."
  exit 1
fi

# This script is used to run the dev server for the project.
# It sets up the environment and starts the server.
# Usage: ./scripts/dev.sh
# Make sure to run this script from the root of the project.
# Check if the script is being run from the root of the project
python main.py
