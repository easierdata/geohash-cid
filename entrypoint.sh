#!/bin/sh
# Use -e to exit immediately if a command fails.
set -e

# This is the user that should own the IPFS repo files.
# The base python:3.10-slim image does not have a non-root user by default.
# For production, it's recommended to create a non-root user.
# For this example, we will run as root.

# Check if the IPFS repository is initialized by looking for the config file.
if [ -e "$IPFS_PATH/config" ]; then
  echo "Found existing IPFS repository at $IPFS_PATH"
else
  echo "No IPFS repository found. Initializing..."
  # Initialize the IPFS repository.
  # --profile server is a good default for nodes that are always online.
  # It disables local network discovery to reduce background noise.
  ipfs init --profile server
  
  # Update configuration to allow API access from any IP address within the container.
  # This is necessary to access the API from the host machine.
  ipfs config Addresses.API /ip4/0.0.0.0/tcp/5001
  ipfs config Addresses.Gateway /ip4/0.0.0.0/tcp/8080

  echo "IPFS repository initialized."
fi

# The command to run (e.g., "daemon") is passed as arguments to this script.
# `exec` replaces the shell process with the IPFS daemon,
# allowing it to receive signals from the container runtime correctly.
echo "Starting IPFS daemon..."
exec ipfs "$@"
