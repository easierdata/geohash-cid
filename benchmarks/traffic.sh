#!/bin/bash

# A shell script to measure Rx and Tx traffic in KB on a specific port and interface using tcpdump.
# It captures packet data and processes it to report the total upon exit (Ctrl+C).

# --- Configuration ---
# Check if the user provided a port number as an argument.
if [ -z "$1" ]; then
  echo "Usage: $0 <port> [interface]"
  echo "Example: $0 5001 eth0"
  echo "If an interface is not provided, it defaults to 'any'."
  exit 1
fi

# Check if the script is run as root, which is required for tcpdump.
if [ "$EUID" -ne 0 ]; then
  echo "Error: This script must be run as root."
  exit 1
fi

PORT=$1
# Use the second argument as the interface, or default to 'any' if it's not provided.
INTERFACE=${2:-any}

# Create a temporary file to store the raw tcpdump output.
# mktemp ensures we have a unique and secure temporary file.
CAPTURE_FILE=$(mktemp)

# --- Cleanup Function ---
# This function is triggered when the script exits (e.g., on Ctrl+C).
# It stops tcpdump, calculates the traffic, prints the report, and removes the temp file.
cleanup() {
  echo -e "\nStopping tcpdump capture..."
  # The TCPDUMP_PID is the process ID of the backgrounded tcpdump command.
  # We send a kill signal to stop it gracefully.
  if [ -n "$TCPDUMP_PID" ]; then
    kill $TCPDUMP_PID
    # Wait for the process to terminate completely.
    wait $TCPDUMP_PID 2>/dev/null
  fi

  echo "Calculating total traffic from capture..."

  # --- Traffic Calculation ---
  # We parse the capture file to distinguish between received (Rx) and transmitted (Tx) traffic.
  # A packet is Rx if the destination port matches our target PORT.
  # A packet is Tx if the source port matches our target PORT.
  # The `grep` command filters for the correct lines, and `awk` finds the packet 'length'
  # value. `paste` and `bc` are used to sum all the length values together.

  # Calculate Received (Rx) bytes: find lines where the destination port is our PORT.
  # The regex looks for patterns like "> 192.168.1.10.5001:"
  RX_BYTES=$(grep -E " > [^ ]+\.${PORT}:" "$CAPTURE_FILE" | awk '{for(i=1; i<=NF; i++) if($i=="length"){print $(i+1);break}}' | paste -sd+ - | bc)

  # Calculate Transmitted (Tx) bytes: find lines where the source port is our PORT.
  # The regex looks for patterns like "192.168.1.10.5001 >"
  TX_BYTES=$(grep -E "[^ ]+\.${PORT} >" "$CAPTURE_FILE" | awk '{for(i=1; i<=NF; i++) if($i=="length"){print $(i+1);break}}' | paste -sd+ - | bc)

  # --- Reporting ---
  echo -e "\n--- Traffic Report for Port ${PORT} on Interface ${INTERFACE} ---"
  # Convert bytes to kilobytes (1 KB = 1024 Bytes), handling cases with no traffic.
  # The :-0 syntax provides a default value of 0 if the variable is null or unset.
  RX_KB=$(echo "scale=2; ${RX_BYTES:-0} / 1024" | bc)
  TX_KB=$(echo "scale=2; ${TX_BYTES:-0} / 1024" | bc)

  echo "Received (Rx):    ${RX_KB} KB"
  echo "Transmitted (Tx): ${TX_KB} KB"
  echo "----------------------------------------------------------"

  # Remove the temporary capture file.
  rm -f "$CAPTURE_FILE"
  echo "Cleanup complete."
  exit 0
}

# 'trap' catches the INT signal (sent by Ctrl+C) and executes the cleanup function.
trap cleanup SIGINT

# --- Main Script Logic ---
echo "Starting tcpdump to monitor port ${PORT} on interface '${INTERFACE}'..."

# Run tcpdump in the background (&).
# -i ${INTERFACE}: Listen on the specified network interface.
# -l: Make stdout line-buffered so we see output immediately.
# -n: Don't resolve hostnames or port names (faster).
# "port ${PORT}": The filter for capturing traffic on the specified port.
# The output is redirected to our temporary file.
tcpdump -l -n -i "${INTERFACE}" "port ${PORT}" > "$CAPTURE_FILE" &
TCPDUMP_PID=$!

echo "Monitoring started (PID: ${TCPDUMP_PID}). Press Ctrl+C to stop and see the report."

# This loop keeps the script alive while tcpdump runs in the background.
# The script will exit when the cleanup function is called via Ctrl+C.
wait $TCPDUMP_PID
# After the user hits Ctrl+C, the trap will run, kill the process, and the script will exit.
# Adding a final cleanup call here in case the wait is interrupted by other means.
cleanup
# End of script