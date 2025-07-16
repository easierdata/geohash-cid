import subprocess

# Execute the shell command to find the path of ipfs
try:
    ipfs_binary = subprocess.check_output(["which", "ipfs"]).decode().strip()
except subprocess.CalledProcessError:
    # If 'which ipfs' fails, alert the user and set a default path
    print("IPFS binary not found in PATH. Please ensure IPFS is installed and available in your PATH.")
    # Set a default path or handle the error as needed
    ipfs_binary = "[IPFS not found]"

print("Using IPFS path:", ipfs_binary)