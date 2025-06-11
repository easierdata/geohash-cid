import subprocess

# Execute the shell command to find the path of ipfs
try:
    ipfs_binary = subprocess.check_output(["which", "ipfs"]).decode().strip()
except subprocess.CalledProcessError:
    # If 'which ipfs' fails, set the path to the default
    ipfs_binary = "/gpfs/data1/oshangp/easier/textile/kubo/ipfs"

print("IPFS path:", ipfs_binary)