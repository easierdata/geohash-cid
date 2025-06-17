# Use a slim Python image as the base.
# Using a specific version is a good practice for reproducibility.
FROM python:3.10-slim

# Set environment variables for IPFS version and repository path.
# You can update IPFS_VERSION to a newer version from https://dist.ipfs.tech/#go-ipfs
ENV IPFS_VERSION=v0.28.0
ENV GO_IPFS_DIST_URL=https://dist.ipfs.tech/go-ipfs/${IPFS_VERSION}/go-ipfs_${IPFS_VERSION}_linux-amd64.tar.gz
ENV IPFS_PATH=/data/ipfs

# Install necessary dependencies.
# - wget: to download the go-ipfs binary.
# - git: to clone the python package from GitHub.
# - tini: a minimal init system for containers to properly handle signals.
# We clean up the apt cache to keep the image size small.
RUN apt-get update && apt-get install -y --no-install-recommends \
    wget \
    git \
    tini \
    && rm -rf /var/lib/apt/lists/*

# Download and install go-ipfs.
# We download the tarball, extract it, run the installer, and then clean up.
RUN wget "${GO_IPFS_DIST_URL}" -O /tmp/go-ipfs.tar.gz \
    && tar -C /tmp -xvzf /tmp/go-ipfs.tar.gz \
    && /tmp/go-ipfs/install.sh \
    && rm -rf /tmp/go-ipfs.tar.gz /tmp/go-ipfs

# Install the geohashtree python package from the specified GitHub repository.
# pip can install directly from a git repository.
RUN pip install --no-cache-dir git+https://github.com/easierdata/geohash-cid.git

# Create a directory for the IPFS repository data.
# This directory can be mounted as a volume to persist data across container restarts.
RUN mkdir -p ${IPFS_PATH}

# Expose standard IPFS ports.
# 4001: Swarm connections (libp2p)
# 5001: API server
# 8080: Gateway server
EXPOSE 4001
EXPOSE 5001
EXPOSE 8080

# Copy the entrypoint script into the image and make it executable.
# This script will run every time the container starts.
COPY entrypoint.sh /usr/local/bin/entrypoint.sh
RUN chmod +x /usr/local/bin/entrypoint.sh

# Set tini as the entrypoint. It will launch our script and manage the IPFS process.
ENTRYPOINT ["/usr/bin/tini", "--", "/usr/local/bin/entrypoint.sh"]

# The default command when the container runs is to start the daemon.
# This can be overridden when running `docker run`.
CMD ["daemon"]
