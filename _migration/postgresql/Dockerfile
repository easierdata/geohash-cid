FROM postgres:latest
RUN apt-get update \
    && apt-get install -y postgresql-13-postgis-3 \
    && rm -rf /var/lib/apt/lists/*
