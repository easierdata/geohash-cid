#!/bin/bash
# filepath: run_geohashtree_get.sh

set -e

echo "Running geohashtree get with timing..."

time python -m geohashtree.cli get ./data/dump/geohashtree/ bafybeifunypta4qryaaaeqmh2xovboqje2ne3o5agv27wiaqax4gb7hh7e \
  --method=offset \
  --format=geojson \
  --level=3 \
  --radius 42.5524208 -71.2832169 50