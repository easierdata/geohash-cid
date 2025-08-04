#!/bin/bash
# filepath: run_geohashtree_create_and_copy.sh

set -e

echo "Running geohashtree create and copy with timing..."
time python -m geohashtree.cli create ./data/overture/us_places.geojson ./data/geohashtree/ \
  --method=offset \
  --format=geojson \
  --level=3

time python -m geohashtree.cli copy ./data/overture/us_places.geojson --to_ipfs