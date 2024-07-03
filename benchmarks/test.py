import pygeohash as pgh
##load geojson
import geopandas as gpd
import pandas as pd
import os
import sys
sys.path.append("../")
from geohashtree.geohashtree import LiteTreeOffset, LiteTreeCID, FullTreeFile
from geohashtree.filesystem import ipfs_add_feature,ipfs_add_index_folder,kubo_rpc_cat_offset_length

asset = "us_places_gh_sorted"
geojson_path = f"../data/overture/{asset}.geojson"
parquet_path = f"../data/overture/{asset}.parquet"

offsettree = LiteTreeOffset()
offsettree.file_format = 'parquet'
fmt = 'parq'
prec = 3
index_path = f"../data/geohash_offset_{asset}_{fmt}_d{prec}/index"
geohashes = ['dqc']
#qr = offsettree.query(geohashes,index_path)
gdf = offsettree.retrieve(geohashes,index_path)
print(gdf.shape)