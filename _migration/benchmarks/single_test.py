import pygeohash as pgh
##load geojson
import geopandas as gpd
import pandas as pd
import os
import sys
import time
sys.path.append("../")
from geohashtree.geohashtree import LiteTreeOffset, LiteTreeCID, FullTreeFile
from geohashtree.filesystem import ipfs_add_feature,ipfs_add_index_folder,kubo_rpc_cat_offset_length
from geohashtree.geohash_func import geohashes_covering_circle, bounding_box

def get_query_center(radius):
    r100 = gpd.read_file('../assets/us_100_random.geojson')
    gdf_rand_points = r100.loc[[7]][['geometry']]
    centre = (gdf_rand_points.geometry.values[0].x,gdf_rand_points.geometry.values[0].y)
    gdf_radius = gpd.GeoDataFrame({'geometry':gdf_rand_points.buffer(radius)})
    return centre,gdf_radius
     
asset = "us_places_gh_sorted_rg32k"
geojson_path = f"../data/overture/{asset}.geojson"
parquet_path = f"../data/overture/{asset}.parquet"



# tree = LiteTreeCID()
# tree.file_format = 'parquet'
# index_path = f"../data/test/us_places_cid_parq/"


#tree = LiteTreeOffset()
#tree.file_format = 'parquet'
#fmt = 'parq'
#index_path = f"../data/geohash_offset_{asset}_{fmt}_d{prec}/index"

# tree = LiteTreeOffset()
# tree.file_format = 'geojson'
# index_path = f"../data/test/us_places_gh_sorted/"

tree = LiteTreeCID()
tree.file_format = 'geojson'
index_path = "../data/test/us_places_cid_d4/"

prec = 4

radius = 0.3
centre,gdf_radius = get_query_center(radius)
t0=time.time()
result_hashes = geohashes_covering_circle(*centre,radius,prec)
t1=time.time()
qe = t1-t0

retr = tree.retrieve(result_hashes,index_path)
#retr.to_parquet('single_test.parquet')
#retr.to_file('single_test.geojson',driver="GeoJSON")
t1=time.time()

normalized = gpd.sjoin(retr,gdf_radius)
t2=time.time()

print(normalized.shape,t2-t1,qe,t2-t0)