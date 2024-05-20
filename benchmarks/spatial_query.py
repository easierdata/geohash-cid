import pygeohash as pgh
import geopandas as gpd
import pandas as pd
import sys
import os
import time
sys.path.append("../")
import warnings
warnings.filterwarnings('ignore')
from geohashtree.geohash_func import geohashes_covering_circle, bounding_box
from geohashtree.trie import trim_hashes
from geohashtree.geohashtree import LiteTreeOffset,FullTreeFile
from geohashtree.filesystem import ipfs_get_index_folder,extract_and_concatenate_from_ipfs

#define dataset to query
#local_index_path = "../data/test/offset_index_1m"
#local_index_path = "../data/test/us_places_gh_sorted_d5"
local_index_path = "../data/test/us_places_gh_sorted"
# cid = "bafybeiawrnzlzeuyzwkgoaugf5gh7jxuydzwqj5f4nvyigme5hdgndqp6e"
#index_cid = "bafybeiez5bwfmxmm2s36sx2rlzeasraxvao2h4swbrp4bft75d62ejv4su" #height5
index_cid = "bafybeifor6qebpdi3z3btw6degyghxgvzwgm6x33rrbdbcztgulq4g3n4i" #height4
#attached_cid="bafybeiftmh7tom3qxsw6rtqw5ntxtsasvowpxmxsmwedaze62rlkwjfliy"
# #restaurants
# local_index_path = "../data/test/offset_index_dc"
# cid = "bafybeia7jlkyzt22qgh66felldr322l5c7jdvm4xspr222f4usnd2ttbze"
# attached_cid="bafybeiez73pdnpevvoptzft54zmljj54di2pp44slk67wfeo7uz5hdbqoe"
# index_cid = "bafybeib6ex2onm2wk5wxrdfe73l4m3la3c35nr5f4b4trbtidqaofduqiu"
mode = 'online'
mode = 'offline'
#define query
gdf_rand_points = gpd.read_file("../data/maryland_demo/rand_dc_point.geojson")


rl = [x for x in range(4,60)]
rl = [0.05,0.5]
radius = 0.7
centre = (gdf_rand_points.geometry.values[0].x,gdf_rand_points.geometry.values[0].y)
precision = 4
# result_hashes = geohashes_covering_circle(*centre,radius,precision)

# print(len(result_hashes),len(trim_hashes(result_hashes)))

# result_hashes = trim_hashes(result_hashes)
#result_hashes = ['d']
print('query started')
# execute time
def attached(result_hashes,radius):
    tree = FullTreeFile()
    t0 = time.time()

    local_index_path = attached_cid
    t1 = time.time()
    #tree.count(result_hashes,local_index_path)
    t2 = time.time()
    retr = tree.retrieve(result_hashes,local_index_path)
    gdf_radius = gpd.GeoDataFrame({'geometry':gdf_rand_points.buffer(radius)})
    result = gpd.sjoin(retr,gdf_radius)
    t3 = time.time()
    print(f'query finished with feature number: {result.shape[0]}')
    print(f'index caching: {t1-t0:.2f}s')
    print(f'fuzzy count: {t2-t1:.2f}s')
    print(f'exact query: {t3-t2:.2f}s')

def detached(result_hashes,radius):
    global local_index_path
    tree = LiteTreeOffset(mode=mode)
    t0 = time.time()
    if tree.mode == 'offline':
        if not os.path.exists(local_index_path):
            print('caching to',local_index_path)
            ipfs_get_index_folder(index_cid,local_index_path)
    else:
        local_index_path = index_cid
    t1 = time.time()
    #tree.count(result_hashes,local_index_path)
    t2 = time.time()
    retr = tree.retrieve(result_hashes,local_index_path)
    gdf_radius = gpd.GeoDataFrame({'geometry':gdf_rand_points.buffer(radius)})
    result = gpd.sjoin(retr,gdf_radius)
    t3 = time.time()

    print(f'query finished with feature number: {result.shape[0]}')
    print(f'index caching: {t1-t0:.2f}s')
    print(f'fuzzy count: {t2-t1:.2f}s')
    print(f'exact query: {t3-t2:.2f}s')

for r in rl:
    print(r)
    result_hashes = geohashes_covering_circle(*centre,r,precision)
    print(len(result_hashes),len(trim_hashes(result_hashes)))
    result_hashes = trim_hashes(result_hashes)
    detached(result_hashes,r)