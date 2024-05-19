import pygeohash as pgh
import geopandas as gpd
import pandas as pd
import sys
import os
import time
sys.path.append("../")
import warnings
warnings.filterwarnings('ignore')
from geohashtree.geohash_func import geohashes_covering_circle,h3tree_covering_circle, bounding_box
from geohashtree.trie import trim_hashes
from geohashtree.geohashtree import LiteTreeOffset,FullTreeFile
from geohashtree.filesystem import ipfs_get_index_folder,extract_and_concatenate_from_ipfs

#define dataset to query
#local_index_path = "../data/test/offset_index_1m"
#local_index_path = "../data/test/us_places_gh_sorted_d5"
#local_index_path = "../data/test/geohash_offset_dc_restaurants_h3_sorted/"
# cid = "bafybeiawrnzlzeuyzwkgoaugf5gh7jxuydzwqj5f4nvyigme5hdgndqp6e"
#index_cid = "bafybeiez5bwfmxmm2s36sx2rlzeasraxvao2h4swbrp4bft75d62ejv4su"
#index_cid = "bafybeiakrsbotrovau2syhfeerthkwqpopytap3onydyvvppky6gvcqn54"
#attached_cid="bafybeiftmh7tom3qxsw6rtqw5ntxtsasvowpxmxsmwedaze62rlkwjfliy"
# #restaurants
#local_index_path = "../data/test/offset_index_dc"
# cid = "bafybeia7jlkyzt22qgh66felldr322l5c7jdvm4xspr222f4usnd2ttbze"
# attached_cid="bafybeiez73pdnpevvoptzft54zmljj54di2pp44slk67wfeo7uz5hdbqoe"
#index_cid = "bafybeib6ex2onm2wk5wxrdfe73l4m3la3c35nr5f4b4trbtidqaofduqiu"
# mode = 'online'
# mode = 'offline'
#define query
gdf_rand_points = gpd.read_file("../../data/maryland_demo/rand_dc_point.geojson")

centre = (gdf_rand_points.geometry.values[0].x,gdf_rand_points.geometry.values[0].y)

# precision = 5
# result_hashes = geohashes_covering_circle(*centre,radius,precision)
# print(len(result_hashes),len(trim_hashes(result_hashes)))
# result_hashes = trim_hashes(result_hashes)
#geohash config
geohash_config = {
    'profile_name': 'geohash',
    'precision': 5,
    'radius': 0.05,
    'mode': 'offline',
    'index_cid': "bafybeigoveu253bxkmz3qbhxjn6kqju5vrcqtjqm2wqrhtxtmaxck5g2ci",
    'local_index_path': "../data/test/geohash_offset_dc_restaurants_gh_sorted/",
    'radius_factor': 1,
}

# h3 config
h3_config = {
    'profile_name': 'h3',
    'precision': 6,
    'radius': 0.05,
    'mode': 'offline',
    'index_cid': "bafybeiakrsbotrovau2syhfeerthkwqpopytap3onydyvvppky6gvcqn54",
    'local_index_path': "../data/test/geohash_offset_dc_restaurants_h3_sorted/",
    'radius_factor': 111*1000,
}
def run_query(config):
    result_hashes = query_hashes(config)
    #result_hashes = ['d']
    print('query started')
    detached(result_hashes,config)
    print('query finished')

def query_hashes(config):
    radius = config['radius']
    radius_proj = config['radius_factor']*radius
    precision = config['precision']

    if config['profile_name'] == 'geohash':
        result_hashes = geohashes_covering_circle(*centre,radius_proj,precision)
        result_hashes = trim_hashes(result_hashes)
    else:
        result_hashes = h3tree_covering_circle(*centre,radius_proj,precision)
    print(len(result_hashes))
    print(result_hashes)
    #result_hashes = ['d']
    return result_hashes
# execute time
def attached(result_hashes,config):
    tree = FullTreeFile()
    t0 = time.time()
    radius = config['radius']
    radius_grid = config['radius_factor']*config['radius']
    local_index_path = config['local_index_path']
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

def detached(result_hashes,config):
    local_index_path = config['local_index_path']
    index_cid = config['index_cid']
    radius_grid = config['radius_factor']*config['radius']
    radius = config['radius']
    tree = LiteTreeOffset(mode=config['mode'])
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
    print('IPFS return size',retr.shape)
    gdf_radius = gpd.GeoDataFrame({'geometry':gdf_rand_points.buffer(radius)})
    result = gpd.sjoin(retr,gdf_radius)
    t3 = time.time()

    print(f'query finished with feature number: {result.shape[0]}')
    print(f'index caching: {t1-t0:.2f}s')
    print(f'fuzzy count: {t2-t1:.2f}s')
    print(f'exact query: {t3-t2:.2f}s')

if __name__ == "__main__":
    run_query(geohash_config)
    run_query(h3_config)