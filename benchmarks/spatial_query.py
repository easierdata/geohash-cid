

# import sys
# import logging

# class StreamToLogger(object):
#     """
#     Fake file-like stream object that redirects writes to a logger instance.
#     """
#     def __init__(self, logger, level):
#        self.logger = logger
#        self.level = level
#        self.linebuf = ''

#     def write(self, buf):
#        for line in buf.rstrip().splitlines():
#           self.logger.log(self.level, line.rstrip())

#     def flush(self):
#         pass

# logging.basicConfig(
#         level=logging.DEBUG,
#         format='%(asctime)s:%(levelname)s:%(name)s:%(message)s',
#         filename='out.log',
#         filemode='a'
#         )
# log = logging.getLogger('foobar')
# sys.stdout = StreamToLogger(log,logging.INFO)
# sys.stderr = StreamToLogger(log,logging.ERROR)


import pygeohash as pgh
import geopandas as gpd
import pandas as pd
from shapely.geometry import Point
import sys
import os
import time
from datetime import datetime
sys.path.append("../")
import warnings
warnings.filterwarnings('ignore')
from geohashtree.geohash_func import geohashes_covering_circle,h3tree_covering_circle, bounding_box
from geohashtree.trie import trim_hashes
from geohashtree.geohashtree import LiteTreeOffset,FullTreeFile,LiteTreeCID
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

gdf_rand_points = gpd.read_file("../data/maryland_demo/rand_dc_point.geojson")


centre = (gdf_rand_points.geometry.values[0].x,gdf_rand_points.geometry.values[0].y)

# precision = 5
# result_hashes = geohashes_covering_circle(*centre,radius,precision)
# print(len(result_hashes),len(trim_hashes(result_hashes)))
# result_hashes = trim_hashes(result_hashes)
#geohash config
approx_lat_to_meter = 111100
geohash_config = {
    'grid': 'geohash',
    'precision': 4,
    'radius': 0.05,
    'mode': 'offline',
    'index_cid': "bafybeifor6qebpdi3z3btw6degyghxgvzwgm6x33rrbdbcztgulq4g3n4i", #"bafybeiez5bwfmxmm2s36sx2rlzeasraxvao2h4swbrp4bft75d62ejv4su", 
    'local_index_path': "../data/test/us_places_gh_sorted/",
    'radius_factor': 1,
}

geojson_index_folder={
    5:{'local':"../data/test/us_places_gh_sorted_d5/",'cid':"bafybeiez5bwfmxmm2s36sx2rlzeasraxvao2h4swbrp4bft75d62ejv4su"},
    4:{'local':"../data/test/us_places_gh_sorted/",'cid':"bafybeifor6qebpdi3z3btw6degyghxgvzwgm6x33rrbdbcztgulq4g3n4i"},
    3:{'local':"../data/test/us_places_gh_sorted_d3/",'cid':"bafybeie5fs3x7wke4b7vnrk6wfgae43j6rlxg4airjefqd5dwey2xkrdbu"},
}
parquet_index_folder={
    5:{'local':"../data/test/us_places_cid_parq_d5/",'cid':"bafybeicttshfwol4g5qsepp3pnukmjwl6tols2i2xl2sxo2svgg2d2mq4y"},
    4:{'local':"../data/test/us_places_cid_parq/",'cid':"bafybeib6s2z535bu5545xfeadklpbauy3hskjv53t72ktz5iluaioltnxq"},
    3:{'local':"../data/test/us_places_cid_parq_d3/",'cid':"bafybeicruvq6vm62wpfuremivwr7eaoavnzsjcn25ujtzh6a3rg6jfcdla"},
}
select_precision = 3
geojson_config = {
    'grid': 'geohash',
    'precision': select_precision,
    'radius': 0.05,
    'mode': 'offline',
    'index_cid': geojson_index_folder[select_precision]['cid'],
    'local_index_path': geojson_index_folder[select_precision]['local'],
    'index_cid': "bafybeifor6qebpdi3z3btw6degyghxgvzwgm6x33rrbdbcztgulq4g3n4i", #"bafybeiez5bwfmxmm2s36sx2rlzeasraxvao2h4swbrp4bft75d62ejv4su", 
    'local_index_path': "../data/test/us_places_gh_sorted/",
    'radius_factor': 1,
}

parquet_config = {
    'grid': 'geohash',
    'precision':select_precision,
    'radius': 0.05,
    'mode': 'offline',
    'index_cid': parquet_index_folder[select_precision]['cid'],
    'local_index_path': parquet_index_folder[select_precision]['local'],
    'radius_factor': 1,
}
# h3 config
h3_config = {
    'grid': 'h3',
    'precision': 6,
    'radius': 0.05,
    'mode': 'offline',
    'index_cid': "bafybeiakrsbotrovau2syhfeerthkwqpopytap3onydyvvppky6gvcqn54",
    'local_index_path': "../data/test/us_places_h3_sorted/",
    'radius_factor': 111*1000,
}
#add support for both parquet and 
def run_query(config,centre,query_func):
    radius = config['radius']
    search_geo = gpd.GeoDataFrame({'geometry':[Point(*centre).buffer(radius)]},crs="EPSG:4326")
    t0 = time.time()
    result_hashes = query_hashes(config,*centre)
    t1 = time.time()
    qe = t1-t0
    #result_hashes = ['d']
    print('query started')
    retr,qt = query_func(result_hashes,config)
    t0 = time.time()
    result = gpd.sjoin(retr,search_geo)
    t1 = time.time()
    qj = t1-t0
    print(f'spatial joined with {qj:.2f} secs')

    print(f'query finished with feature number: {result.shape[0]}')
    return (qe,qt,qj,retr.shape[0],result.shape[0])

def run_query_parquet(config):
    result_hashes = query_hashes(config)
    #result_hashes = ['d']
    print('query started')
    detached_parquet(result_hashes,config)
    print('query finished')
    
def query_hashes(config,centre_x,centre_y):
    radius = config['radius']
    radius_proj = config['radius_factor']*radius
    precision = config['precision']

    if config['grid'] == 'geohash':
        result_hashes = geohashes_covering_circle(centre_x,centre_y,radius_proj,precision)
        result_hashes = trim_hashes(result_hashes)
    else:
        result_hashes = h3tree_covering_circle(centre_x,centre_y,radius_proj,precision)
    print(len(result_hashes))
    #print(result_hashes)
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
    t3 = time.time()
    print(f'index caching: {t1-t0:.2f}s')
    print(f'fuzzy count: {t2-t1:.2f}s')
    print(f'retrieval: {t3-t2:.2f}s')
    return retr


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
    t3 = time.time()
    print(f'index caching: {t1-t0:.2f}s')
    print(f'fuzzy count: {t2-t1:.2f}s')
    print(f'retrieval: {t3-t2:.2f}s')
    return retr,t3-t2

def detached_parquet(result_hashes,config):
    '''
    query parquet file using geohashes
    '''
    local_index_path = config['local_index_path']
    index_cid = config['index_cid']
    radius_grid = config['radius_factor']*config['radius']
    radius = config['radius']
    tree = LiteTreeCID()
    tree.file_format='parquet'
    t0 = time.time()
    
    if not os.path.exists(local_index_path):
        print('caching to',local_index_path)
        ipfs_get_index_folder(index_cid,local_index_path)

    t1 = time.time()
    #tree.count(result_hashes,local_index_path)
    t2 = time.time()
    retr = tree.retrieve(result_hashes,local_index_path)
    print('IPFS return size',retr.shape)
    t3 = time.time()

    
    print(f'index caching: {t1-t0:.2f}s')
    print(f'fuzzy count: {t2-t1:.2f}s')
    print(f'retrieval: {t3-t2:.2f}s')
    return retr , t3-t2

if __name__ == "__main__":
    #run_query(geohash_config)
    # run_query(h3_config)
    #run_query_parquet(parquet_config)

    #centre = (-119.1771,34.1975)
    file_format = sys.argv[1]
    r100 = gpd.read_file('../assets/us_100_random.geojson')
    repeat = 3
    
    # radius_meters=[100,150,200,250,300,350,400,450,500,550,600,800,1000,
    # #2000,5000,10000,20000,50000,100000,200000,300000,500000
    # ]
    radius_meters = []
    radius_meters += list(range(100,1000,100))
    radius_meters += list(range(1000,10000,1000))
    radius_meters += list(range(10000,100000,10000))
    radius_meters += list(range(100000,500000,50000))
    
    for r_m in radius_meters:
        r = r_m / approx_lat_to_meter
        
        
        
        for sel_prec in range(3,6):
            res = []
            parquet_config = {
                'grid': 'geohash',
                'precision':sel_prec,
                'radius': r,
                'mode': 'offline',
                'index_cid': parquet_index_folder[sel_prec]['cid'],
                'local_index_path': parquet_index_folder[sel_prec]['local'],
                'radius_factor': 1,
            }
            geojson_config = {
                'grid': 'geohash',
                'precision':sel_prec,
                'radius': r,
                'mode': 'offline',
                'index_cid': geojson_index_folder[sel_prec]['cid'],
                'local_index_path': geojson_index_folder[sel_prec]['local'],
                'radius_factor': 1,
            }

            config = geojson_config if file_format == 'geojson' else parquet_config
            execute_function = detached if file_format == 'geojson' else detached_parquet
            for i,row in r100.loc[[8,0,7],['id','x','y']].iterrows():
                print(row['id'])
                
                for _ in range(repeat):
                    centre = (row['x'],row['y'])
                    qe,qt,qj,fetch,valid = run_query(config,centre,execute_function)
                    res.append((sel_prec,row['id'],r_m,qe,qt,qj,fetch,valid))
                    time.sleep(1)
            record_df = pd.DataFrame(res,columns=['prec','id','radius_meter','hash_time','query_time','join_time','fetched','valid'])
            record_df.to_csv(f"output/Jun4/perf_{file_format}_{sel_prec}_{r_m}_{datetime.now().strftime('%b_%d_%H_%M_%S')}.csv",index=False)

