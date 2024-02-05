'''
Filesystem module for geohashtree, including file I/O and filesystem operations.
'''

def get_geojson_path_from_cid(cid):
    """
    Get the geojson file from the CID
    """
    return f"/mnt/data/{cid}/geojson.geojson"