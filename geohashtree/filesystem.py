'''
Filesystem module for geohashtree, including file I/O and IPFS operations.
'''
import subprocess
import pandas as pd
import geopandas as gpd
from io import StringIO

def get_geojson_path_from_cid(cid):
    """
    Get the geojson file from the CID
    """

    return f"../assets/dc_restaurants.geojson"

def extract_and_concatenate(file_path, chunks, suffix_string = "]\n}"):
    concatenated_data = ""
    with open(file_path, 'r') as file:
        res = []
        for i, (offset, length) in enumerate(chunks):
            file.seek(offset)
            data = file.read(length)
            # Remove trailing comma from the second chunk
            
            data = data.rstrip(',\n')
            res.append(data)
        
        concatenated_data += res[0]+",".join(res[1:])

    concatenated_data += suffix_string
    return concatenated_data

def extract_and_concatenate_from_ipfs(cid, chunks, suffix_string = "]\n}"):
    concatenated_data = ""
    res = []
    for i, (offset, length) in enumerate(chunks):
        result = subprocess.run(["ipfs","cat",cid,"-o",offset,"-l",length],stdout=subprocess.PIPE)
        data = result.stdout.decode()
        # Remove trailing comma from the second chunk
        data = data.rstrip(',\n')
        res.append(data)
        
    concatenated_data += res[0]+",".join(res[1:])
    concatenated_data += suffix_string
    return concatenated_data

def ipfs_ready():
    """
    Check if IPFS is ready
    """
    result = subprocess.run(["ipfs","swarm","addrs"],stdout=subprocess.PIPE)
    return not result.returncode

def compute_cid(file_path):
    """
    Compute the CID for a file
    """
    if not ipfs_ready():
        raise Exception("IPFS is not ready")
    cid = subprocess.check_output(["ipfs", "add", "-qn","--cid-version=1", file_path]).decode().strip()
    return cid
def ipfs_add_feature(geojson_path):
    """
    Add a GeoJSON feature to IPFS
    """
    if not ipfs_ready():
        raise Exception("IPFS is not ready")
    try:
        # Use IPFS add command to add the file to IPFS
        result = subprocess.run(['ipfs', 'add', '-q', '--cid-version=1', geojson_path], stdout=subprocess.PIPE, check=True, text=True)
        return result.stdout.strip()
    except Exception as e:
        print(f"An unexpected error occurred: {e}")

def ipfs_add_index_folder(index_path):
    """
    Add an index folder to IPFS
    """
    if not ipfs_ready():
        raise Exception("IPFS is not ready")
    try:
        # Use IPFS add command to add the file to IPFS
        result = subprocess.run(['ipfs', 'add', '-r', '-q', '--cid-version=1', index_path], stdout=subprocess.PIPE, check=True, text=True)
        return result.stdout.strip()
    except Exception as e:
        print(f"An unexpected error occurred: {e}")

def ipfs_get_feature(cid):
    """
    Retrieve a GeoJSON feature from IPFS using its CID
    """
    if not ipfs_ready():
        raise Exception("IPFS is not ready")
    
    try:
        # Use IPFS cat command to stream the file content
        result = subprocess.run(['ipfs', 'cat', cid], stdout=subprocess.PIPE, check=True, text=True)
        # Use StringIO to convert string to a file-like object for pandas
        json_content = StringIO(result.stdout)
        # Load the JSON content into a DataFrame
        gdf = gpd.read_file(json_content)
        return gdf
    except Exception as e:
        print(f"An unexpected error occurred: {e}")