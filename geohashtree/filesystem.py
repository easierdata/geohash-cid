'''
Filesystem module for geohashtree, including file I/O and IPFS operations.
'''
import subprocess
import pandas as pd
import geopandas as gpd
from io import StringIO
from geohashtree.config import ipfs_binary
print('ipfs path',ipfs_binary)
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
        result = subprocess.run([ipfs_binary,"cat",cid,"-o",str(offset),"-l",str(length)],stdout=subprocess.PIPE)
        data = result.stdout.decode()
        # Remove trailing comma from the second chunk
        data = data.rstrip(',\n')
        res.append(data)
        
    concatenated_data += res[0]+",".join(res[1:])
    concatenated_data += suffix_string
    return concatenated_data

def write_raw_json_to_file(geojson, file_path):
    with open(file_path, 'w') as file:
        file.write(geojson)
def ipfs_ready():
    """
    Check if IPFS is ready
    """
    result = subprocess.run([ipfs_binary,"swarm","addrs"],stdout=subprocess.PIPE)
    return not result.returncode

def compute_cid(file_path):
    """
    Compute the CID for a file
    """
    if not ipfs_ready():
        raise Exception("IPFS is not ready")
    cid = subprocess.check_output([ipfs_binary, "add", "-qn","--cid-version=1", file_path]).decode().strip()
    print(cid)
    return cid
def ipfs_add_feature(geojson_path):
    """
    Add a GeoJSON feature to IPFS
    """
    if not ipfs_ready():
        raise Exception("IPFS is not ready")
    try:
        if isinstance(geojson_path, list):
            return [ipfs_add_feature(geojson) for geojson in geojson_path]
        # Use IPFS add command to add the file to IPFS
        result = subprocess.run([ipfs_binary, 'add', '-q', '--cid-version=1', geojson_path], stdout=subprocess.PIPE, check=True, text=True)
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
        result = subprocess.run([ipfs_binary, 'add', '-r', '--cid-version=1', index_path], stdout=subprocess.PIPE, check=True, text=True)
        return result.stdout.strip()
    except Exception as e:
        print(f"An unexpected error occurred: {e}")
def ipfs_list_folder(cid):
    """
    List the contents of an IPFS folder
    """
    if not ipfs_ready():
        raise Exception("IPFS is not ready")
    try:
        # Use IPFS ls command to list the CID
        result = subprocess.run([ipfs_binary, 'ls', cid], stdout=subprocess.PIPE, check=True, text=True)
        return [row.split(" ")[-1] for row in result.stdout.strip().split('\n')]   
    except Exception as e:
        print(f"An unexpected error occurred: {e}")
def ipfs_check_folder(cid):
    """
    Check if a CID is a folder on IPFS
    """
    if not ipfs_ready():
        raise Exception("IPFS is not ready")
    try:
        # Use IPFS ls command to list the CID
        result = subprocess.run([ipfs_binary, 'ls', cid], stdout=subprocess.PIPE, check=True, text=True)
        if result.stdout.strip() == "":
            return False
        else:
            return True
    except Exception as e:
        return False
def ipfs_link_exists(cid):
    """
    Check if a CID exists on IPFS
    """
    if not ipfs_ready():
        raise Exception("IPFS is not ready")
    try:
        # Use IPFS ls command to list the CID
        result = subprocess.run([ipfs_binary, 'ls', cid], stdout=subprocess.PIPE, check=True, text=True)
        return True
    except subprocess.CalledProcessError as e:
        return False
    except Exception as e:
        print(f"An unexpected error occurred: {e}")
        return False

def ipfs_cat_file(cid):
    """
    Retrieve a file from IPFS using its CID
    """
    if not ipfs_ready():
        raise Exception("IPFS is not ready")
    try:
        # Use IPFS cat command to stream the file content
        result = subprocess.run([ipfs_binary, 'cat', cid], stdout=subprocess.PIPE, check=True, text=True)
        return result.stdout
    except Exception as e:
        print(f"An unexpected error occurred: {e}")
        return None
def ipfs_get_feature(cid):
    """
    Retrieve a GeoJSON feature from IPFS using its CID
    """
    cat_string = ipfs_cat_file(cid)
    if cat_string:
        return gpd.read_file(StringIO(cat_string))
    else:
        return None

def ipfs_get_index_folder(cid,index_path):
    """
    Retrieve an index folder from IPFS using its CID
    """
    if not ipfs_ready():
        raise Exception("IPFS is not ready")
    try:
        # Use IPFS get command to retrieve the folder and save to index path
        result = subprocess.run([ipfs_binary, 'get', cid, '-o', index_path], stdout=subprocess.PIPE, check=True, text=True)
        return result.stdout
    except Exception as e:
        print(f"An unexpected error occurred: {e}")
        return None