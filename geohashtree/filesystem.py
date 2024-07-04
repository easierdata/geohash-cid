'''
Filesystem module for geohashtree, including file I/O and IPFS operations.
'''
from abc import ABC, abstractmethod
import subprocess
import pandas as pd
import geopandas as gpd
from io import StringIO
from geohashtree.config import ipfs_binary
import requests
import os
import tqdm
import json

class FileSystem(ABC):
    @abstractmethod
    def listdir(self, path):
        pass

    @abstractmethod
    def path_isdir(self, path):
        pass

    @abstractmethod
    def path_exists(self, path):
        pass
    
    @abstractmethod
    def readlines(self, path):
        pass

class LocalFS(FileSystem):
    def listdir(self, path):
        return os.listdir(path)

    def path_isdir(self, path):
        return os.path.isdir(path)

    def path_exists(self, path):
        return os.path.exists(path)
    
    def readlines(self, path):
        with open(path, 'r', encoding='utf-8') as file:
            lines = file.readlines()
        return lines
    
class InterPlanetaryFS(FileSystem):

    def __init__(self,rpc_url="http://127.0.0.1:5001/api/v0/"):
        self.url = rpc_url
    def listdir(self, path):
        response = requests.post(self.url+'ls', params={'arg': path})
        return [c['Name'] for c in response.json()['Objects'][0]['Links']]

    def path_isdir(self, path):
        response = requests.post(self.url+'ls', params={'arg': path})
        return len(response.json()['Objects'][0]['Links']) > 0

    def path_exists(self, path):
        response = requests.post(self.url+'ls', params={'arg': path})
        return not 'Type' in response.json()
    
    def readlines(self, path):
        response = requests.post(self.url+'cat', params={'arg': path})
        return response.content.decode().split("\n")
    
def ipfs_ready():
    """
    Check if IPFS is ready
    """
    result = subprocess.run([ipfs_binary,"swarm","addrs"],stdout=subprocess.PIPE)
    return not result.returncode

def kubo_rpc_cat_offset_length(cid,offset,length):
    """
    Retrieve a file from IPFS using its CID
    """
    # Define the URL
    url = "http://127.0.0.1:5001/api/v0/cat"
    # Define the query parameters
    params = {
        'arg': cid,
        'offset':offset,
        'length':length
    }

    # Make the POST request
    response = requests.post(url, params=params)
    return response.content

def kubo_rpc_cat(cid):
    """
    Retrieve a file from IPFS using its CID
    """
    # Define the URL
    url = "http://127.0.0.1:5001/api/v0/cat"
    # Make the POST request
    response = requests.post(url, params={'arg': cid})
    return response.content

def kubo_cli_cat_offset_length(cid,offset,length):
    """
    Retrieve a file from IPFS using its CID
    """
    result = subprocess.run([ipfs_binary,"cat",cid,"-o",str(offset),"-l",str(length)],stdout=subprocess.PIPE)
    return result.stdout
def combine_tuples(tuples):
    combined = []  # Initialize an empty list to hold the combined tuples
    for offset, length in tuples:
        if combined and combined[-1][0] + combined[-1][1] == offset:
            # If the current tuple can be combined with the last one in the combined list,
            # update the last tuple's length to include the current tuple's length
            combined[-1] = (combined[-1][0], combined[-1][1] + length)
        else:
            # If the current tuple cannot be combined with the last one, add it as a new tuple
            combined.append((offset, length))
    return combined

def extract_and_concatenate(file_path, chunks, suffix_string = "]\n}"):
    concatenated_data = ""
    with open(file_path, 'r') as file:
        res = []
        for i, (offset, length) in enumerate(chunks):
            file.seek(offset)
            data = file.read(length).decode()
            # Remove trailing comma from the second chunk
            
            data = data.rstrip(',\n')
            res.append(data)
        
        concatenated_data += res[0]+",".join(res[1:])

    concatenated_data += suffix_string
    return concatenated_data
def parallel_cat(params):
    cid, offset, length = params
    return kubo_rpc_cat_offset_length(cid, offset, length).decode().rstrip(',\n')
def extract_and_concatenate_from_ipfs(cid, chunks, suffix_string = "]\n}"):
    concatenated_data = ""
    res = []
    #read header
    offset, length = chunks[0]
    print(offset,length)
    header = kubo_rpc_cat_offset_length(cid, offset, length).decode().rstrip(',\n')
    
    feature_chunks = combine_tuples(chunks[1:])
    print('comb',len(feature_chunks))
    
    # #multiprocessing
    # params = [(cid,offset,length) for offset,length in feature_chunks]
    # from multiprocessing import Pool
    
    # with Pool(8) as pool:
    #     res = pool.map(parallel_cat, params)

    #single loop
    for i, (offset, length) in enumerate(feature_chunks):
        content = kubo_rpc_cat_offset_length(cid,offset,length)
        data = content.decode()
        # Remove trailing comma from the second chunk
        data = data.rstrip(',\n')
        res.append(data)

    concatenated_data += header+",".join(res)
    concatenated_data += suffix_string
    return concatenated_data

def write_raw_json_to_file(geojson, file_path):
    with open(file_path, 'w') as file:
        file.write(geojson)


def compute_cid(file_path):
    """
    Compute the CID for a file
    """

    url = "http://127.0.0.1:5001/api/v0/add"
    # Make the POST request
    with open(file_path, mode='rb') as f:
        r = requests.post(url='http://127.0.0.1:5001/api/v0/add?cid-version=1&only-hash=True',
                          files={file_path: f})
    
    cid = json.loads(r.content)['Hash']
    return cid

def compute_cid_old(file_path):
    """
    Compute the CID for a file
    """
    if not ipfs_ready():
        raise Exception("IPFS is not ready")
    cid = subprocess.check_output([ipfs_binary, "add", "-qn","--cid-version=1", file_path]).decode().strip()
    print(cid)
    return cid
def ipfs_add_feature(geojson_path):
    try:
        if isinstance(geojson_path, list):
            result_list = []
            for geojson in tqdm.tqdm(geojson_path):
                result_list.append(ipfs_add_feature(geojson))
            return result_list
        # Use IPFS add command to add the file to IPFS
        with open(geojson_path, mode='rb') as f:
            r = requests.post(url='http://127.0.0.1:5001/api/v0/add?cid-version=1&quiet=True',
                            files={geojson_path: f})
        
        return r.content
    except Exception as e:
        print(f"An unexpected error occurred: {e}")
def ipfs_add_feature_old(geojson_path):
    """
    Add a GeoJSON feature to IPFS
    """
    if not ipfs_ready():
        raise Exception("IPFS is not ready")
    try:
        if isinstance(geojson_path, list):
            result_list = []
            for geojson in tqdm.tqdm(geojson_path):
                result_list.append(ipfs_add_feature(geojson))
            return result_list
        # Use IPFS add command to add the file to IPFS
        result = subprocess.run([ipfs_binary, 'add', '-q', '--cid-version=1', geojson_path], stdout=subprocess.PIPE, check=True, text=True)
        return result.stdout.strip()
    except Exception as e:
        print(f"An unexpected error occurred: {e[:100]}")

def ipfs_add_index_folder(index_path):
    """
    Add an index folder to IPFS
    """
    if not ipfs_ready():
        raise Exception("IPFS is not ready")
    try:
        # Use IPFS add command to add the file to IPFS
        result = subprocess.run([ipfs_binary, 'add', '-Qr', '--cid-version=1', index_path], stdout=subprocess.PIPE, check=True, text=True)
        return result.stdout.strip()
    except Exception as e:
        print(f"An unexpected error occurred: {e[:100]}")
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
        print(f"An unexpected error occurred: {e[:100]}")
def ipfs_rpc_list_folder(cid):
    """
    List the contents of an IPFS folder
    """
    url = "http://127.0.0.1:5001/api/v0/ls"
    # Make the POST request
    response = requests.post(url, params={'arg': cid})
    return [c['Name'] for c in response.json()['Objects'][0]['Links']]
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
def ipfs_rpc_check_folder(cid):
    """
    Check if a CID is a folder on IPFS
    """
    url = "http://127.0.0.1:5001/api/v0/ls"
    # Make the POST request
    response = requests.post(url, params={'arg': cid})
    return len(response.json()['Objects'][0]['Links']) > 0

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
        print(f"An unexpected error occurred: {e[:100]}")
        return False
def ipfs_rpc_link_exists(cid):
    """
      Check if a CID exists on IPFS with RPC
    """
    url = "http://127.0.0.1:5001/api/v0/ls"
    # Make the POST request
    response = requests.post(url, params={'arg': cid})
    return not 'Type' in response.json()

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
        print(f"An unexpected error occurred: {e[:100]}")
        return None
def ipfs_get_feature(cid):
    """
    Retrieve a GeoJSON feature from IPFS using its CID
    """
    content = kubo_rpc_cat(cid)
    if content:
        return gpd.read_file(StringIO(content.decode()))
    else:
        return None
def ipfs_get_parquet(cid):
    """
    Retrieve a parquet file from IPFS using its CID
    """
    import io
    pq_bytes = kubo_rpc_cat(cid)
    
    if pq_bytes:
        pq_file = io.BytesIO(pq_bytes)

        return gpd.read_parquet(pq_file)
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
        print(f"An unexpected error occurred: {e[:100]}")
        return None