from abc import ABC, abstractmethod
import geopandas as gpd
import pandas as pd
import pygeohash as pgh
import ast
import os
from .trie import Trie
from .util import merge_dict,compose_path
from .filesystem import *

def append_geohash_to_dataframe(df,precision=4):
    """
    Append geohash to a dataframe
    """
    df = pd.concat([df,df.get_coordinates()],axis=1)
    df['geohash'] = df.apply(lambda row: pgh.encode(row['y'], row['x'],precision), axis=1)
    return df

def splitting_dataframe_to_files(df, target_directory,bucket_size = 1):
    # Initialize an empty list to store file paths
    file_paths = []
    # Make sure the directory exists, if not create it
    if not os.path.exists(target_directory):
        os.makedirs(target_directory)
    if bucket_size == 1:
        # Loop through each row in GeoDataFrame
        for index, row in df.iterrows():
            # Slice the GeoDataFrame to get a single feature (row)
            single_feature_gdf = df.iloc[[index]]

            # Get 'osm_id' for the single feature
            osm_id = row['osm_id']

            # Define the full file path
            file_path = os.path.join(target_directory, f"{osm_id}.geojson")

            # Save single feature GeoDataFrame as GeoJSON
            single_feature_gdf.to_file(file_path, driver="GeoJSON")

            # Append file_path to list
            file_paths.append(file_path)

        # Create a new column in the original GeoDataFrame to store file paths
        df['single_path'] = file_paths

    return df
class GeohashTree(ABC):
    @abstractmethod
    def add_from_geojson(self, geojson):
        pass

    @abstractmethod
    def generate_tree_index(self):
        pass

    @abstractmethod
    def query(self, geohashes):
        pass

class LiteTreeCID(GeohashTree):
    def add_from_geojson(self, geojson, target_directory):
        """
        Add a GeoJSON file to the index tree
        """
        features = gpd.read_file(geojson)
        features = append_geohash_to_dataframe(features)
        features = splitting_dataframe_to_files(features, target_directory,bucket_size = 1)
        features['single_cid'] = features.apply(lambda x: compute_cid(x['single_path']),axis=1)
        pairs = list(zip(features['geohash'],features['single_cid']))
        # Create an empty Trie dictionary
        self.trie_dict = Trie()
        # Insert each index-value pair into the Trie dictionary
        for index, value in pairs:
            self.trie_dict.insert(index, value)

    def export_trie(self,trie_node,geohash,root_path):
        #export geojson at current hash level
        next_path = root_path+"/"+"".join(geohash)
        leaf_path = root_path+f"/{geohash}.txt"
        if trie_node.value:
            # Open a file in write mode
            with open(leaf_path, 'w') as f:
                for item in trie_node.value:
                    f.write(f"{item}\n")
        #make path and export to sub folder
        import os 
        if trie_node.children and not os.path.exists(next_path):
            os.makedirs(next_path)
        for ch in trie_node.children:
            child_hash = geohash+ch
            self.export_trie(trie_node.children[ch],child_hash,next_path)

    def generate_tree_index(self,destination_path):
        # Implementation specific to Backend2
        self.export_trie(self.trie_dict.root,"",destination_path)
    def process_leaf_node(self,leaf):
        """
        process index leaf.
        leaf: txt file path of a index leaf, like a//ab/abc.txt
        """
        with open(leaf, 'r', encoding='utf-8') as file:
            lines = file.readlines()
        return [line.strip() for line in lines]

    def traverse_sub_node(self,node):
        """
        recursively collect all the leaf node under the current node
        """
        import os
        
        results=[]
        excludes = [".ipynb_checkpoints"]
        # Get list of items in the directory
        subfolders = [d for d in os.listdir(node) if os.path.isdir(os.path.join(node, d)) and d not in excludes]
        # If there are subfolders, traverse them
        if subfolders:
            for subfolder in subfolders:
                results.extend(self.traverse_sub_node(os.path.join(node, subfolder)))
        else:
            # Otherwise, process txt files in the directory
            txt_files = [f for f in os.listdir(node) if f.endswith('.txt')]
            for txt_file in txt_files:
                results.extend(self.process_leaf_node(os.path.join(node, txt_file)))
        return results
    def query_single(self,geohash,index_root):
        """
        find matching geohash or sub-level hashs
        """
        import os
        target_path = compose_path(geohash,index_root)
        cid_list = []
        if os.path.exists(target_path):
            cid_list = self.traverse_sub_node(target_path)
        if os.path.exists(target_path+'.txt'):
            cid_list = self.process_leaf_node(target_path+'.txt')
        return cid_list
    def query(self, geohashes,index_root):
        results = []
        for nei in geohashes:
            query = self.query_single(nei,index_root)
            if query:
                results.extend(query)
        return results
    def retrieve(self,geohashes,index_root):
        results = self.query(geohashes,index_root)
        ipfs_retrieval = pd.concat([ipfs_get_feature(cid) for cid in results])
        return ipfs_retrieval
    
class LiteTreeOffset(GeohashTree):
    def __init__(self,mode="offline"):
        self.mode = mode
        self.fs = InterPlanetaryFS() if mode == "online" else LocalFS()
        print(f"Index Mode: {mode}")
        self.offsets = []
    def calculate_offsets_and_lengths_stream(self,geojson_file_path):
        def count_char_outside_quotes(line, char):
            count = 0
            in_quotes = False
            escape_next = False  # Track whether the next character is escaped
            
            for c in line:
                # If the current character is a backslash and escape_next is False, toggle escape_next
                if c == '\\' and not escape_next:
                    escape_next = True
                    continue
                elif c == '\"' and not escape_next:
                    # Toggle in_quotes if not preceded by an unescaped backslash
                    in_quotes = not in_quotes
                elif c == char and not in_quotes:
                    # Count the char if it's not inside quotes
                    count += 1
                escape_next = False if escape_next else escape_next  # Reset escape_next if it was True
            
            return count
        offsets_and_lengths = []
        feature_start_token = '"type": "Feature"'
        
        with open(geojson_file_path, 'r') as file:
            feature_start = None
            brace_count = 0

            while True:
                # Track the current position
                current_position = file.tell()

                # Read the next line
                line = file.readline()

                if not line:
                    break  # End of file

                # Check for the start of a feature
                if feature_start_token in line and feature_start is None:
                    feature_start = current_position
                    brace_count = 0

                # Count braces to find the end of the feature
                if feature_start is not None:
                    brace_count += count_char_outside_quotes(line,'{')
                    brace_count -= count_char_outside_quotes(line,'}')

                # When the braces balance out, we've found the end of the feature
                if feature_start is not None and brace_count == 0:
                    feature_end = current_position + len(line.encode())
                    offsets_and_lengths.append((feature_start, feature_end - feature_start))
                    feature_start = None

        return offsets_and_lengths
    
    
    
    def export_trie(self,trie_node,geohash,root_path):
        #export geojson at current hash level
        next_path = root_path+"/"+"".join(geohash)
        leaf_path = root_path+f"/{geohash}.txt"
        cid = self.CID if self.CID else "[CID placeholder]"
        if trie_node.value:
            # Open a file in write mode
            with open(leaf_path, 'w') as f:
                f.write(f"{cid}\n{self.head_offset_length}\n")
                for item in trie_node.value:
                    f.write(f"{item}\n")
        #make path and export to sub folder
        import os 
        if trie_node.children and not os.path.exists(next_path):
            os.makedirs(next_path)
        for ch in trie_node.children:
            child_hash = geohash+ch
            self.export_trie(trie_node.children[ch],child_hash,next_path)
    def add_from_geojson(self, geojson,precision=4):
        # Implementation specific to Backend2
        offsets_lengths = self.calculate_offsets_and_lengths_stream(geojson)
        features = gpd.read_file(geojson)
        features['offlen'] = offsets_lengths
        self.head_offset_length = (0,offsets_lengths[0][0])
        features = append_geohash_to_dataframe(features,precision)
        pairs = list(zip(features['geohash'],features['offlen']))
        # Create an empty Trie dictionary
        self.trie_dict = Trie()
        # Insert each index-value pair into the Trie dictionary
        for index, value in pairs:
            self.trie_dict.insert(index, value)
        self.CID = compute_cid(geojson)

    def generate_tree_index(self,destination_path):
        # Implementation specific to Backend2
        self.export_trie(self.trie_dict.root,"",destination_path)
    
    def process_leaf_node(self,leaf):
        """
        process index leaf. [TODO]
        leaf: txt file path of a index leaf, like a//ab/abc.txt
        """
        lines = self.fs.readlines(leaf)
        return {lines[0].strip():[line.strip() for line in lines[1:] if line.strip()] }

    def traverse_sub_node(self,node):
        """
        recursively collect all the leaf node under the current node
        """
        import os
        results={}
        excludes = [".ipynb_checkpoints"]
        # Get list of items in the directory
        subfolders = [d for d in self.fs.listdir(node) if self.fs.path_isdir(os.path.join(node, d)) and d not in excludes]
        # If there are subfolders, traverse them
        if subfolders:
            for subfolder in subfolders:
                results = merge_dict(results,self.traverse_sub_node(os.path.join(node, subfolder)))
        else:
            # Otherwise, process txt files in the directory
            txt_files = [f for f in self.fs.listdir(node) if f.endswith('.txt')]
            for txt_file in txt_files:
                results = merge_dict(results,self.process_leaf_node(os.path.join(node, txt_file)))
        return results


    def query_single(self,geohash,index_root):
        """
        query a single geohash
        """
        import os
        file_exists_func = self.fs.path_exists
        target_path = compose_path(geohash,index_root)
        cid_dict = {}
        if file_exists_func(target_path):
            cid_dict = self.traverse_sub_node(target_path)
        if file_exists_func(target_path+'.txt'):
            cid_dict = self.process_leaf_node(target_path+'.txt')
        return cid_dict
    
    def query(self, geohashes,index_root):
        # Implementation specific to Backend2
        results = {}
        for nei in geohashes:
            query = self.query_single(nei,index_root)
            if query:
                results = merge_dict(results,query)
        return results
    
    def count(self,geohashes,index_root):
        query_ret = self.query(geohashes,index_root)
        return sum([len(query_ret[cid])-1 for cid in query_ret]) if query_ret else 0
    def retrieve(self,geohashes,index_root):
        from time import time
        t0 = time()
        query_ret = self.query(geohashes,index_root)
        t1 = time()
        results = []
        t_pd = 0
        for cid in query_ret:
            offset_list = [ast.literal_eval(str_tuple) for str_tuple in sorted(query_ret[cid])]
            print(len(offset_list))
            self.offsets = offset_list
            geojson = extract_and_concatenate_from_ipfs(cid, offset_list, suffix_string = "]\n}")
            t21 = time()
            results.append(gpd.read_file(geojson))
            t22 = time()
            t_pd+=t22-t21
        t2 = time()
        print(t1-t0,t2-t1-t_pd,t_pd)
        ret = pd.concat(results)
        return ret
        

class FullTreeFile(GeohashTree):

    def __init__(self):
        self.fs = InterPlanetaryFS()
        
    def add_from_geojson(self, geojson):
        """
        Add a GeoJSON file to the index tree
        """
        features = gpd.read_file(geojson)
        self.features = append_geohash_to_dataframe(features)
        pairs = list(zip(self.features['geohash'],self.features.index.values))
        # Create an empty Trie dictionary
        self.trie_dict = Trie()
        # Insert each index-value pair into the Trie dictionary
        for index, value in pairs:
            self.trie_dict.insert(index, value)
    def export_trie(self,trie_node,geohash,root_path):
        #export geojson at current hash level
        next_path = root_path+"/"+"".join(geohash)
        leaf_path = root_path+f"/{geohash}.geojson"
        if trie_node.value:
            # Open a file in write mode
            self.features.iloc[trie_node.value].to_file(leaf_path, driver="GeoJSON")
        #make path and export to sub folder
        import os 
        if trie_node.children and not os.path.exists(next_path):
            os.makedirs(next_path)
        for ch in trie_node.children:
            child_hash = geohash+ch
            self.export_trie(trie_node.children[ch],child_hash,next_path)
    def generate_tree_index(self,destination_path):
        self.export_trie(self.trie_dict.root,"",destination_path)

    def process_leaf_node(self,leaf):
        """
        process index leaf.
        leaf: CID path of a index leaf, like CID/a/ab/abc.txt
        """
        
        return [ipfs_get_feature(leaf)]

    def traverse_sub_node(self,node):
        """
        recursively collect all the leaf node under the current node
        """
        import os
        
        results=[]
        excludes = [".ipynb_checkpoints"]
        # Get list of items in the directory
        subfolders = [d for d in self.fs.listdir(node) if self.fs.path_isdir(os.path.join(node, d)) and d not in excludes]
        # If there are subfolders, traverse them
        if subfolders:
            for subfolder in subfolders:
                results.extend(self.traverse_sub_node(os.path.join(node, subfolder)))
        else:
            # Otherwise, process txt files in the directory
            geo_features = [f for f in self.fs.listdir(node) if f.endswith('.geojson')]
            for f in geo_features:
                results.extend(self.process_leaf_node(os.path.join(node, f)))
        return results
    def query_single(self,geohash,index_cid):
        """
        find matching geohash or sub-level hashs
        """
        import os
        target_path = compose_path(geohash,index_cid)
        df_list = []
        if self.fs.path_exists(target_path):
            df_list = self.traverse_sub_node(target_path)
        if self.fs.path_exists(target_path+'.geojson'):
            df_list = self.process_leaf_node(target_path+'.geojson')
        return df_list
    def query(self, geohashes,index_cid):
        results = []
        for nei in geohashes:
            query = self.query_single(nei,index_cid)
            if query:
                results.extend(query)
        return pd.concat(results)
    
    def retrieve(self,geohashes,index_cid):
        return self.query(geohashes,index_cid)
