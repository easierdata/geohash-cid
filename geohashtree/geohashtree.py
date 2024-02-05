from abc import ABC, abstractmethod
import geopandas as gpd
import pandas as pd
import pygeohash as pgh
import ast
from .trie import Trie
from .util import merge_dict,compose_path
from .filesystem import get_geojson_path_from_cid
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
    def add_from_geojson(self, geojson):
        # Implementation specific to Backend1
        pass

    def generate_tree_index(self):
        # Implementation specific to Backend1
        pass

    def query(self, lat, lon, radius):
        # Implementation specific to Backend1
        pass

class LiteTreeOffset(GeohashTree):

    def calculate_offsets_and_lengths_stream(geojson_file_path):
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
                    brace_count += line.count('{')
                    brace_count -= line.count('}')

                # When the braces balance out, we've found the end of the feature
                if feature_start is not None and brace_count == 0:
                    feature_end = current_position + len(line)
                    offsets_and_lengths.append((feature_start, feature_end - feature_start))
                    feature_start = None

        return offsets_and_lengths
    
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
    
    def export_trie(self,trie_node,geohash,root_path):
        #export geojson at current hash level
        next_path = root_path+"/"+"".join(geohash)
        leaf_path = root_path+f"/{geohash}.txt"
        print(geohash,root_path,next_path,leaf_path)
        if trie_node.value:
            # Open a file in write mode
            with open(leaf_path, 'w') as f:
                f.write(f"[CID placeholder]\n{self.head_offset_length}\n")
                for item in trie_node.value:
                    f.write(f"{item}\n")
        #make path and export to sub folder
        import os 
        if trie_node.children and not os.path.exists(next_path):
            os.makedirs(next_path)
        for ch in trie_node.children:
            child_hash = geohash+ch
            self.export_trie(trie_node.children[ch],child_hash,next_path)
    def add_from_geojson(self, geojson):
        # Implementation specific to Backend2
        offsets_lengths = self.calculate_offsets_and_lengths_stream(geojson)
        features = gpd.read_file(geojson)
        features['offlen'] = offsets_lengths
        self.head_offset_length = (0,offsets_lengths[0][0])
        features = pd.concat([features,features.get_coordinates()],axis=1)
        features['geohash'] = features.apply(lambda row: pgh.encode(row['y'], row['x'],precision=6), axis=1)
        pairs = list(zip(features['geohash'],features['offlen']))
        # Create an empty Trie dictionary
        self.trie_dict = Trie()

        # Insert each index-value pair into the Trie dictionary
        for index, value in pairs:
            self.trie_dict.insert(index, value)

    def generate_tree_index(self,destination_path):
        # Implementation specific to Backend2
        self.export_trie(self.trie_dict.root,"",destination_path)
    
    def process_leaf_node(self,leaf):
        """
        process index leaf. [TODO]
        leaf: txt file path of a index leaf, like a//ab/abc.txt
        """
        with open(leaf, 'r', encoding='utf-8') as file:
            lines = file.readlines()
        return {lines[0].strip():[line.strip() for line in lines[1:]]}

    def traverse_sub_node(self,node):
        """
        recursively collect all the leaf node under the current node
        """
        import os
        
        results={}
        excludes = [".ipynb_checkpoints"]
        # Get list of items in the directory
        subfolders = [d for d in os.listdir(node) if os.path.isdir(os.path.join(node, d)) and d not in excludes]
        # If there are subfolders, traverse them
        if subfolders:
            for subfolder in subfolders:
                results = merge_dict(results,self.traverse_sub_node(os.path.join(node, subfolder)))
        else:
            # Otherwise, process txt files in the directory
            txt_files = [f for f in os.listdir(node) if f.endswith('.txt')]
            for txt_file in txt_files:
                results = merge_dict(results,self.process_leaf_node(os.path.join(node, txt_file)))
        return results

    def query_single(self,geohash,index_root):
        """
        query a single geohash
        """
        import os
        target_path = compose_path(geohash,index_root)
        cid_dict = {}
        if os.path.exists(target_path):
            cid_dict = self.traverse_sub_node(target_path)
        if os.path.exists(target_path+'.txt'):
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
    def retrieve(self,geohashes,index_root):
        results = self.query(geohashes,index_root)
        for cid in results:
            geojson_path = get_geojson_path_from_cid(cid)
            offset_list = [ast.literal_eval(str_tuple) for str_tuple in sorted(results[cid])]
            print(len(offset_list))
            geojson = self.extract_and_concatenate(geojson_path, offset_list, suffix_string = "]\n}")
            with open(f'result_{cid}.geojson', 'w') as of:
                of.write(geojson)

class FullTreeFile(GeohashTree):
    def add_from_geojson(self, geojson):
        # Implementation specific to Backend3
        pass

    def generate_tree_index(self):
        # Implementation specific to Backend3
        pass

    def query(self, geohashes):
        # Implementation specific to Backend3
        pass
