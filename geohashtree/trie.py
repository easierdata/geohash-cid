'''
Script to create a tree structure for geohash

Author: Zheng Liu
Date: 2024-02-01
'''

class TrieNode:
    def __init__(self):
        self.children = {}
        self.value = []
class Trie:
    def __init__(self):
        self.root = TrieNode()

    def insert(self, index, value):
        node = self.root
        for char in str(index):
            if char not in node.children:
                node.children[char] = TrieNode()
            node = node.children[char]
        node.value.append(value)

    def get(self, index):
        node = self.root
        for char in str(index):
            if char not in node.children:
                return None
            node = node.children[char]
        return node.value

def trim_full_node(node):
    if len(node.children) == 0:
        return True
    cnt = 0
    for child in node.children:
        
        cnt+=trim_full_node(node.children[child])
    if cnt == 32:
        node.children = {}
        return True
    else:
        return False

def get_trie_leaves(node,prefix_hash):
    if not node.children:
        return [prefix_hash]
    else:
        res = []
        for ch in node.children:
            res.extend(get_trie_leaves(node.children[ch],prefix_hash+ch))
        return res

def trim_hashes(input_hashes):
    trie_dict = Trie()
    # Insert each index-value pair into the Trie dictionary
    for index in input_hashes:
        trie_dict.insert(index, 1)
    trim_full_node(trie_dict.root)
    return get_trie_leaves(trie_dict.root,"")