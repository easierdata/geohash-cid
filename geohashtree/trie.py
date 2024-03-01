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