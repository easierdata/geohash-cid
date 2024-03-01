'''
Some utility functions for the geohash tree
'''

def merge_dict(dict1:dict,dict2:dict) -> dict:
    return {k: list(set(dict1.get(k, [])).union(set(dict2.get(k, [])))) for k in set(dict1) | set(dict2)}

def compose_path(s:str,root:str) -> str:
    """
    compose path a/ab/abc for geohash `abc`
    """
    path = [root]
    for i in range(len(s)):
        path.append(s[:i+1])
    return "/".join(path)

