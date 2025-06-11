'''
Some utility functions for the geohash tree
'''
def merge_lists(list1, list2):
    val1 = list1[0]  # assuming val1 is the same in both lists
    rest_values = list(set(list1[1:] + list2[1:]))  # merge and remove duplicates
    return [val1] + rest_values
def merge_dict(dict1:dict,dict2:dict) -> dict:
    merged_dict = {}
    for d in (dict1, dict2):
        for key, value in d.items():
            if key in merged_dict:
                merged_dict[key] = merge_lists(merged_dict[key], value)
            else:
                merged_dict[key] = value
    return merged_dict
def merge_dict_old(dict1:dict,dict2:dict) -> dict:
    return {k: list(set(dict1.get(k, [])).union(set(dict2.get(k, [])))) for k in set(dict1) | set(dict2)}

def compose_path(s:str,root:str) -> str:
    """
    compose path a/ab/abc for geohash `abc`
    """
    path = [root]
    for i in range(len(s)):
        path.append(s[:i+1])
    return "/".join(path)

