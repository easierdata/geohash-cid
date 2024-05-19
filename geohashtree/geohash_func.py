import geopandas as gpd
import pygeohash as pgh
import h3
from shapely.geometry import Polygon
from geohashtree.geometry import rect_outside_a_circle, rect_overlap

def geohash_encode(lat,lon,precision):
    return pgh.encode(latitude=lat, longitude=lon, precision=precision)

def h3_encode(lat,lon,precision):
    return h3.geo_to_h3(lat,lon,precision)

def h3_to_h3tree(h3str):
    return h3str[1:].rstrip('f')

def h3tree_to_h3(geohash):
    return "8"+geohash+"f"*(14-len(geohash))

def bounding_box(geohash:str) -> tuple:
    mid_lat,mid_lon,d_lat,d_lon = pgh.decode_exactly(geohash)
    min_longitude, max_longitude = mid_lon-d_lon, mid_lon+d_lon
    min_latitude, max_latitude = mid_lat-d_lat,mid_lat+d_lat
    return min_longitude, max_longitude,min_latitude,max_latitude

def bounding_box_to_gdf(min_lon,max_lon,min_lat,max_lat):
    polygon = Polygon([
            (min_lon, min_lat),
            (max_lon, min_lat),
            (max_lon, max_lat),
            (min_lon, max_lat),
            (min_lon, min_lat)
        ])
    gdf = gpd.GeoDataFrame({'geometry': [polygon]},crs="EPSG:4326")
    return gdf


def geohashes_covering_rectangle(min_lon,max_lon,min_lat,max_lat,precision):
    # Get the geohash for the center of the rectangle
    cgh = pgh.encode(latitude=(min_lat+max_lat)/2, longitude=(min_lon+max_lon)/2, precision=precision)
    visited = {}
    queue = [cgh]
    while queue:
        p = queue.pop(0)
        if p in visited:
            continue
        visited[p] = 1
        directions = ["top","right","bottom","left"]
        for dir in directions:
            nei = pgh.get_adjacent(p,dir)
            if rect_overlap(*bounding_box(nei),min_lon,max_lon,min_lat,max_lat):
                queue.append(nei)
    return list(visited.keys())
def geohashes_covering_circle(xc,yc,radius,precision,distance_type='eculidean'):
    cgh = pgh.encode(latitude=yc, longitude=xc, precision=precision)
    visited = {}
    queue = [cgh]
    while queue:
        p = queue.pop(0)
        if p in visited:
            continue
        visited[p] = 1
        directions = ["top","right","bottom","left"]
        for dir in directions:
            nei = pgh.get_adjacent(p,dir)
            if not rect_outside_a_circle(*bounding_box(nei),xc,yc,radius,distance_type):
                queue.append(nei)

    return list(visited.keys())
def h3tree_covering_circle(xc,yc,radius,precision):
    '''
    Get the h3 (tree encoding) covering a circle
    '''
    radius_km = radius / 1000.0
    center_hex = h3.geo_to_h3(yc,xc,precision)
    # Get the average cell edge length for the given origin index
    edge_length_km = h3.edge_length(precision)*2
    
    # Calculate the ring size based on the radius and average edge length
    ring_size = int(radius_km / edge_length_km)+1
    covering = h3.hex_range_distances(center_hex, ring_size)
    h3list = list(map(h3_to_h3tree,[_x for _set in covering for _x in _set]))
    return h3list

def geohash_to_gdf(geohash):

    mid_lat,mid_lon,d_lat,d_lon = pgh.decode_exactly(geohash)

    min_longitude, max_longitude = mid_lon-d_lon, mid_lon+d_lon
    min_latitude, max_latitude = mid_lat-d_lat,mid_lat+d_lat
    gdf = bounding_box_to_gdf(min_longitude, max_longitude, min_latitude, max_latitude)
    gdf['geohash'] = [geohash]
    return gdf

def h3_to_gdf(h3str):
    vertices = h3.h3_to_geo_boundary(h3str)
    gdf = gpd.GeoDataFrame({'geometry': [Polygon(vertices)],'h3': [h3str]},crs="EPSG:4326")
    return gdf