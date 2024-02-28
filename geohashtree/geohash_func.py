import geopandas as gpd
import pygeohash as pgh
from shapely.geometry import Polygon
from geohashtree.geometry import rect_outside_a_circle, rect_overlap
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
def geohashes_covering_circle(xc,yc,radius,precision):
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
            if not rect_outside_a_circle(*bounding_box(nei),xc,yc,radius):
                queue.append(nei)

    return list(visited.keys())

def geohash_to_gdf(geohash):

    mid_lat,mid_lon,d_lat,d_lon = pgh.decode_exactly(geohash)

    min_longitude, max_longitude = mid_lon-d_lon, mid_lon+d_lon
    min_latitude, max_latitude = mid_lat-d_lat,mid_lat+d_lat
    gdf = bounding_box_to_gdf(min_longitude, max_longitude, min_latitude, max_latitude)
    gdf['geohash'] = [geohash]
    return gdf