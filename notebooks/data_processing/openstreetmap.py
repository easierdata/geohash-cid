
from pyrosm import get_data,OSM
from datetime import datetime
regions_prefix = [
#     'us-midwest',
#   'us-northeast',
#   'us-pacific',
#   'us-south',
  'us-west']
print(datetime.now().strftime('%Y-%m-%d %H:%M:%S'))
for region in regions_prefix:
    fp = f'/gpfs/data1/oshangp/easier/geohash-cid/data/osm/{region}-latest.osm.pbf'
    osm = OSM(fp)
    custom_filter = {'amenity': True, 'osm_type':['node']}
    pois = osm.get_pois(custom_filter=custom_filter)
    pois.to_file(f'/gpfs/data1/oshangp/easier/geohash-cid/data/osm/{region}-poi.geojson',driver='GeoJSON')
    print('finished',fp, 'at', (datetime.now().strftime('%Y-%m-%d %H:%M:%S')))