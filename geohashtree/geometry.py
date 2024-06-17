import math
def eculidean_distance(x1,y1,x2,y2):
    return math.sqrt((x2-x1)**2+(y2-y1)**2)

def haversine_distance(lon1,lat1,lon2,lat2):
    # Radius of the Earth in meters
    R = 6371000
    
    # Convert latitude and longitude from degrees to radians
    lat1_rad = math.radians(lat1)
    lon1_rad = math.radians(lon1)
    lat2_rad = math.radians(lat2)
    lon2_rad = math.radians(lon2)
    
    # Difference in coordinates
    dlat = lat2_rad - lat1_rad
    dlon = lon2_rad - lon1_rad
    
    # Haversine formula
    a = math.sin(dlat / 2)**2 + math.cos(lat1_rad) * math.cos(lat2_rad) * math.sin(dlon / 2)**2
    c = 2 * math.atan2(math.sqrt(a), math.sqrt(1 - a))
    
    # Distance
    distance = R * c
    
    return distance

def closest_point_in_rect_to_outside_point(xmin,xmax,ymin,ymax,x,y):
    if x < xmin:
        x = xmin
    elif x > xmax:
        x = xmax
    if y < ymin:
        y = ymin
    elif y > ymax:
        y = ymax
    return (x,y)

def rect_outside_a_circle(xmin,xmax,ymin,ymax,cx,cy,r,distance_type='eculidean'):
    distance_func = eculidean_distance if distance_type=='eculidean' else haversine_distance
    closest = closest_point_in_rect_to_outside_point(xmin,xmax,ymin,ymax,cx,cy)
    return distance_func(closest[0],closest[1],cx,cy) > r

def rect_overlap(xmin1,xmax1,ymin1,ymax1,xmin2,xmax2,ymin2,ymax2):
    return not (xmax1 < xmin2 or xmax2 < xmin1 or ymax1 < ymin2 or ymax2 < ymin1)