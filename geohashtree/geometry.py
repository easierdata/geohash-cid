import math
def eculidean_distance(x1,y1,x2,y2):
    return math.sqrt((x2-x1)**2+(y2-y1)**2)

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

def rect_outside_a_circle(xmin,xmax,ymin,ymax,cx,cy,r):
    closest = closest_point_in_rect_to_outside_point(xmin,xmax,ymin,ymax,cx,cy)
    return eculidean_distance(closest[0],closest[1],cx,cy) > r

def rect_overlap(xmin1,xmax1,ymin1,ymax1,xmin2,xmax2,ymin2,ymax2):
    return not (xmax1 < xmin2 or xmax2 < xmin1 or ymax1 < ymin2 or ymax2 < ymin1)