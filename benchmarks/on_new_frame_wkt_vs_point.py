

# ONF Shapely Point and QgsPointXY

from shapely.geometry import Point
import time

Timer = 0
n = 20000
item = Point(0,0)
for i in range(n) :        
    now = time.time()
    x,y = item.x, item.y
    geom = QgsGeometry.fromPointXY(QgsPointXY(x,y))
    Timer += time.time() - now 
    
print(f"time over {n} : {Timer} s, average {Timer/n}")



# Using WKT code :


from shapely.geometry import Point
import time

Timer = 0
n = 20000
item = Point(0,0).wkt
for i in range(n) :        
    now = time.time()
    geom = QgsGeometry().fromWkt(item)
    Timer += time.time() - now 
    
print(f"time over {n} : {Timer} s, average {Timer/n}")

"""
Results on Laptop : 

QgsPointXY:
time over 20000 : 0.6315727233886719 s, average 3.1578636169433594e-05

Wkt: 
time over 20000 : 0.06986188888549805 s, average 3.4930944442749022e-06


Advantage for WKT 

"""