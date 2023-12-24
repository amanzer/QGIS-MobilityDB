## Import rows of mobilitydb table into a 'rows' variable
import psycopg2
canvas = iface.mapCanvas()
temporalController = canvas.temporalController()
currentFrameNumber = temporalController.currentFrameNumber()

connection = None

try:
    # Set the connection parameters to PostgreSQL
    connection = psycopg2.connect(host='localhost', database='postgres', user='postgres', password='postgres')
    connection.autocommit = True

    # Open a cursor to perform database operations
    cursor = connection.cursor()

    # Query the database and obtain data as Python objects
    select_query = "SELECT trip FROM trips_test"
    dtrange = temporalController.dateTimeRangeForFrameNumber(currentFrameNumber)
    #select_query = "SELECT valueAtTimestamp(trip, '"+range.end().toString("yyyy-MM-dd HH:mm:ss-04")+"') FROM trips_test"
    cursor.execute(select_query)
    rows = cursor.fetchall()

except (Exception, psycopg2.Error) as error:
    print("Error while connecting to PostgreSQL", error)

finally:
    # Close the connection
    if connection:
        connection.close()


## Create a temporal layer (variable 'vlayer') with single field for datetime
vlayer = QgsVectorLayer("Point", "points_3", "memory")
pr = vlayer.dataProvider()
pr.addAttributes([QgsField("time", QVariant.DateTime)])
vlayer.updateFields()
tp = vlayer.temporalProperties()
tp.setIsActive(True)
tp.setMode(Qgis.VectorTemporalMode.FixedTemporalRange)
#tp.setMode(1) #single field with datetime
tp.setStartField("time")
crs = vlayer.crs()
crs.createFromId(22992)
vlayer.setCrs(crs)
vlayer.updateFields()
QgsProject.instance().addMapLayer(vlayer)


from pymeos import *
import time
now = time.time()
FRAMES_NB = 50 # Number of frames to generate
canvas = iface.mapCanvas()
temporalController = canvas.temporalController()
currentFrameNumber = temporalController.currentFrameNumber()
features_list = []
interpolation_times = []
feature_times = []

# Use Pymeos to get the value At timestamp