"""
This script measures the average FPS we get for a range of number of objects.

The goal is define a upper bound for the FPS to acheive for a given number of objects if
there was no cost other than the update of qgs features in ONF.

We also measure the time it takes to create the features at the beginning of the script.


Desktop Ubuntu results  :

results = {1: (756.7142636953172, 6.818771362304688e-05), 5: (752.0861097157176, 3.5762786865234375e-05), 10: (734.9132354218076, 4.696846008300781e-05), 15: (727.8781218207665, 5.364418029785156e-05), 20: (780.570630921476, 6.699562072753906e-05), 25: (780.5667570325951, 7.843971252441406e-05), 30: (771.5408195393601, 8.7738037109375e-05), 35: (757.1901638574137, 8.559226989746094e-05), 40: (754.9551209939114, 0.00013756752014160156), 45: (746.5028062910591, 0.00010895729064941406), 50: (737.5422747017209, 0.00016498565673828125), 55: (723.0280682815153, 0.00011491775512695312), 60: (712.6428130626836, 0.00012969970703125), 65: (693.5795308717555, 0.00014162063598632812), 70: (695.8144345215716, 0.0001437664031982422), 75: (678.8263487015299, 0.00015616416931152344), 80: (660.3990626740696, 0.0002639293670654297), 85: (681.6050077356548, 0.00017642974853515625), 90: (662.8293364225015, 0.000232696533203125), 95: (639.2256790139359, 0.0001838207244873047), 100: (663.5511344044796, 0.0002162456512451172), 200: (636.7081478977428, 0.0003974437713623047), 300: (612.541197654669, 0.0005514621734619141), 400: (539.2367743158001, 0.0007376670837402344), 500: (543.0569581405757, 0.0009522438049316406), 600: (524.5067919926378, 0.0011074542999267578), 700: (501.52365841236576, 0.0014069080352783203), 800: (492.03959829569465, 0.0014514923095703125), 900: (450.6870372011422, 0.002003192901611328), 1000: (451.33616314847063, 0.0021398067474365234), 5000: (206.555131859612, 0.009563922882080078), 10000: (111.96632611732639, 0.020222902297973633), 15000: (74.30399053385449, 0.0458979606628418), 20000: (56.07695762063882, 0.058228254318237305), 25000: (41.67863349244524, 0.06564736366271973), 30000: (35.57793295180229, 0.08289575576782227), 35000: (27.791367130998825, 0.09359288215637207), 40000: (23.40653567488394, 0.12318563461303711), 45000: (19.905702272348964, 0.12233591079711914), 50000: (17.73415509242768, 0.1566476821899414), 100000: (8.120455127727444, 0.3336620330810547)}
"""

from datetime import datetime
import matplotlib.pyplot as plt
import time


def create_vlayer(name, num_fields, frames):
    vlayer = QgsVectorLayer("Point", name, "memory")
    pr = vlayer.dataProvider()
    pr.addAttributes([QgsField("start_time", QVariant.DateTime), QgsField("end_time", QVariant.DateTime)])
    vlayer.updateFields()
    tp = vlayer.temporalProperties()
    tp.setIsActive(True)
    tp.setMode(qgis.core.QgsVectorLayerTemporalProperties.ModeFeatureDateTimeStartAndEndFromFields)
    tp.setStartField("start_time")
    tp.setEndField("end_time")
    QgsProject.instance().addMapLayer(vlayer)

    now = time.time()
    features_list =[]
    vfields = vlayer.fields()
    start_datetime_obj = QDateTime(datetime(2023,6,1,0,0,0))
    end_datetime_obj = QDateTime(datetime(2023,6,1,23,0,0))


    for i in range(num_fields):
        feat = QgsFeature(vfields)
        feat.setAttributes([start_datetime_obj, end_datetime_obj])
        features_list.append(feat)
    
    pr.addFeatures(features_list)

    TIME_create_features = time.time() - now

    current_time_stamp_column = ["POINT (1 1)"]*num_fields

    fps_record = []
    for i in range(frames):
        start_time = time.time()    

        new_geometries = {}  # Dictionary {feature_id: QgsGeometry}
        for j in range(num_fields): #TODO : compare vs Nditer
            new_geometries[j] = QgsGeometry.fromWkt(current_time_stamp_column[j])


        vlayer.startEditing()
        # self.qviz.vlayer.dataProvider().changeAttributeValues(attribute_changes) # Updating attribute values for all features
        pr.changeGeometryValues(new_geometries) # Updating geometries for all features
        vlayer.commitChanges()
        iface.vectorLayerTools().stopEditing(vlayer)

        end_time = time.time()
        fps_record.append(1/(end_time-start_time))

    average_fps = sum(fps_record)/len(fps_record)
    
    return average_fps, TIME_create_features


x = [1, 5, 10, 15, 20, 25, 30, 35, 40, 45, 50, 55, 60, 65, 70, 75, 80, 85, 90, 95, 100, 200, 300, 400, 500, 600, 700, 800, 900, 1000, 5000, 10000, 15000, 20000, 25000, 30000, 35000, 40000, 45000, 50000, 100000]


frames = 100
results = {}
for i, x in enumerate(x):
    print(f"Average fps for {x} features over {frames} frames : ")
    avg_fps, create_time = create_vlayer(f"vlayer_{x}", x, frames)
    results[x] = (avg_fps, create_time)
    print(f"{avg_fps} fps")
    print("")

print(results)



num_objects = list(results.keys())
fps_values = [v[0] for v in results.values()]
generation_times = [v[1] for v in results.values()]

fig, ax1 = plt.subplots(figsize=(10, 6))

# Plotting FPS
color = 'tab:blue'
ax1.set_xlabel('Number of Objects')
ax1.set_ylabel('FPS', color=color)
ax1.plot(num_objects, fps_values, label='FPS', color=color, marker='o')
ax1.tick_params(axis='y', labelcolor=color)

# Creating a second y-axis for the generation time
ax2 = ax1.twinx()
color = 'tab:red'
ax2.set_ylabel('Generation Time (s)', color=color)
ax2.plot(num_objects, generation_times, label='Generation Time (s)', color=color, marker='o')
ax2.tick_params(axis='y', labelcolor=color)


plt.title('FPS and Generation Time vs Number of Objects')
fig.tight_layout()
plt.legend()
plt.grid(True)

plt.show()
