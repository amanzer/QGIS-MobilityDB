"""
This script measures the average FPS we get for a range of number of objects.

The goal is define a upper bound for the FPS to acheive for a given number of objects if
there was no cost other than the update of qgs features in ONF.

We also measure the time it takes to create the features at the beginning of the script.


Desktop Ubuntu results  :

results1 = {1: (895.5832458699174, 0.008231401443481445), 5: (874.8947350169816, 0.008946418762207031), 10: (861.9517510324698, 0.0067234039306640625), 15: (803.4033510619589, 0.008788347244262695), 20: (826.4461108372362, 0.011795282363891602), 25: (892.2949024381811, 0.012117624282836914), 30: (880.817766568021, 0.010245561599731445), 35: (870.7990479559708, 0.011226654052734375), 40: (856.2405411467754, 0.009530782699584961), 45: (852.171281943518, 0.01001596450805664), 50: (852.4271657097851, 0.01156163215637207), 55: (838.039967717471, 0.012759923934936523), 60: (831.595780199258, 0.0122222900390625), 65: (820.7425999354139, 0.014039039611816406), 70: (813.8101676970061, 0.013093709945678711), 75: (811.8249181952104, 0.012933969497680664), 80: (799.3792770682517, 0.01446986198425293), 85: (788.8950743434195, 0.014393091201782227), 90: (783.6711882756115, 0.017041683197021484), 95: (771.6661333180548, 0.01423501968383789), 100: (761.0693111304771, 0.01479196548461914)}
results2 ={100: (441.6851926149922, 0.02807760238647461), 200: (419.65448893830427, 0.028905630111694336), 300: (408.06818540429015, 0.042719364166259766), 400: (401.04018964273985, 0.05560708045959473), 500: (387.80391563178296, 0.07719802856445312), 600: (362.5535223776417, 0.10336446762084961), 700: (344.51494390683365, 0.13973140716552734), 800: (336.1252336785607, 0.15714693069458008), 900: (319.16115668374533, 0.1921098232269287), 1000: (303.07042293646634, 0.22408413887023926)}
results3 = {5000: (173.79310336775754, 4.458710432052612), 10000: (97.34369737469942, 16.613506317138672), 15000: (71.63895361199617, 37.4998984336853), 20000: (52.126252868439764, 65.75739455223083), 25000: (41.32996647957075, 106.81102228164673), 30000: (32.796557206527396, 149.94286131858826), 35000: (26.769924945136474, 206.16963148117065), 40000: (20.769881932148966, 271.9198348522186)}
results4 = {45000: (21.152451350178843, 333.5160973072052), 50000: (15.779823653917148, 419.4396266937256), 100000: (7.910946864734796, 1765.4237129688263)}
                                                                                        408.1181209087372 ==> using qgsgeom()
results = {**results1, **results2, **results3, **results4}
"""

from datetime import datetime
import matplotlib.pyplot as plt
import time

canvas = iface.mapCanvas()
temporalController = canvas.temporalController()



def create_vlayer(name, num_fields, frames):
    task_manager = QgsApplication.taskManager()
    vlayer = QgsVectorLayer("Point", name, "memory")
    pr = vlayer.dataProvider()
    pr.addAttributes([QgsField("start_time", QVariant.DateTime), QgsField("end_time", QVariant.DateTime)])
    vlayer.updateFields()
    tp = vlayer.temporalProperties()
    tp.setIsActive(True)
    tp.setMode(qgis.core.QgsVectorLayerTemporalProperties.ModeFeatureDateTimeStartAndEndFromFields)
    tp.setStartField("start_time")
    tp.setEndField("end_time")

    now = time.time()
    features_list =[]
    start_datetime_obj = QDateTime(datetime(2023,6,1,0,0,0))
    end_datetime_obj = QDateTime(datetime(2023,6,1,23,0,0))

    
    for i in range(num_fields):
        feat = QgsFeature(vlayer.fields())
        feat.setAttributes([start_datetime_obj, end_datetime_obj])
        geom = QgsGeometry()
        feat.setGeometry(geom)
        features_list.append(feat)

    QgsProject.instance().addMapLayer(vlayer)
    
    
    vlayer.startEditing()
    vlayer.addFeatures(features_list)
    vlayer.commitChanges()
    iface.vectorLayerTools().stopEditing(vlayer)

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
        vlayer.dataProvider().changeGeometryValues(new_geometries) # Updating geometries for all features
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
