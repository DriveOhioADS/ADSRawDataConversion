import pymongo
import json
import numpy as np
import matplotlib.pyplot as plt
import sys

myclient = pymongo.MongoClient("mongodb://localhost:27017/")
mydb = myclient["rosbag"]
mycol = mydb["rosbag"]

metadID = mydb['metadata'].find_one({'experimentID': 3})



query = {'metadataID': metadID['_id'], 'topic': "/vehicle/brake_cmd"} #, 'pedal_cmd': {"$gt": 0}}


engagecmd = mycol.find({'topic': '/vehicle/engage'})
egpsloc = []
for ec in engagecmd:
    closeGPS = mycol.find_one({'topic': '/gps/gps', 'timeField': {'$gte': ec['timeField']}})
    egpsloc.append(closeGPS)

print(egpsloc)
sys.exit()

query = {'topic': '/gps/gps', 'metadataID': metadID['_id']}
lat = []
lon = []
if mycol.find_one(query) is not None:
    cursor = mycol.find(query)#, {"latitude": 1, "longitude": 1, "altitude": 1})
    for data in cursor:
        lat.append(data['latitude'])
        lon.append(data['longitude'])
        #print(data)

    plt.scatter(lat, lon)
    plt.scatter(maxbrakePos['latitude'], maxbrakePos['longitude'], s=80)

    plt.xlim(min(lat)-0.0001, max(lat)+0.0001)
    plt.ylim(min(lon)-0.0001, max(lon)+0.0001)
    plt.show()
