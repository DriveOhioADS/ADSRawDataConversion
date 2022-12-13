import sys
import pymongo
import json
from datetime import datetime
import pyprog
import argparse

#print("Connecting to mongodb at " + args.databaseuri + " / " + args.collection)
myclient = pymongo.MongoClient("mongodb://172.31.144.1:27017")  # "mongodb://localhost:27017/")
mydb = myclient["rosbag"]
mycol = None
collection = "rosbag"
for name in mydb.list_collection_names():
    if (name == collection):
        mycol = mydb[collection]
        print("Found collection: " + name)
        break

if (mycol == None):
    print("Creating the collection: " + collection)
    mydb.create_collection(collection, timeseries={'timeField': 'timeField'})
    mycol = mydb[collection]
