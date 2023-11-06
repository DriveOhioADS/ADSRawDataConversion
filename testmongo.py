import sys
import pymongo
import json
from datetime import datetime
import pyprog
import argparse
from bson.objectid import ObjectId

def CheckMongoResult():
    myclient = pymongo.MongoClient("mongodb://172.20.128.1:27017")  # "mongodb://localhost:27017/")
    mydb = myclient["cyber"]
    metadata = mydb['metadata']
    cyberdata = mydb['cyber']

    cybercount = cyberdata.count_documents({})
    print(f"overal cyber messages {cybercount}")

    metacount = metadata.count_documents({})
    print(f"metadata count {metacount}")

    metadatalist = metadata.find({})
    totalcount = 0
    for md in metadatalist:
        metadataID = md['_id']
        print(metadataID)
        query = {"metadataID": ObjectId(metadataID)}
        result = cyberdata.find(query)
        count = 0
        for item in result:
            count = count+1
        print(count)
        totalcount = totalcount + count
    print(totalcount)

def CheckDynamoResult():
    



# mydb = myclient["cyber"]
# mycol = None
# collection = "cyber"
# for name in mydb.list_collection_names():
#     if (name == collection):
#         mycol = mydb[collection]
#         print("Found collection: " + name)
#         break

# if (mycol == None):
#     print("Creating the collection: " + collection)
#     mydb.create_collection(collection, timeseries={'timeField': 'timeField'})
#     mycol = mydb[collection]
