import sys
import pymongo
from pandasgui import show
from pandas import DataFrame
myclient = pymongo.MongoClient("mongodb://127.0.0.1:27017")
mydb = myclient["rosbag"]
collection = "rosbag"
mycol = mydb[collection]
show(DataFrame(mycol.find({})))
# show(DataFrame(mycol.find({'topic': '/gps/gps'})))
