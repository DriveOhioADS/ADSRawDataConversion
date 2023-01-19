import sys
import rosbag
import boto3
from boto3.dynamodb.conditions import Key, Attr
from botocore.exceptions import ClientError
import pymongo
import json
from bson import json_util
from rospy_message_converter import message_converter
from datetime import datetime
import pyprog
import argparse
import sensor_msgs.point_cloud2 as pc2
import numpy as np
import uuid
from decimal import Decimal
from CyberReader import CyberReader
from google.protobuf.json_format import MessageToJson
# import datetime
# import time
from databaseinterface import DatabaseDynamo, DatabaseMongo
import logging

def generateRosMetaData(rosbagdata, metadata, vehicleID, experimentnumber, topics):
    starttime = rosbagdata.get_start_time()
    endtime = rosbagdata.get_end_time()
    duration = endtime - starttime
    interal_metadata = {
                'startTime': starttime,
                'endTime': endtime,
                'duration': duration,
                'filename': rosbagdata.filename,
                'size': rosbagdata.size,
                'msgnum': rosbagdata.get_message_count(),
                'topics': topics,
                'type': 'rosbag'
                }
    metadata.update(interal_metadata)
    return metadata


def checkExistingMetaData(dbobject, metadata):
    searchstring = {"filename": metadata["filename"],
                    "experimentID": metadata["experimentID"],
                    "startTime": metadata['startTime'],
                    "size": metadata['size'],
                    'msgnum': metadata['msgnum']}
    return dbobject.db_find_metadata("metadata", searchstring['startTime'])
    # result = dbconn["metadata"].find_one(searchstring)
    # if (result != None):
    #    return result["_id"]
    # return None


def insertMetaData(dbobject, metadata):
    return dbobject.db_insert("metadata", metadata)
    # result = dbconn["metadata"].insert_one(metadata)
    # return result.inserted_id  # needs to be ID of metadata

def generateFilteredTopicList(rosbagfile, PointCloud2=False):
    banned = ['sensor_msgs/CompressedImage',
              'sensor_msgs/Image',
              'velodyne_msgs/VelodyneScan',
              'theora_image_transport/Packet',
              'sensor_msgs/LaserScan',
              'autoware_lanelet2_msgs/MapBin',  # issues with insert
              'visualization_msgs/MarkerArray',  # breaks insert
              'autoware_msgs/DetectedObjectArray',  # breaks insert
              'autoware_msgs/LaneArray',
              'autoware_msgs/Lane', #too big for dynamo
              '/rosout'
              ]
    if (PointCloud2 == False):
        banned.append('sensor_msgs/PointCloud2')

    goodtopiclist = []
    topics = rosbagfile.get_type_and_topic_info()
    topiclist = topics.topics
    for tp in topiclist.items():
        clean = 1
        for ban in banned:
            if (tp[1].msg_type == ban):
                clean = 0
        if (clean):
            # print("adding: " + tp[0])
            goodtopiclist.append(tp[0])
        else:
            print("skip: " + tp[0] + " => " + tp[1].msg_type)
    return goodtopiclist

def insertRosbagMessagesByTopicFilter(dbobject, rosbagfile, goodtopiclist, newmeta_id, prog, LiDARbool):
    count = 0
    for topic, msg, t in rosbagfile.read_messages(topics=goodtopiclist):
        msgdict = message_converter.convert_ros_message_to_dictionary(msg)
        adjTime = datetime.utcfromtimestamp(t.secs + (t.nsecs / 10e6)).timestamp()
        newitem = {"topic": topic,
                   "timeField": adjTime,
                   "size": len(msgdict),
                   "msg_type": msg._type,
                   "metadataID": newmeta_id}

        if (LiDARbool and msg._type == 'sensor_msgs/PointCloud2'):
            pcread = pc2.read_points(msg, skip_nans=True, field_names=("x", "y", "z"))
            ptlist = list(pcread)
            msgdict.pop('data')
            xdata = []
            ydata = []
            zdata = []
            for xyzpoint in ptlist:
                xdata.append(xyzpoint[0])
                ydata.append(xyzpoint[1])
                zdata.append(xyzpoint[2])

            # xdict = {'x': xdata}
            pdata_dict = {}
            pdata_dict["PointCloud2"] = {'x': xdata, 'y': ydata, 'z': zdata}

            newitem.update(pdata_dict)
        else:
            result = newitem.update(msgdict)
        result = dbobject.db_insert_main(newitem)
        count = count + 1
        prog.set_stat(count)
        prog.update()

    return count

def ProcessRosbagFile(args, dbobject, channelList, metadatasource):
    #todo add channel list processing here
    bag = rosbag.Bag(args.rosbag)

    IncludeLiDAR = False
    if (args.lidar):
        IncludeLiDAR = True
        print("Including PointCloud2 LiDAR data")
    else:
        print("Skipping LiDAR messages")

    print("Scanning messages")
    selecttopiclist = generateFilteredTopicList(bag, PointCloud2=IncludeLiDAR)
    num_msg = bag.get_message_count(selecttopiclist)

    bagmetadata = generateRosMetaData(bag, metadatasource,
                                topics={'selectedtopics': selecttopiclist})
    dataexists = checkExistingMetaData(dbobject, bagmetadata)
    if (args.force == False and dataexists):
        print("metadata already present")
        sys.exit()
    elif (args.force == 1 and dataexists):
        newmeta_id = dataexists
    else:
        print("inserting the metadata tag")
        newmeta_id = insertMetaData(dbobject, bagmetadata)

    print("Inserting data # -> " + str(num_msg))
    prog = pyprog.ProgressBar("-> ", " OK!", num_msg)
    prog.update()
    insertRosbagMessagesByTopicFilter(dbobject, bag, selecttopiclist, newmeta_id, prog, LiDARbool=IncludeLiDAR)
    prog.end()
    bag.close()


def ProcessCyberFile(args, dbobject, channelList, metadatasource):
    cr = CyberReader(args.cyberfolder, args.cyberfilebase)
    #check that deny/allow are present and set defaults
    if(channelList != None):
        if('deny' in channelList and channelList['deny'] != None):
            deny = channelList['deny']
        else:
            deny = None
        if('allow' in channelList and channelList['allow'] != None):
            allow = channelList['allow']
        else:
            allow = None
        channelList = {
                    'deny': deny,
                    'allow': allow
                    }  
    cr.InsertDataFromFolder(dbobject, metadatasource, channelList)   
    return 0
    
def main(args):
    
    if (args.mongodb != None):
        logging.info(f"Connecting to database at {args.mongodb} / {args.collection}")
        dbobject = DatabaseMongo(args.mongodb)
        dbobject.check()
    elif (args.dynamodb != None):
        logging.info(f"Connecting to database at {args.dynamodb} / {args.collection}")
        dbobject = DatabaseDynamo(args.dynamodb)
        dbobject.check()    
    else:
        logging.error("No database specified")
        sys.exit()

    dbobject.setCollectionName(args.collection)
    dbobject.db_connect()
    
    try:
        with open(args.metadatafile, 'r') as file:
            metadatasource = json.load(file)
    except:
        logging.error("failed to load metadata from file")
        return -1
    
    json_channels = None
    if(args.channellist != None):
        with open(args.channellist, 'r') as file:
            json_channels = json.load(file)
    
    if(args.cyberfolder != None):
        logging.info('Processing Cyber data')
        ProcessCyberFile(args, dbobject, json_channels, metadatasource)
    elif(args.rosbag != None):
        logging.info("Loading rosbag")
        ProcessRosbagFile(args, dbobject, json_channels, metadatasource)
        
    else:
        logging.error("No data file source specified")
        sys.exit()

    logging.info("All done")
      
if __name__ == '__main__':
    logging.basicConfig(filename="insert.log", encoding='utf-8', level=logging.DEBUG)
    logging.getLogger().addHandler(logging.StreamHandler(sys.stdout))

    logging.info("datainsert start")
    parser = argparse.ArgumentParser()
    parser.add_argument('--dynamodb', help='dynamo url string', required=False)
    parser.add_argument('--mongodb', help='mongodb url string', required=False)
    parser.add_argument('--cyberfolder', help='cyber data folder', required=False)
    parser.add_argument('--cyberfilebase', help='cyber file name w/o extension', required=False)
    parser.add_argument('--rosbag', help='rosbag file', required=False)
    parser.add_argument('--metadatafile', help='json metadata file',required=True)
    #parser.add_argument('-v', '--vehicleid', type=int, help='vehicle ID', required=True)
    #parser.add_argument('-e', '--experimentid', type=int, help='experiment ID', required=True)
    parser.add_argument('--collection', default='rosbag', help='Collection Name', required=False)
    parser.add_argument('--lidar', default='', dest='lidar', action='store_true', help='Insert LiDAR', required=False)
    parser.add_argument('--force', default=False, dest='force', action='store_true', help='force insert')
    parser.add_argument('--channellist', default=None, help='json file with accecpt/deny list of channels')
    try:
        args = parser.parse_args()
    except:
        logging.error("argument parsing failed")
        sys.exit(-1)
    
    main(args)
