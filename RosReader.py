import sys
import rosbag
import json
#from bson import json_util
from rospy_message_converter import message_converter
from datetime import datetime
import pyprog
import argparse
import sensor_msgs.point_cloud2 as pc2
import numpy as np
#import uuid
from decimal import Decimal
# from CyberReader import CyberReader
#from google.protobuf.json_format import MessageToJson
# from databaseinterface import DatabaseDynamo, DatabaseMongo
import logging

class RosReader:
    def __init__(self):
        return 0

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
            newitem = {
                        "topic": topic,
                        "timeField": adjTime,
                        "size": len(msgdict),
                        "msg_type": msg._type,
                        "metadataID": newmeta_id
                        }

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
    
    def ProcessFile(self, file, dbobject, metadata, channelList, force=False, process_lidar=False):

        #todo add channel list processing here
        bag = rosbag.Bag(file)#args.rosbag)

        IncludeLiDAR = False
        if (process_lidar):#args.lidar):
            IncludeLiDAR = True
            print("Including PointCloud2 LiDAR data")
        else:
            print("Skipping LiDAR messages")

        print("Scanning messages")
        selecttopiclist = self.generateFilteredTopicList(bag, PointCloud2=IncludeLiDAR)
        num_msg = bag.get_message_count(selecttopiclist)

        bagmetadata = self.generateRosMetaData(bag, metadata,
                                    topics={'selectedtopics': selecttopiclist})
        dataexists = self.checkExistingMetaData(dbobject, bagmetadata)
        if (force == False and dataexists):
            print("metadata already present")
            sys.exit()
        elif (force == 1 and dataexists):
            newmeta_id = dataexists
        else:
            print("inserting the metadata tag")
            newmeta_id = self.insertMetaData(dbobject, bagmetadata)

        print("Inserting data # -> " + str(num_msg))
        prog = pyprog.ProgressBar("-> ", " OK!", num_msg)
        prog.update()
        self.insertRosbagMessagesByTopicFilter(dbobject, bag, selecttopiclist, newmeta_id, prog, LiDARbool=IncludeLiDAR)
        prog.end()
        bag.close()

if __name__ == "__main__":
    
    rr = RosReader(filename=sys.argv[1])