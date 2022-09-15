import sys
import rosbag
import pymongo
import json
from rospy_message_converter import message_converter
from datetime import datetime
import pyprog
import argparse

def generateMetaData(rosbagdata, vehicleID, experimentnumber, other):
    starttime = rosbagdata.get_start_time()
    endtime = rosbagdata.get_end_time()
    duration = endtime - starttime
    metadata = {'vehicleID': vehicleID,
                'experimentID': experimentnumber,
                'startTime': starttime,
                'endTime': endtime,
                'duration': duration,
                'filename': bag.filename,
                'size': bag.size,
                'msgnum': bag.get_message_count(),
                'other': other,
    }
    return metadata

def checkExistingMetaData(dbconn, metadata):
    searchstring = {"filename": metadata["filename"],
                    "experimentID": metadata["experimentID"],
                    "startTime": metadata['startTime'],
                    "size": metadata['size'],
                    'msgnum': metadata['msgnum']}
    result = dbconn["metadata"].find_one(searchstring)
    if(result != None):
        return result["_id"]
    return None

def insertMetaData(dbconn, metadata):
    result = dbconn["metadata"].insert_one(metadata)
    return result.inserted_id #needs to be ID of metadata


count = 0

def generateFilteredTopicList(rosbagfile):
    banned = ['sensor_msgs/CompressedImage',
              'sensor_msgs/Image',
              'sensor_msgs/PointCloud2',
              'velodyne_msgs/VelodyneScan',
              'theora_image_transport/Packet',
              'sensor_msgs/LaserScan',
              'autoware_lanelet2_msgs/MapBin',  # issues with insert
              'visualization_msgs/MarkerArray',  # breaks insert
              'autoware_msgs/DetectedObjectArray',  # breaks insert
              ]

    goodtopiclist = []
    topics = rosbagfile.get_type_and_topic_info()
    topiclist = topics.topics
    for tp in topiclist.items():
        clean = 1
        for ban in banned:
            if (tp[1].msg_type == ban):
                clean = 0
        if (clean):
            #print("adding: " + tp[0])
            goodtopiclist.append(tp[0])
        else:
            print("skip: " + tp[0])
    return goodtopiclist


def insertMessagesByTopicFilter(collection, rsobagfile, goodtopiclist, newmeta_id, prog):
    count = 0
    for topic, msg, t in rsobagfile.read_messages(topics=goodtopiclist):
        msgdict = message_converter.convert_ros_message_to_dictionary(msg)
        newitem = {"topic": topic,
                   "timeField": datetime.utcfromtimestamp(t.secs + (t.nsecs/10e6)),
                   "size": len(msgdict),
                   "msg_type": msg._type,
                   "metadataID": newmeta_id}
        result = newitem.update(msgdict)
        try:
            mycol.insert_one(newitem)
            count = count + 1
            # print(count)
            prog.set_stat(count)
            prog.update()
        except pymongo.errors.OperationFailure:
            print("\nerror with message " + msg)
            return -1
        except pymongo.errors.DocumentTooLarge:
            print("\nTopic too large " + topic)
            return -1
        except:
            print("\nError with message of topic " + topic)
            return -1
    return count


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('-d', '--databaseuri', help='mongodb url string', required=True)
    parser.add_argument('-b', '--rosbag', help='rosbag file', required=True)
    parser.add_argument('-v', '--vehicleid', type=int, help='vehicle ID', required=True)
    parser.add_argument('-e', '--experimentid', type=int, help='experiment ID', required=True)
    parser.add_argument('-c', '--collection', default='rosbag', help='Collection Name', required=False)
    parser.add_argument('--force', default=False, dest='force', action='store_true', help='force insert')
    args = parser.parse_args()


    print("Connecting to mongodb at " + args.databaseuri + " / "+args.collection)
    myclient = pymongo.MongoClient(args.databaseuri)#"mongodb://localhost:27017/")
    mydb = myclient["rosbag"]
    mycol = None
    for name in mydb.list_collection_names():
        if(name == args.collection):
            mycol = mydb[args.collection]
        #if(name == "rosbag"):
            #mycol = mydb["rosbag"]
            break

    #mycol = mydb["rosbag"]
    #mycol.drop()
    if(mycol == None):
        print("Creating the collection...")
        mydb.create_collection("rosbag", timeseries={'timeField': 'timeField'})

    print("Loading rosbag")
    bag = rosbag.Bag(args.rosbag)#'DATA_CE_Test_0079.bag')
    #bag = rosbag.Bag('ADS_Deployment1_Bucket3_Mary_2022-04-25-15-35-30.bag')

    print("Scanning messages")
    selecttopiclist = generateFilteredTopicList(bag)
    num_msg = bag.get_message_count(selecttopiclist)


    bagmetadata = generateMetaData(bag, vehicleID=args.vehicleid, experimentnumber=args.experimentid, other={'selectedtopics':selecttopiclist})
    dataexists = checkExistingMetaData(mydb, bagmetadata)
    if(args.force==False and dataexists):
        print("metadata already present")
        sys.exit()
    elif(args.force==1 and dataexists):
        newmeta_id = dataexists
    else:
        print("inserting the metadata tag")
        newmeta_id = insertMetaData(mydb, bagmetadata)


    print("Inserting data # -> " + str(num_msg))
    prog = pyprog.ProgressBar("-> ", " OK!", num_msg)
    prog.update()
    insertMessagesByTopicFilter(mycol, bag, selecttopiclist, newmeta_id, prog)
    prog.end()
    bag.close()

    print("All done")
