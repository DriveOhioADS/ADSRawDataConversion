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
import cyberreader
from google.protobuf.json_format import MessageToJson
# import datetime
# import time
class DatabaseInterface:
    def __init__(self, uristring):
        self.uristring = uristring
        self.cname = None

    def check(self):
        print("class check")

    def db_connect(self):
        print("Connecting")

    def db_insert(self, collection_name, newdata):
        print("Inserting data")

    def setCollectionName(self, cname):
        self.cname = cname


class DatabaseMongo(DatabaseInterface):
    def __init__(self, uristring):
        super().__init__(uristring)
        self.mycol = None
        self.mydb = None
        self.myclient = None
        self.dname = "rosbag"
        self.cname = "rosbag"
        print("init")

    def db_insert_main(self, newdata):
        return self.db_insert(self.cname, newdata)

    def db_insert(self, collection_name, newdata):
        return self.mydb[collection_name].insert_one(newdata)

    def db_connect(self):
        myclient = pymongo.MongoClient(self.uristring)  # "mongodb://localhost:27017/")
        mydb = myclient["rosbag"]
        mycol = None
        for name in mydb.list_collection_names():
            print(name)
            if (name == self.cname):
                mycol = mydb[self.cname]
                print("Found collection: " + name)
                break

        if (mycol == None):
            print("Creating the collection: " + args.collection)
            mydb.create_collection(args.collection, timeseries={'timeField': 'timeField'})
            mycol = mydb[self.cname]

        self.myclient = myclient
        self.mydb = mydb
        self.mycol = mycol

    def db_find_metadata(self, cname, sdata):
        result = self.mydb[cname].find_one(sdata)
        if result != None:
            return result["_id"]
        return None

    # def insert_metadata(self, metadata):
    #     result = self.mydb[self.dname]["metadata"].insert_one(metadata)
    #     if result != None:
    #         return result.inserted_id
    #     return None


def generate_unique_id():
    return uuid.uuid1()


class DatabaseDynamo(DatabaseInterface):
    def __init__(self, uristring):
        super().__init__(uristring)
        print("DynamoDB init")
        self.ddb = None

    def db_connect(self):
        print(f"connecting to dynamodb {self.uristring}")
        # client = boto3.client('dynamodb')
        # ddb = boto3.client('dynamodb', endpoint_url='http://172.31.144.1:8000',
        #                     aws_access_key_id="anything",
        #                     aws_secret_access_key="anything",
        #                     region_name="us-west-2",)
        ddb = boto3.resource('dynamodb', endpoint_url=self.uristring,
                             aws_access_key_id="anything",
                             aws_secret_access_key="anything",
                             region_name="us-west-2", )
        tables = list(ddb.tables.all())
        print(tables)
        self.ddb = ddb

        result = self.checkTableExistsCreateIfNot("metadata")
        if result == 0:
            print("Table check/create issue")
            sys.exit()
        result = self.checkTableExistsCreateIfNot(self.cname)
        if result == 0:
            print("Table check/create issue")
            sys.exit()

    def db_find_metadata(self, cname, sdata):
        sdata = json.loads(json.dumps(sdata), parse_float=Decimal)
        item_to_find = sdata['startTime']
        filter_to_find = Attr('startTime').eq(item_to_find)
        ttable = self.ddb.Table(cname)
        try:
            result = ttable.scan(FilterExpression=filter_to_find)
            if result['Count'] == 0:
                return None
            return result['Items'][0]['_id']
            # mongo only gives ID because its not scanning
            # change from scan to query someday
        except TypeError:
            print("cannot find item")
            return None
        # result = self.mydb[cname].find_one(sdata)
        # if (result != None):
        #    return result["_id"]

    def db_insert_main(self, newdata):
        return self.db_insert(self.cname, newdata)

    def db_insert(self, collection_name, newdata):
        ttable = self.ddb.Table(collection_name)
        # dynamo does not support float only decimal, watch out for datetime

        newdata = json.loads(json.dumps(newdata), parse_float=Decimal)

        # new data is already in json, but needs dynamo format
        # also we need to generate a unique ID
        newUUID = str(generate_unique_id())
        #print(newUUID)
        checkdata = {'_id': newUUID}
        checkdata.update(newdata)
        try:
            ttable.put_item(Item=checkdata)
        except ClientError as ce:
            print(f"\nclient error on insert {ce}")
            sys.exit()
        except TypeError as e:
            print(f"\ntype error on insert {e}")
            #sys.exit()

    def checkTableExistsCreateIfNot(self, tname):
        ddb = self.ddb
        # dynamo only has tables, not dbs+collections, so the collection is table here
        ttable = self.ddb.Table(tname)
        print(f"Looking for table {tname}")

        timeField = 'timeField'
        if (tname == 'metadata'):
            timeField = 'startTime'

        is_table_existing = False
        createTable = False
        try:
            is_table_existing = ttable.table_status in ("CREATING", "UPDATING",
                                                        "DELETING", "ACTIVE")
            print(f"table {tname} already exists, no need to create")
            return 1
        except ClientError:
            print(f"Missing table {tname}")
            createTable = True

        if (createTable):
            try:
                ttable = ddb.create_table(TableName=tname,
                                          KeySchema=[
                                              {
                                                  'AttributeName': '_id',
                                                  'KeyType': 'HASH'
                                              },
                                              {
                                                  'AttributeName': timeField,
                                                  'KeyType': 'RANGE'
                                              }
                                          ],
                                          AttributeDefinitions=[
                                              {
                                                  'AttributeName': '_id',
                                                  'AttributeType': 'S'
                                              },
                                              {
                                                  'AttributeName': timeField,
                                                  'AttributeType': 'N'
                                              }
                                          ],
                                          ProvisionedThroughput={'ReadCapacityUnits': 1, 'WriteCapacityUnits': 1}
                                          )
                print("Waiting for table creation")
                response = ttable.wait_until_exists()
                return 1
            except:
                print("failed to create table")
                return -1
        return -1


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


def checkExistingMetaData(dbobject, metadata):
    searchstring = {"filename": metadata["filename"],
                    "experimentID": metadata["experimentID"],
                    "startTime": metadata['startTime'],
                    "size": metadata['size'],
                    'msgnum': metadata['msgnum']}
    return dbobject.db_find_metadata("metadata", metadata)
    # result = dbconn["metadata"].find_one(searchstring)
    # if (result != None):
    #    return result["_id"]
    # return None


def insertMetaData(dbobject, metadata):
    return dbobject.db_insert("metadata", metadata)
    # result = dbconn["metadata"].insert_one(metadata)
    # return result.inserted_id  # needs to be ID of metadata


count = 0


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


def insertMessagesByTopicFilter(collection, rosbagfile, goodtopiclist, newmeta_id, prog, LiDARbool):
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
        # try:
        result = dbobject.db_insert_main(newitem)
        # iresult = mycol.insert_one(newitem)
        count = count + 1
        # print(count)
        prog.set_stat(count)
        prog.update()
        # except pymongo.errors.OperationFailure:
        #     print("\nOpFail Error with message " + msg + " => " + msg._type)
        #     return -1
        # except pymongo.errors.DocumentTooLarge:
        #     print("\nTopic too large " + topic + " => " + msg._type)
        #     return -1
        # except Exception as ex:
        #     print("\nError with message of topic " + topic + " => " + msg._type)
        #     template = "An exception of type {0} occurred. Arguments:\n{1!r}"
        #     message = template.format(type(ex).__name__, ex.args)
        #     print(message)
        #     return -1
    return count

def ProcessRosbagFile(args, dbobject):
    bag = rosbag.Bag(args.rosbag)

    IncludeLiDAR = False
    if (args.lidar):
        IncludeLiDAR = True
        print("Including PointCloud2 LiDAR data")
        # print("Inserting LiDAR # -> ")
        # #prog = pyprog.ProgressBar("-> ", " OK!", num_msg)
        # #prog.update()
        # insertLiDARMessages(mycol, bag, newmeta_id=0)
        # #prog.end()
        # bag.close()
    else:
        print("Skipping LiDAR messages")

    print("Scanning messages")
    selecttopiclist = generateFilteredTopicList(bag, PointCloud2=IncludeLiDAR)
    num_msg = bag.get_message_count(selecttopiclist)

    bagmetadata = generateMetaData(bag, vehicleID=args.vehicleid, experimentnumber=args.experimentid,
                                other={'selectedtopics': selecttopiclist})
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
    insertMessagesByTopicFilter(dbobject, bag, selecttopiclist, newmeta_id, prog, LiDARbool=IncludeLiDAR)
    prog.end()
    bag.close()


def ProcessCyberFile(args, dbobject):
    filename = args.cyber
    unqiue_channel = []
    #filename = sys.argv[1]
    pbfactory = cyberreader.ProtobufFactory()
    reader = cyberreader.RecordReader(filename)
    for channel in reader.GetChannelList():
        desc = reader.GetProtoDesc(channel)
        pbfactory.RegisterMessage(desc)
        unqiue_channel.append(channel)
        
    message = cyberreader.RecordMessage()
    count = 0
    while reader.ReadMessage(message):
        message_type = reader.GetMessageType(message.channel_name)
        msg = pbfactory.GenerateMessageByType(message_type)
        msg.ParseFromString(message.content)
        print(message.channel_name)
        if(message.channel_name == "/tf" or
            message.channel_name == "/apollo/sensor/gnss/raw_data" or
            message.channel_name == "/apollo/sensor/gnss/corrected_imu" or
            message.channel_name == "/apollo/localization/pose"):
            #print("msg[%d]-> channel name: %s; message type: %s; message time: %d, content: %s" % (count, message.channel_name, message_type, message.time, msg))
            jdata = MessageToJson(msg)
            print(jdata)
            #todo have accept/deny list
            #insert into database
    print("Message Count %d" % count)
    
      
if __name__ == '__main__':

    parser = argparse.ArgumentParser()
    parser.add_argument('--dynamodb', help='dynamo url string', required=False)
    parser.add_argument('--mongodb', help='mongodb url string', required=False)
    parser.add_argument('--cyber', help='cyber data folder', required=False)
    parser.add_argument('-b', '--rosbag', help='rosbag file', required=False)
    parser.add_argument('-v', '--vehicleid', type=int, help='vehicle ID', required=True)
    parser.add_argument('-e', '--experimentid', type=int, help='experiment ID', required=True)
    parser.add_argument('-c', '--collection', default='rosbag', help='Collection Name', required=False)
    parser.add_argument('--lidar', default='', dest='lidar', action='store_true', help='Insert LiDAR', required=False)
    parser.add_argument('--force', default=False, dest='force', action='store_true', help='force insert')
    args = parser.parse_args()

    if (args.mongodb != None):
        print("Connecting to database at " + args.mongodb + " / " + args.collection)
        dbobject = DatabaseMongo(args.mongodb)
        dbobject.check()
        # dbobject.setCollectionName(args.collection)
        # dbobject.db_connect()
    elif (args.dynamodb != None):
        print("Connecting to database at " + args.dynamodb + " / " + args.collection)
        dbobject = DatabaseDynamo(args.dynamodb)
        dbobject.check()

        # dbobject.db_connect()
        # dbobject.db_insert('test', "test")
        # sys.exit()
    else:
        print("No database specified")
        sys.exit()

    dbobject.setCollectionName(args.collection)
    dbobject.db_connect()
    
    if(args.cyber != None):
        print('Processing Cyber data')
        ProcessCyberFile(args, dbobject)
    elif(args.rosbag != None):
        print("Loading rosbag")
        ProcessRosbagFile(args, dbobject)
        
    else:
        print("No data file source specified")
        sys.exit()

    print("All done")
