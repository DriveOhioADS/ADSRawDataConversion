import os
import sys
import glob
import json
from datetime import datetime
import cyberreaderlib as cyberreader
from google.protobuf.json_format import MessageToJson
from databaseinterface import DatabaseDynamo, DatabaseMongo
import pyprog
import pymongo
class CyberReader:
    def __init__(self, foldername=None, basefilename=None):
        self.foldername = foldername
        self.basefilename = basefilename
        self.message = cyberreader.RecordMessage()
        self.reader = None
        self.pbfactory = None
        self.unique_channels = None
        self.filelist = None
        self.message_process_count = 0
        self.totalmessagecount = 0

    # def ScanChannelFolder(self):
    #     all_channels = []
    #     #folderlist = os.listdir(self.foldername)
    #     # todo: path combine instead of +
    #     filelist = glob.glob(self.foldername+"/"+self.basefilename+"*")
    #     for file in filelist:
    #         new_channels = []
    #         new_channels = self.ScanChannelsSingleFile(file)
    #         #print(len(new_channels))
    #         all_channels = list(set(all_channels + new_channels))
    #     self.unique_channels = all_channels
    #     return all_channels
    
    # def ScanChannelsSingleFile(self, filename):
    #     unqiue_channel = []
    #     # filename = sys.argv[1]
    #     self.pbfactory = cyberreader.ProtobufFactory()
    #     reader = cyberreader.RecordReader(filename)
    #     for channel in reader.GetChannelList():
    #         desc = reader.GetProtoDesc(channel)
    #         self.pbfactory.RegisterMessage(desc)
    #         unqiue_channel.append(channel)
    #         #print(channel)
    #     self.unique_channels = unqiue_channel
    #     self.reader = None
    #     return unqiue_channel

    # def GetNextMessageFromFolder(self):
    #     if(self.filelist == None):
    #         #todo fix path combine
    #         self.filelist = glob.glob(self.foldername+"/"+self.basefilename+"*")
    #         self.SetCurrentFile(self.filelist.pop(0))
    #         return self.GetNextMessageFromFile()
    #     else:
    #         msg = self.GetNextMessageFromFile()
    #         if(msg == None and len(self.filelist) > 0):
    #             self.SetCurrentFile(self.filelist.pop(0))
    #             return self.GetNextMessageFromFile()
    #         elif(msg == None and len(self.filelist) == 0):
    #             #end of files and messages
    #             return None
    #         else:
    #             return msg
        
    
    # def SetCurrentFile(self, filename):
    #     self.curr_filename = filename
    #     self.reader = None
    #     print(self.message_process_count)
    #     print("Moving to file: "+ self.curr_filename)

        
    # def GetNextMessageFromFile(self):
    #     if(self.reader == None):
    #         self.reader = cyberreader.RecordReader(self.curr_filename)
    #         if(self.reader.is_valid != True):
    #             print("Invalid file")
    #             sys.exit(-1)
    #     if(self.pbfactory == None):
    #         print("need to scan channels first")
    #         sys.exit(-1)
               
    #     message = self.message
    #     if(self.reader.ReadMessage(message)):
    #         self.message_process_count = self.message_process_count + 1
    #         self.totalmessagecount = self.totalmessagecount + 1
    #         message_type = self.reader.GetMessageType(message.channel_name)
    #         msg = self.pbfactory.GenerateMessageByType(message_type)
    #         msg.ParseFromString(message.content)
    #         #print(message.channel_name)
    #         # if(message.channel_name == "/tf" or
    #         #     message.channel_name == "/apollo/sensor/gnss/raw_data" or
    #         #     message.channel_name == "/apollo/sensor/gnss/corrected_imu" or
    #         #     message.channel_name == "/apollo/localization/pose"):
    #             #print("msg[%d]-> channel name: %s; message type: %s; message time: %d, content: %s" % (count, message.channel_name, message_type, message.time, msg))
    #         if(message.channel_name == "/apollo/sensor/camera/front_6mm/image" or
    #             message.channel_name == "/apollo/sensor/camera/front_6mm/image/compressed" or
    #             message.channel_name == "/apollo/sensor/camera/front_25mm/image" or
    #             message.channel_name == "/apollo/sensor/camera/front_25mm/image/compressed" or
    #             message.channel_name == "/apollo/sensor/velodyne32/PointCloud2" or
    #             message.channel_name == "/apollo/sensor/velodyne32/VelodyneScan" or
    #             message.channel_name == "/apollo/prediction/perception_obstacles"):
    #             return 0
    #         else:
    #             jdata = MessageToJson(msg)
    #             #print(jdata)
    #             # todo add metadata, time, channel, etc.
    #             return jdata
    #             #todo have accept/deny list
    #             #insert into database
    #     else:
    #         # all out of messages for this file
    #         return None
        
    def InsertDataFromFolder(self, dbobject, metadatasource,
                             channelList=None,
                             forceInsert=False):
        
        print("Inserting cyberdata from folder " + self.foldername)

        unique_channels = []
        #todo fix the path combine
        filelist = glob.glob(self.foldername+"/"+self.basefilename+"*")
        self.totalmessagecount = 0
        for filename in filelist:
            pbfactory = cyberreader.ProtobufFactory()
            reader = cyberreader.RecordReader(filename)
            for channel in reader.GetChannelList():
                desc = reader.GetProtoDesc(channel)
                pbfactory.RegisterMessage(desc)
                unique_channels.append(channel)
            
            #have todo this for each file in case allow is blank and gets set
            deny_channels=None
            allow_channels=None
            if(channelList != None):
                if(channelList['deny'] != None):
                    deny_channels=channelList['deny']
                if(channelList['allow'] != None):
                    allow_channels=channelList['allow']
            if(allow_channels == None):
                allow_channels = set(unique_channels)
            #run check that gives priority to deny
            for deny in deny_channels:
                if(deny in allow_channels):
                    allow_channels.remove(deny)
            #have to wait until startime is found for each file
            print(f"Checking cyber metadata for file {filename}")
        
            #todo check time format for start and end
            specificmeta = {
                'filename': filename,
                'foldername': self.foldername,
                'startTime': datetime.utcfromtimestamp(reader.header.begin_time/1000000000),
                'endTime': datetime.utcfromtimestamp(reader.header.end_time/1000000000),
                'msgnum': reader.header.message_number,
                'size': reader.header.size,
                'topics': unique_channels,
                #'deny': deny_channels,
                #'allow': allow_channels,
                'type': 'cyber'
            }
            specificmeta.update(metadatasource)
            metadata_search = dbobject.db_find_metadata_by_startTime('metadata', specificmeta['startTime'])
            if(metadata_search == None):
                result = dbobject.db_insert("metadata", specificmeta)
                if(result == -1):
                    print(f"metadata insert from cyber failed {filename}")
                    return -1
                #check the insert was good
                metadata_search = dbobject.db_find_metadata_by_id('metadata', result.inserted_id)
                if(metadata_search == None):
                    print(f"metadata check from cyber failed {filename}")
                    return -1
            elif not forceInsert:
                print(f"metadata for {filename} already exists, data most likely is already present. Override with --force")
                continue
                           
            #start the message extract process

            message = cyberreader.RecordMessage()
            num_msg = reader.header.message_number
            print("Inserting data # -> " + str(num_msg))
            prog = pyprog.ProgressBar("-> ", " OK!", num_msg)
            prog.update()
            msgcount = 0
            while reader.ReadMessage(message):
                self.totalmessagecount = self.totalmessagecount + 1
                msgcount = msgcount + 1
                
                prog.set_stat(msgcount)
                prog.update()
                
                message_type = reader.GetMessageType(message.channel_name)
                msg = pbfactory.GenerateMessageByType(message_type)
                msg.ParseFromString(message.content)
                #print(message.channel_name)
                if(message.channel_name not in deny_channels and
                   message.channel_name in allow_channels):
                    try:
                        jdata = json.loads(MessageToJson(msg))
                    except:
                        print("json conversion failed")
                        sys.exit(-1)
                    
                    try:
                        ntime = datetime.utcfromtimestamp(message.time/1000000000)#t.secs + (t.nsecs / 10e6)).timestamp()
                    except:
                        print("cyber time to timestamp failed")
                        sys.exit(-1)
                        
                    newmeta_id = metadata_search
                    newitem = {
                        "topic": message.channel_name,
                        "timeField": ntime,
                        "size": len(message.content), #todo check this
                        "msg_type": "",     #msg._type,
                        "metadataID": newmeta_id}
                    try:
                        jdata = json.loads(MessageToJson(msg))
                    except Exception as e:
                        print("cyber message to json failed")
                        return -1
                    
                    newitem.update(jdata)
                    dbobject.db_insert_main(newitem)
                
                    #print("msg[%d]-> channel name: %s; message type: %s; message time: %d, content: %s" % (count, message.channel_name, message_type, message.time, msg))
                    #
                    #print(jdata)
                #else:
                #    print("Ignore " + message.channel_name)
            prog.end()
            print(f"Message Count {msgcount}")
                         
if __name__ == "__main__":
    #cr = CyberReader("/mnt/e/van/apollo_ridges_cyberbags",basefilename='20221117125313.record')
    cr = CyberReader("../delete",basefilename='20221117125313.record')
    #ch = cr.ScanChannelFolder()
    #print(ch)
    
    # cr.SetCurrentFile('20221117125313.record.00000')
    # msg = 1
    # while (msg != 0):
    #     msg = cr.GetNextMessageFromFile()
    #     print(msg)
    
    # msg = 1
    # while(msg != None):
    #     msg = cr.GetNextMessageFromFolder()
    #     print(cr.message_process_count)
    #172.31.48.1
    
    deny_channels = set(["/apollo/sensor/camera/front_6mm/image",
        "/apollo/sensor/camera/front_6mm/image/compressed",
        "/apollo/sensor/camera/front_25mm/image",
        "/apollo/sensor/camera/front_25mm/image/compressed",
        "/apollo/sensor/velodyne32/PointCloud2",
        "/apollo/sensor/velodyne32/VelodyneScan",
        "/apollo/prediction/perception_obstacles"])
    
    channelList={
                'deny': deny_channels,
                'allow': None
                }
    
    dbobject = DatabaseMongo("mongodb://172.31.32.1:27017")
    dbobject.setCollectionName("cyber")
    dbobject.db_connect()
    metadatasource = {
                        'vehicleID': 8,
                        'experimentID': 8,
                        'other': 0,
                     }
    
    cr.InsertDataFromFolder(dbobject, metadatasource, channelList, forceInsert=True)
