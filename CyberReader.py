import os
import sys
import glob
import json
from datetime import datetime
import cyberreaderlib as cyberreader
from google.protobuf.json_format import MessageToJson
from databaseinterface import DatabaseDynamo, DatabaseMongo
import databaseinterface
#import pyprog
from tqdm import tqdm
import logging
import time
import datetime
import uuid

class CyberReader:
    def __init__(self, rootdir=None, foldername=None, basefilename=None):
        self.rootdir = rootdir
        self.foldername = foldername
        self.basefilename = basefilename
        self.message = cyberreader.RecordMessage()
        self.reader = None
        self.pbfactory = None
        self.unique_channels = None
        self.filelist = None
        self.message_process_count = 0
        self.totalmessagecount = 0

    def ScanChannelFolder(self,fullfoldername):
        all_channels = []
        filelist = glob.glob(os.path.join(fullfoldername,self.basefilename+"*"))
        logging.info(os.path.join(fullfoldername,self.basefilename+"*"))
        logging.info(filelist)
        for file in filelist:
            new_channels = []
            new_channels = self.ScanChannelsSingleFile(file)
            #print(len(new_channels))
            all_channels = list(set(all_channels + new_channels))
        self.unique_channels = all_channels
        return all_channels
    
    def ScanChannelsSingleFile(self, filename):
        unqiue_channel = []
        self.pbfactory = cyberreader.ProtobufFactory()
        reader = cyberreader.RecordReader(filename)
        for channel in reader.GetChannelList():
            desc = reader.GetProtoDesc(channel)
            self.pbfactory.RegisterMessage(desc)
            unqiue_channel.append(channel)
            #print(channel)
        self.unique_channels = unqiue_channel
        self.reader = None
        return unqiue_channel

       
    def InsertDataFromFolder(self, dbobject, metadatasource,
                             channelList=None,
                             forceInsert=False,
                             batch=False):
        
        fullfoldername = os.path.join(self.rootdir,self.foldername)

        logging.info("Scanning folder to get list of all channels:")
        all_channels = self.ScanChannelFolder(fullfoldername)
        for channel in all_channels:
            logging.info(channel)
        
        logging.info("Inserting cyberdata from folder " + fullfoldername)

        unique_channels = []
        filelist = sorted(glob.glob(os.path.join(fullfoldername,self.basefilename + "*")))
        self.totalmessagecount = 0
        filecount = 0
        
        gpID = ""
        try:
            with open(os.path.join(fullfoldername,"groupid.txt"),'r') as f:
                gpID = f.read()
                logging.info("reading groupID:"+gpID)
        except:
            logging.info("no groupid file found") 

        if(len(gpID)==0):
            groupMetaDataID = str(uuid.uuid1())
            logging.info(f"NEW ID for this insert is {groupMetaDataID}")
            open(os.path.join(fullfoldername,"groupid.txt"),'w').write(groupMetaDataID)
        else:
            groupMetaDataID = gpID

        
        for filename in filelist:
            filecount = filecount + 1
            pbfactory = cyberreader.ProtobufFactory()
            reader = cyberreader.RecordReader(filename)
            for channel in reader.GetChannelList():
                desc = reader.GetProtoDesc(channel)
                pbfactory.RegisterMessage(desc)
                unique_channels.append(channel)
            
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
            logging.info(f"Checking cyber metadata for file {filename}")
        
            #timeName = 'startTime'
            timeName = databaseinterface.TIME_FIELD_NAME
            mfilename = filename.replace(self.rootdir,'')
            specificmeta = {
                'filename': mfilename,
                'foldername': self.foldername,
                databaseinterface.TIME_FIELD_NAME: reader.header.begin_time,#datetime.utcfromtimestamp(reader.header.begin_time/1000000000),
                'endTime': reader.header.end_time,#datetime.utcfromtimestamp(reader.header.end_time/1000000000),
                'msgnum': reader.header.message_number,
                'headerSize': reader.header.size,
                'topics': unique_channels,
                #'deny': deny_channels, #having the full list and deny/accept was too much for mongo
                #'allow': allow_channels,
                'dataType': 'cyber',
                'groupMetadataID': groupMetaDataID,
                'insertDateTime': str(datetime.datetime.now())
            }
            specificmeta.update(metadatasource)
            #print(specificmeta)
            logging.info(f"Looking for meta time {specificmeta[timeName]}")
            metadata_search = dbobject.db_find_metadata_by_startTime(dbobject.metatablename, specificmeta[timeName])
            if(metadata_search == None):
                logging.info(f"Did not find it, so inserting meta object {specificmeta[timeName]}")
                insert_result = dbobject.db_insert(dbobject.metatablename, specificmeta)
                if(insert_result == -1):
                    logging.error(f"metadata insert from cyber failed {filename}")
                    return -1
                metadata_search = insert_result
                #check the insert was good
                
                logging.info(f"Looking for meta time again {specificmeta[timeName]}")
                time.sleep(0.5)
                metadata_search = dbobject.db_find_metadata_by_id(dbobject.metatablename, insert_result)
                if(metadata_search is not None):
                    logging.info(f"Meta object found again {specificmeta[timeName]}")  
                elif(metadata_search == None):
                    logging.error(f"metadata check from cyber failed {filename}")
                    return -1

            elif not forceInsert:
                logging.warning(f"metadata for {filename} already exists, data most likely is already present. Override with --force")
                continue
                           
            
            #setup batch, if requested
            if(batch):
                batchobject = dbobject.db_getBatchWriter() #forces creation of the batch writer
                logging.info('using batch mode')
            #start the message extract process
            message = cyberreader.RecordMessage()
            num_msg = reader.header.message_number
            logging.info(f"Inserting data # -> {str(num_msg)} file {filecount} of {len(filelist)}")
            #prog = pyprog.ProgressBar("-> ", " OK!", num_msg)
            #prog.update()
            pbar = tqdm(total=num_msg)
            msgcount = 0
            numinsert = 0
            
            try:
                while reader.ReadMessage(message):
                    self.totalmessagecount = self.totalmessagecount + 1      
        
                    try:
                        message_type = reader.GetMessageType(message.channel_name)
                        msg = pbfactory.GenerateMessageByType(message_type)
                        msg.ParseFromString(message.content)
                    except KeyError:
                        logging.info(f"Stopping current file, error finding the message {message.channel_name}")
                        return None

                    if(message.channel_name not in deny_channels and
                        message.channel_name in allow_channels):
                        jdata=None
                        try:
                            jdata = json.loads(MessageToJson(msg))
                            jdata['time'] = message.time
                            jdata['topic'] = message.channel_name
                            jdata['msgsize'] = len(message.content)
                        except:
                            logging.info("json conversion failed")
                            sys.exit(-1)
                        result = self.ProcessMessage(jdata,
                                                    metadata_search, groupMetaDataID, 
                                                    dbobject, numinsert, batch=batch)
                        if(result is None):
                            logging.error("Stopping processing for this file, unable to process message")
                            break
                        numinsert = result
                    msgcount = msgcount + 1
                    pbar.update()
                    #prog.set_stat(msgcount)
                    #prog.update()
            except: #google.protobuf.message.DecodeError as e:
                e='not implemented'
                logging.info(f"message read error {e}")
            if(batch):#make sure the last bit gets sent
                dbobject.FlushBatch()
            pbar.close()#prog.end()
            logging.info(f"Insert Count:{numinsert}")
            logging.info(f"Message Count {msgcount}")
        dbobject.db_close()
        logging.info(f"Processed {filecount} files")
    
    def ProcessMessage(self, jdata, metadata_search, groupMetaDataID, dbobject, numinsert, batch=False):
             
        newmeta_id = metadata_search
        newitem = {
            #"topic": jdata['channel_name'],#message.channel_name,
            databaseinterface.TIME_FIELD_NAME: jdata['time'], #remove isoformat todo .isoformat()
            #"msgsize": len(jdata['content']),#message.content), 
            "msg_type": "",     #msg._type,
            "metadataID": newmeta_id,
            "groupMetadataID": groupMetaDataID} #todo remove str force 
    
        newitem.update(jdata)   
        ######################################################
        if(newitem['msgsize'] < 400000):
            if(batch):
                dbobject.db_putItemBatch(newitem)     
            else:
                dbobject.db_insert_main(newitem)
        else:
            logging.info(f"Skipping message {newitem['topic']} because of size")
            return numinsert
            
        numinsert = numinsert + 1
        return numinsert
        ######################################################
        
                                    
#if __name__ == "__main__":
    
    #cr = CyberReader(foldername=sys.argv[1], basefilename=sys.argv[2])
    # cr = CyberReader('/mnt/h/cyberdata','20221117125313.record.00000')
    # # scan a folder
    # #ch = cr.ScanChannelFolder()
    # #print(ch)
    
    # # read a single file
    # # cr.SetCurrentFile('20221117125313.record.00000')
    # # msg = 1
    # # while (msg != 0):
    # #     msg = cr.GetNextMessageFromFile()
    # #     print(msg)
    
    # # read a folder with re-entry
    # # msg = 1
    # # while(msg != None):
    # #     msg = cr.GetNextMessageFromFolder()
    # #     print(cr.message_process_count)

    # logging.basicConfig(filename="cyberreader.log", encoding='utf-8', level=logging.DEBUG)
    # logging.getLogger().addHandler(logging.StreamHandler(sys.stdout))

    # deny_channels = set([
    #     "/apollo/sensor/camera/front_6mm/image",
    #     "/apollo/sensor/camera/front_6mm/image/compressed",
    #     "/apollo/sensor/camera/front_25mm/image",
    #     "/apollo/sensor/camera/front_25mm/image/compressed",
    #     "/apollo/sensor/velodyne32/PointCloud2",
    #     "/apollo/sensor/velodyne32/VelodyneScan",
    #     #"/apollo/prediction/perception_obstacles",
    #     #"/apollo/perception/obstacles",
    #     #"/apollo/prediction",
    #     #"/apollo/planning"
    #     ])
    
    # channelList={
    #             'deny': deny_channels,
    #             'allow': None
    #             }
    
    # dbobject = DatabaseMongo("mongodb://windows:27017",'cyber')
    # dbobject.setCollectionName("cyber")
    # dbobject.db_connect()
    # metadatasource = {
    #                     'vehicleID': 8,
    #                     'experimentID': 8,
    #                     'other': 0,
    #                  }
    
    #cr.InsertDataFromFolder(dbobject, metadatasource, channelList, forceInsert=True)
