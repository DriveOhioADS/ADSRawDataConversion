import sys
import json
from datetime import datetime
import argparse
from decimal import Decimal
from databaseinterface import DatabaseInterface
import logging
import os

def ProcessRosbagFile(file, dbobject, channelList, metadata, force):
    from RosReader import RosReader
    rr = RosReader()
    return rr.ProcessFile(dbobject=dbobject, metadatasource=metadata,
                          channelList=channelList, 
                          force=force, process_lidar=False)


def ProcessCyberFile(cyberfolder, cyberfilebase, dbobject, channelList, metadata, force, batch,rootdir):
    from CyberReader import CyberReader
    cr = CyberReader(rootdir, cyberfolder, cyberfilebase) #rootpath + rootdir + cyberfilebase (isolate out to remove the root later on)
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
        channelList = {'deny': deny,'allow': allow}
    cr.InsertDataFromFolder(dbobject, metadata, channelList, force, batch)   
    return 0

def checkKey(dict, key):
    if(key in dict):
        return True
    return False
    
def main(args):
    #logging.basicConfig()
    #logging.basicConfig(level = logging.INFO)
    #logging.basicConfig(filename='insert.log', filemode='a', encoding='utf-8', level=logging.DEBUG)
    
    try:
        with open(args.config, 'r') as file:
            config = json.load(file)
        if(not checkKey(config, 'file')):
            logging.error("file is required")
        if(not checkKey(config['file'], 'type')):
            logging.error("file - type is required")
        if(not checkKey(config, 'metadata')):
            logging.error("metadata section is required")
        if(not checkKey(config, 'database')):
            logging.error("database section is required")
        if(not checkKey(config['database'], 'type')):
            logging.error("database - type is required")
        if(not checkKey(config['database'], 'databasename')):
            logging.error("database - databasename is required")
        if(not checkKey(config['database'], 'uri')):
            logging.error("database - uri is required")
        if(not checkKey(config['database'], 'collection')):
            logging.error("database - collection is required")
    except:
        logging.error(f"failed to load config from file {args.config}")
        return -1

    logging.info("Creating connection to database:")
    if (config['database']['type'] == 'mongo'):
        logging.info(f"Connecting to database at {config['database']['uri']} / {config['database']['collection']}")
    elif (config['database']['type'] ==  'dynamo'):
        logging.info(f"Connecting to database at {config['database']['uri']} / {config['database']['collection']}")   
    elif (config['database']['type'] ==  'djson'):
        logging.info(f"Using DynamoDB json export to files")
    else:
        logging.error(f"No database specified: {config['database']['type']}")
        sys.exit()
    
    if(config['database']['databasename'] == config['database']['metatablename']):
        logging.error("Data table and metadata name cannot be the same!")
        sys.exit()
        
    dbobject = DatabaseInterface.CreateDatabaseInterface(config['database']['type'], 
                                                         config['database']['uri'], 
                                                         config['database']['databasename'],
                                                         metatablename=config['database']['metatablename'])
   
    if (config['database']['type'] ==  'dynamo' and checkKey(config['database'], 'throughputSleep')):
        dbobject.throughputSleep=config['database']['throughputSleep']
    if (config['database']['type'] ==  'dynamo' and checkKey(config['database'], 'throughputExceededRepeat')):
        dbobject.throughputExceededRepeat=config['database']['throughputExceededRepeat']
    if (config['database']['type'] ==  'djson' and checkKey(config['database'], 'sizelimit')):
        dbobject.setFileLimit(config['database']['sizelimit'])
    if (config['database']['type'] ==  'djson' and checkKey(config['database'], 'sizelimit')):
        dbobject.setFileExportLocation(config['database']['fileexportlocation'])
        
    dbobject.setCollectionName(config['database']['collection'])
    dbobject.db_connect()  
    if(args.checktables):
        dbobject.CheckAllTables()
    json_channels = None
    if('channelList' in config):
        json_channels = config['channelList']

    if(args.rootdir != None):  
        foldername = config['file']['folder']
        rootdir=args.rootdir
    else:
        foldername = config['file']['folder']
        rootdir=""

    logging.info(f"Folder to use: {rootdir}{foldername}")
    ret = 0
    if(config['file']['type'] == 'cyber'):
        logging.info('Processing Cyber data')
        batchmode = False
        if('batch' in config['database']):
            batchmode = config['database']['batch']
        ret = ProcessCyberFile(cyberfolder=foldername,cyberfilebase=config['file']['filebase'], 
                         dbobject=dbobject,
                         channelList=json_channels,
                         metadata=config['metadata'], force=args.force, batch = batchmode,rootdir=rootdir)
    elif(config['file']['type'] == 'rosbag'):
        logging.info("Loading rosbag")
        ret = ProcessRosbagFile(file=config['file']['filename'],
                          dbobject=dbobject, 
                          channeList=json_channels, 
                          metadata=config['metadata'], force=args.force)  
    else:
        logging.error(f"No data file source specified: {config['file']['type']}")
        sys.exit()
    if(ret != 0):
        logging.error("Failure to finish")
    logging.info("All done")
      
if __name__ == '__main__':
    logging.basicConfig(
    level=logging.DEBUG,
    format='%(asctime)s - %(levelname)s - %(message)s',
    #filemode='a',
    handlers=[
        logging.FileHandler('insert.log','a'),
        logging.StreamHandler()
    ]
    )
    logging.info("Welcome to the ADS data to database process - by Wilhelm - jwilhelm@ohio.edu")
    logging.warning("Warning test")
    logging.info("Info test")
    logging.error("Error test")
    logging.critical("Critical test")
    #logging.basicConfig(filename="insert.log", encoding='utf-8', level=logging.DEBUG)
    #logging.getLogger().addHandler(logging.StreamHandler(sys.stdout))

    #logging.info("datainsert start")
    parser = argparse.ArgumentParser()
    parser.add_argument("--config", help='JSON formatted settings file', required=True)
    parser.add_argument('--lidar', default='', dest='lidar', action='store_true', help='Insert LiDAR', required=False)
    parser.add_argument('--force', default=False, dest='force', action='store_true', help='force insert')
    parser.add_argument('--checktables', default=False, dest='checktables', action='store_true', help='check for tables and create if missing')
    parser.add_argument('--rootdir', dest='rootdir', help='root dir for data for path prefix')
    try:
        args = parser.parse_args()
    except:
        logging.error("argument parsing failed")
        sys.exit(-1)
    
    main(args)
