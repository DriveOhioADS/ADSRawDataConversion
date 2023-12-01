import sys
import os
import json
#from bson import json_util
#from rospy_message_converter import message_converter
#from datetime import datetime
#import sensor_msgs.point_cloud2 as pc2
#import numpy as np
import pymongo
from os import environ as env
from dotenv import find_dotenv, load_dotenv
import uuid
import boto3
from boto3.dynamodb.conditions import Key, Attr
from botocore.exceptions import ClientError
from decimal import Decimal
#from google.protobuf.json_format import MessageToJson
import logging
import time
import datetime
from tinydb import TinyDB, Query
from dynamodb_json import json_util as djson
import botocore.exceptions

#boto3.set_stream_logger('boto', 'logs/boto.log')
logging.getLogger('boto3').setLevel(logging.CRITICAL)
logging.getLogger('botocore').setLevel(logging.CRITICAL)
logging.getLogger('nose').setLevel(logging.CRITICAL)

class DatabaseInterface:
    def __init__(self, uristring):
        self.uristring = uristring
        self.cname = None
        self.type = ''
        self.fileexportloc = ""
        self.filesizelimit = 100e6
        self.metatablename = "metadata"

    def check(self):
        print("class check")

    def db_connect(self):
        print("Connecting")

    def db_insert(self, collection_name, newdata):
        print("Inserting data")

    def setCollectionName(self, cname):
        self.cname = cname
    
    def CreateDatabaseInterface(type, uri, dbname,metatablename="metadata"):
        if(type == 'mongo'):
            obj = DatabaseMongo(uri, dbname)
        elif(type == 'dynamo'):
            obj = DatabaseDynamo(uri, dbname)
        elif(type == 'djson'):
            obj = DatabaseExport(uri, dbname)
        obj.type = type
        obj.metatablename = metatablename
        return obj

    def setFileLimit(self, limit):
        self.filesizelimit = limit
    def setFileExportLocation(self, loc):
        self.fileexportloc = loc
            
class DatabaseMongo(DatabaseInterface):
    def __init__(self, uristring, dbname):
        super().__init__(uristring)
        self.mycol = None
        self.mydb = None
        self.myclient = None
        self.dname = dbname
        #self.cname = collection
        print("init")

    def db_insert_main(self, newdata):
        return self.db_insert(self.cname, newdata)

    def db_insert(self, collection_name, newdata):
        # try:
        result = self.mydb[collection_name].insert_one(newdata)
        return result.inserted_id
        # except pymongo.errors.OperationFailure:
        #     print("\ndb_insert OperationFailure")
        #     return -1
        # except pymongo.errors.DocumentTooLarge:
        #     print("\ndb_insert DocumentTooLarge")
        #     return -1
        # except Exception as ex:
        #     logging.error("\ndb_insert Exception")      
        #     logging.error(newdata)         
        #     template = "An exception of type {0} occurred. Arguments:\n{1!r}"
        #     message = template.format(type(ex).__name__, ex.args)
        #     logging.error(message)
        #     sys.exit(-1)
        #     return -1
    def db_close(self):
        return 0
    
    def db_connect(self):
        mclient = pymongo.MongoClient(self.uristring)  # "mongodb://localhost:27017/")
        mydb = mclient[self.dname]
        mycol = None
        try:
            mclient.server_info() 
        except pymongo.errors.ServerSelectionTimeoutError as err:
            logging.error(f"error -> unable to connect with mongo server {self.uristring}")
            sys.exit(-1)

        for name in mydb.list_collection_names():
            print(name)
            if (name == self.cname):
                mycol = mydb[self.cname]
                print("Found collection: " + name)
                break

        if (mycol == None):
            print("Creating the collection: " + self.cname)
            mydb.create_collection(self.cname)#, timeseries={'timeField': 'timeField'})
            mycol = mydb[self.cname]

        self.myclient = mclient
        self.mydb = mydb
        self.mycol = mycol


    def db_find_metadata_by_startTime(self, cname, key):
        return self.__db_find_metadata(cname, {'time': {'$eq':key}})

    def db_find_metadata_by_id(self, cname, key):
        return self.__db_find_metadata(cname, {'_id': {'$eq':key}})
        
    def __db_find_metadata(self, cname, filter):
        #key = sdata['startTime']
        result = self.mydb[cname].find_one(filter)
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

class DatabaseExport(DatabaseInterface):
    def __init__(self, uri, collection):
        self.basedjsonfile='tempdata'
        #self.djson_sizelimit = 10e6
        self.dlistsize=0
        #self.djson_file=None
        self.dfilecount=0
        self.ddatalist=[]
        self.metadatafile='metadb'
        
        self.cname = collection
        
    def db_connect(self):
        print('using djson export')
        self.tinydbmetaddata = TinyDB(os.path.join(self.fileexportloc,self.metadatafile))
        
    def db_close(self):
        print('closing')
        self._writeoutfile()
        
    def _writeoutfile(self):
        print('writing file...')
        ddata = djson.dumps(self.ddatalist)
        filelong = os.path.join(self.fileexportloc,self.basedjsonfile + str(self.dfilecount)+'.txt')
        with open(filelong,'+w') as writer:
            writer.write(ddata)
        self.dfilecount=self.dfilecount+1
        self.dlistsize=0
        self.ddatalist=[]
        
    def db_insert(self, collection, newdata):
        newdata = DatabaseDynamo._prepDataForInsert(collection, newdata)
        newdata = djson.loads(newdata)
        if(collection == 'metadata'):
            self.tinydbmetaddata.insert(newdata)
            return newdata["_id"]
        else: 
            datalen = len(djson.dumps(newdata))
            if(self.dlistsize + datalen >= self.filesizelimit):
                self._writeoutfile()
            
            self.ddatalist.append(newdata)
            self.dlistsize = self.dlistsize + datalen
            if(self.dlistsize >= self.filesizelimit):
                self._writeoutfile()
                
            return newdata["_id"]
           
    def db_insert_main(self, newdata):
        self.db_insert(self.cname, newdata)
        
    def db_find_metadata_by_startTime(self, cname, key):
        q = Query()
        key = json.loads(json.dumps(key, indent=4, sort_keys=True, default=str), parse_float=Decimal)
        result = self.tinydbmetaddata.search(q.startTime == key)
        if(len(result) <= 0):
            result = None
        else:
            result = result[0]['_id']
        return result
    
    def db_find_metadata_by_id(self, cname, key):
        q = Query()
        result = self.tinydbmetaddata.search(q._id == key)
        if(len(result) <= 0):
            result = None
        else:
            result = result[0]['_id']
        return result
        
            
class DatabaseDynamo(DatabaseInterface):
    throughputSleep = 30
    throughputExceededRepeat=10
    def __init__(self, uristring, collection, throughputSleep=30, throughputExceededRepeat=10):
        super().__init__(uristring)
        logging.info("DynamoDB init")
        self.ddb = None
        self.throughputSleep = throughputSleep
        self.throughputExceededRepeat = throughputExceededRepeat
    def db_close(self):
        return 0
    
    def db_connect(self):
        
        ENV_FILE = find_dotenv()
        if ENV_FILE:
            load_dotenv(ENV_FILE)
            
        logging.info(f"connecting to dynamodb {self.uristring}")
        # client = boto3.client('dynamodb')
        # ddb = boto3.client('dynamodb', endpoint_url='http://172.31.144.1:8000',
        #                     aws_access_key_id="anything",
        #                     aws_secret_access_key="anything",
        #                     region_name="us-west-2")
        
        akey = env.get('access_key_id')
        skey = env.get('secret_access_key')
        
        ddb = boto3.resource('dynamodb', endpoint_url=self.uristring,#dynamodb.us-east-2.amazonaws.com:443
                             aws_access_key_id=akey,
                             aws_secret_access_key=skey,
                             region_name="us-east-2", )
        
        self.ddb = ddb

        #self.CheckAllTables()

    def db_find_metadata_by_startTime(self, cname, key):
        key = json.loads(json.dumps(key, indent=4, sort_keys=True, default=str), parse_float=Decimal)
        # key = time.mktime(key.timetuple())
        filter_to_find = Attr('time').eq(key)
        return self.__db_find_metadata(cname, filter_to_find)

    def db_find_metadata_by_id(self, cname, key):
        filter_to_find = Attr('_id').eq(key)
        return self.__db_find_metadata(cname, filter_to_find)
    
    def __db_find_metadata(self, cname, filter_to_find):
        ttable = self.ddb.Table(cname)
        try:
            result = ttable.scan(FilterExpression=filter_to_find)
            if result['Count'] == 0:
                return None
            return result['Items'][0]['_id']
            # mongo only gives ID because its not scanning
            # change from scan to query someday
        except TypeError:
            logging.info("cannot find item")
            return None
        # result = self.mydb[cname].find_one(sdata)
        # if (result != None):
        #    return result["_id"]

    def db_getBatchWriter(self):
        self.bwriter = self.ddb.Table(self.cname).batch_writer()
        return self.bwriter
    
    def db_putItemBatch(self, newdata):
        checkdata = DatabaseDynamo._prepDataForInsert(self.cname, newdata)
        result = None
        tries = 0
        while True:
            try:
                tries = tries + 1
                result = self.bwriter.put_item(checkdata)
                return result
            #except ClientError as err:
            except botocore.exceptions.ClientError as err:
                #botocore.errorfactory.ProvisionedThroughputExceededException as err:
                if 'ProvisionedThroughputExceededException' not in err.response['Error']['Code']:
                    raise
                logging.info(f"throughput exceeded, sleeping for {self.throughputSleep} seconds")
                time.sleep(self.throughputSleep)
            if(tries >= self.throughputExceededRepeat):
                logging.info("too many attempts")
                raise TimeoutError("Too many attempts")
        
        #return result
    
    
    def db_insert(self, collection, newdata):
        return self.db_single_insert(collection, newdata)

    def db_insert_main(self, newdata):
        return self.db_insert(self.cname, newdata)
    
    @staticmethod
    def _prepDataForInsert(collection_name, newdata):
        #if(collection_name == 'metadata'):
        #    newdata['time'] = newdata['startTime']#time.mktime(newdata['startTime'].timetuple())
        #else:   
            #if(isinstance(newdata['timeField'],float)==False):
            #    tf = newdata['timeField'].timetuple()
            #    newdata['timeField'] = time.mktime(tf)
            
        newdata = json.loads(json.dumps(newdata, indent=4, sort_keys=True, default=str), parse_float=Decimal)

        # new data is already in json, but needs dynamo format
        # also we need to generate a unique ID
        newUUID = str(generate_unique_id())
        #print(newUUID)
        checkdata = {'_id': newUUID}
        checkdata.update(newdata)
        return checkdata
    
    def db_single_insert(self, collection_name, newdata):
        ttable = self.ddb.Table(collection_name)
        # dynamo does not support float only decimal, watch out for datetime
        checkdata = DatabaseDynamo._prepDataForInsert(collection_name, newdata)
        try:
            ttable.put_item(Item=checkdata)
            return checkdata['_id']
        except ClientError as ce:
            logging.info(f"Fail on table {collection_name}")
            logging.info(newdata)
            logging.info(f"\nclient error on insert {ce}")

            sys.exit()
        except TypeError as e:
            logging.info(f"\ntype error on insert {e}")
            #sys.exit()

    def CheckAllTables(self):
        tables = list(self.ddb.tables.all())
        logging.info(tables)
        logging.info(f"Checking for table{self.metatablename}")
        result = self.checkTableExistsCreateIfNot(self.metatablename)
        if result == -1:
            logging.info("Table check/create issue 1")
            sys.exit()
        logging.info(f"Checking for table{self.cname}")
        result = self.checkTableExistsCreateIfNot(self.cname)
        if result == -1:
            logging.info("Table check/create issue 2")
            sys.exit()
        return 0  
     
    def checkTableExistsCreateIfNot(self, tname):
        ddb = self.ddb
        # dynamo only has tables, not dbs+collections, so the collection is table here
        ttable = self.ddb.Table(tname)
        logging.info(f"Looking for table {tname}")

        #timeField = 'timeField'
        # if (tname == 'metadata'):
        #     timeField = 'startTime'

        #is_table_existing = False
        createTable = False
        try:
            is_table_existing = ttable.table_status in ("CREATING", "UPDATING",
                                                        "DELETING", "ACTIVE")
            logging.info(f"table {tname} already exists, no need to create")
            return 1
        except ClientError:
            logging.info(f"Missing table {tname}")
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
                                                  'AttributeName': 'time',
                                                  'KeyType': 'RANGE'
                                              },
                                             
                                          ],
                                          AttributeDefinitions=[
                                              {
                                                  'AttributeName': '_id',
                                                  'AttributeType': 'S'
                                              },
                                              {
                                                  'AttributeName': 'time',
                                                  'AttributeType': 'N'
                                              },
                                              {
                                                  'AttributeName': 'groupMetadataID',
                                                  'AttributeType': 'S'
                                              },
                                              {
                                                  'AttributeName': 'topic',
                                                  'AttributeType': 'S'
                                              }
                                          ],
                                          GlobalSecondaryIndexes=[
                                            {
                                                'IndexName': 'DataIndex',
                                                'KeySchema': [
                                                    {
                                                       'AttributeName': 'groupMetadataID',
                                                       'KeyType': 'HASH'
                                                    },
                                                ],
                                                'Projection': {
                                                    'ProjectionType': 'ALL',
                                                },
                                                'ProvisionedThroughput': {
                                                    'ReadCapacityUnits': 5,
                                                    'WriteCapacityUnits': 5,
                                                }
                                            },
                                            {
                                                'IndexName': 'TopicIndex',
                                                'KeySchema': [
                                                    {
                                                       'AttributeName': 'topic',
                                                       'KeyType': 'HASH'
                                                    }
                                                ],
                                                'Projection': {
                                                    'ProjectionType': 'ALL',
                                                },
                                                'ProvisionedThroughput': {
                                                    'ReadCapacityUnits': 5,
                                                    'WriteCapacityUnits': 5,
                                                }
                                            }
                                        ],
                                        ProvisionedThroughput={'ReadCapacityUnits': 5, 'WriteCapacityUnits': 5}
                                        )
                logging.info("Waiting for table creation")
                response = ttable.wait_until_exists()
                return 1
            except ClientError as e:
                logging.info(e.response)
                logging.info("failed to create table")
                return -1
        return 0


