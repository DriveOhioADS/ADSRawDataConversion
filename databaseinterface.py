import sys
import json
#from bson import json_util
#from rospy_message_converter import message_converter
#from datetime import datetime
#import sensor_msgs.point_cloud2 as pc2
#import numpy as np
import pymongo

import uuid
import boto3
from boto3.dynamodb.conditions import Key, Attr
from botocore.exceptions import ClientError
from decimal import Decimal
#from google.protobuf.json_format import MessageToJson
import logging
import os

class DatabaseInterface:
    def __init__(self, uristring):
        self.uristring = uristring
        self.cname = None
        self.type = ''

    def check(self):
        print("class check")

    def db_connect(self):
        print("Connecting")

    def db_insert(self, collection_name, newdata):
        print("Inserting data")

    def setCollectionName(self, cname):
        self.cname = cname

    def set_bucket(self, s3bucket):
        self.bucket = s3bucket

    def CreateDatabaseInterface(type, uri, dbname):
        if(type == 'mongo'):
            obj = DatabaseMongo(uri, dbname)
        elif(type == 'dynamo'):
            obj = DatabaseDynamo(uri, dbname)
        obj.type = type
        return obj

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
        try:
            result = self.mydb[collection_name].insert_one(newdata)
            return result
        except pymongo.errors.OperationFailure:
            print("\ndb_insert OperationFailure")
            return -1
        except pymongo.errors.DocumentTooLarge:
            print("\ndb_insert DocumentTooLarge")
            return -1
        except Exception as ex:
            logging.error("\ndb_insert Exception")
            logging.error(newdata)
            template = "An exception of type {0} occurred. Arguments:\n{1!r}"
            message = template.format(type(ex).__name__, ex.args)
            logging.error(message)
            sys.exit(-1)
            return -1

    def db_connect(self):
        mclient = pymongo.MongoClient("mongodb://localhost:27017/")
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
        return self.__db_find_metadata(cname, {'startTime': {'$eq':key}})

    def db_find_metadata_by_id(self, cname, key):
        return self.__db_find_metadata(cname, {'_id': {'$eq':key}})

    def __db_find_metadata(self, cname, filter):
        #key = sdata['startTime']
        result = self.mydb[cname].find_one(filter)
        if result != None:
            return result["_id"]
        return None

    def db_export(self, config):
        with open('cred.json','rb') as f:
            cred = json.load(f)
        target = config['file']['folder'][1:]
        target = target[:-1]
        with open("cybersettings.json",'rb') as f:
            config = json.load(f)
        region = config["region"]
        client = boto3.client('s3',aws_access_key_id=cred['ACCESS_ID'],
                                    aws_secret_access_key=cred['ACCESS_KEY'],
                                    region_name=region)
        # command = "mongoexport --db "+config['database']['databasename']+" --collection "+config['database']['collection']+" --out="+target+".json"
        # os.system(command)
        # folder = config['file']['folder'][1:]
        
        os.system('mkdir results')
        
        command = "mongoexport --db "+config['database']['databasename']+" --collection "+config['database']['collection']+" --out="+config["file"]["filebase"]+".json"
        os.system(command)

        command = "cp "+config["file"]["filebase"]+".json results/"+config["file"]["filebase"]+".json"
        os.system(command)

        command = "zip -r "+config["file"]["filebase"]+"zip results"
        os.system(command)
        
        folder = config['file']['folder'][1:]
        client.upload_file(config["file"]["filebase"]+'zip',self.bucket, folder+config["file"]["filebase"]+'zip')
    
    # def insert_metadata(self, metadata):
    #     result = self.mydb[self.dname]["metadata"].insert_one(metadata)
    #     if result != None:
    #         return result.inserted_id
    #     return None


def generate_unique_id():
    return uuid.uuid1()


class DatabaseDynamo(DatabaseInterface):
    def __init__(self, uristring, collection):
        super().__init__(uristring)
        print("DynamoDB init")
        self.ddb = None

    def db_connect(self):
        print(f"connecting to dynamodb {self.uristring}")
        # client = boto3.client('dynamodb')
        # ddb = boto3.client('dynamodb', endpoint_url='http://172.31.144.1:8000',
        #                     aws_access_key_id="anything",
        #                     aws_secret_access_key="anything",
        #                     region_name="us-west-2")
        
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

    def db_find_metadata_by_startTime(self, cname, key):
        filter_to_find = Attr('startTime').eq(key)
        return self.__db_find_metadata(cname, filter_to_find)

    def db_find_metadata_by_id(self, cname, key):
        filter_to_find = Attr('_id').eq(key)
        return self.__db_find_metadata(cname, filter_to_find)

    def __db_find_metadata(self, cname, filter_to_find):
        sdata = json.loads(json.dumps(sdata), parse_float=Decimal)
        #item_to_find = sdata['startTime']

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

        #is_table_existing = False
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
