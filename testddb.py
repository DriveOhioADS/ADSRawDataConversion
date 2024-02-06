import boto3
import botocore.exceptions
from boto3.dynamodb.conditions import Key, Attr

from dotenv import find_dotenv, load_dotenv
#from os import environ as env
from boto3.dynamodb.conditions import Key, Attr

import sys
from halo import Halo

#ENV_FILE = find_dotenv()
##if ENV_FILE:
#    load_dotenv(ENV_FILE)
            
#akey = env.get('access_key_id')
#skey = env.get('secret_access_key')
#EPURL = "http://localhost:8000"
EPURL = "https://dynamodb.us-east-2.amazonaws.com:443"
dynamodb = boto3.resource('dynamodb', endpoint_url=EPURL,
                        #aws_access_key_id=akey,
                        #aws_secret_access_key=skey,
                        region_name="us-east-2", )
metatable = dynamodb.Table('ads_passenger_processed_metadata')
cybertable = dynamodb.Table('ads_passenger_processed')

#print(metatable.creation_date_time)
print("Table status: ", metatable.table_status)

#response = table.scan()
#print(response)


def GrabGPSDataSet(groupID,topicName):
    return GrabCyberDataByTopic(groupID,'/apollo/sensor/gnss/best_pose')

def GrabCyberDataByTopic(groupID,topicName):
    spinner = Halo(text='Performing query', text_color= 'cyan', color='green', spinner='dots')
    spinner.start()
    scan_kwargs = {
                'IndexName': 'groupMetadataID-index',
                'KeyConditionExpression': Key('groupMetadataID').eq(groupID)
                #"FilterExpression": FilterExpression,
                #"ProjectionExpression": "#_id, #time, msgtime, dataid, filename, groupMetadataID, size, msgnum, foldername, vehicleID, experimentID",
                #"ExpressionAttributeNames": {":time":{"N":"1696870306072587273"}}#{ "#_id": "_id" , "#time": "time"},
            }
    items_scanned = 0
    
    filteredItems = []
    try:
        done = False
        start_key = None
        while not done:
            items = []
            if start_key:
                scan_kwargs["ExclusiveStartKey"] = start_key
            response = cybertable.query(**scan_kwargs)
            items_scanned = items_scanned + response['ScannedCount']
            #item_count = response['Count']
            items.extend(response.get("Items", []))
            start_key = response.get("LastEvaluatedKey", None)
            #print(f"{start_key} / {items_scanned} - {len(items)} + {item_count}")
            done = start_key is None
            for newitem in items:
                if(topicName == newitem['topic']):
                #if('pose' in newitem['topic'] and newitem['topic'] != '/apollo/localization/pose'):
                    filteredItems.append(newitem)
            spinner.text = 'Items found via query -> {}'.format(len(filteredItems))
    except botocore.exceptions.ClientError as err:
        print(f"Couldn't scan for item. Here's why: {err.response['Error']['Code']} -> {err.response['Error']['Message']}")
    #print(items_scanned)   
    spinner.stop()
    return filteredItems
    
def GrabMetaDataByGroupID(gid):
    items = []
    items_scanned = 0
    item_count = 0
    scan_kwargs = {
                'IndexName': 'groupMetadataID-index',
                'KeyConditionExpression': Key('groupMetadataID').eq(gid),
                #"FilterExpression": FilterExpression,
                "ProjectionExpression": "#_id, #time, msgtime, dataid, filename, groupMetadataID, size, msgnum, foldername, vehicleID, experimentID",
                "ExpressionAttributeNames": { "#_id": "_id" , "#time": "time"},
            }
    try:
        done = False
        start_key = None
        while not done:
            if start_key:
                scan_kwargs["ExclusiveStartKey"] = start_key
            response = metatable.query(**scan_kwargs)
            items_scanned = items_scanned + response['ScannedCount']
            item_count = item_count = response['Count']
            items.extend(response.get("Items", []))
            start_key = response.get("LastEvaluatedKey", None)
            print(f"{start_key} / {items_scanned} - {len(items)} + {item_count}")
            done = start_key is None
    except botocore.exceptions.ClientError as err:
        print(f"Couldn't query for item. Here's why: {err.response['Error']['Code']} -> {err.response['Error']['Message']}")
    return items

def GrabMetaData():
    items = []
    items_scanned = 0
    item_count = 0
    #FilterExpression=Key('_id').gt(-1)
    FilterExpression=Attr('time').gt(0)#eq(1696870306072587273)

    scan_kwargs = {
                'KeyConditionExpression': FilterExpression,
                #"FilterExpression": FilterExpression,
                #"ProjectionExpression": "#_id, #time, filename, groupMetadataID, size, msgnum, foldername, vehicleID, experimentID",
                #"ExpressionAttributeNames": { "#_id": "_id" , "#time": "time"},
            }
    # scan_kwargs = {
    #             'IndexName': 'time-index',
    #             'KeyConditionExpression': Key('time').eq(0),
    #             #"ProjectionExpression": "#_id, #time, msgtime, dataid, filename, groupMetadataID, size, msgnum, foldername, vehicleID, experimentID",
    #             #"ExpressionAttributeNames": {":time":{"N":"1696870306072587273"}}#{ "#_id": "_id" , "#time": "time"},
    # }
    # scan_kwargs = {
    #             'IndexName': 'groupMetadataID-index',
    #             'KeyConditionExpression': Key('groupMetadataID').eq('da853e0c-a10f-11ee-981c-d126ddbe9afa')
    #             #"FilterExpression": FilterExpression,
    #             #"ProjectionExpression": "#_id, #time, msgtime, dataid, filename, groupMetadataID, size, msgnum, foldername, vehicleID, experimentID",
    #             #"ExpressionAttributeNames": {":time":{"N":"1696870306072587273"}}#{ "#_id": "_id" , "#time": "time"},
    #         }
    try:
        done = False
        start_key = None
        while not done:
            if start_key:
                scan_kwargs["ExclusiveStartKey"] = start_key
            #response = metatable.get_item(Key={'time':{'N':1696870306072587273}})
            response = metatable.query(**scan_kwargs)
            #response = metatable.scan(**scan_kwargs)
            items_scanned = items_scanned + response['ScannedCount']
            item_count = item_count = response['Count']
            items.extend(response.get("Items", []))
            start_key = response.get("LastEvaluatedKey", None)
            #print(f"{start_key} / {items_scanned} - {len(items)} + {item_count}")
            done = start_key is None
    except botocore.exceptions.ClientError as err:
        print(f"Couldn't scan for item. Here's why: {err.response['Error']['Code']} -> {err.response['Error']['Message']}")
    return items

groupids=[
'3a116996-93a9-11ee-956e-9da2d070324c',
'ba6e1072-9524-11ee-956e-9da2d070324c',
'2837eb9c-9542-11ee-956e-9da2d070324c',
'da853e0c-a10f-11ee-981c-d126ddbe9afa',
'154fab12-a43f-11ee-88ec-eb6a8d5269b4',
'f6ac3c82-a445-11ee-88ec-eb6a8d5269b4',
'58263e34-a45c-11ee-88ec-eb6a8d5269b4',
'c335d84c-a45c-11ee-88ec-eb6a8d5269b4',
'5976b77a-a504-11ee-88ec-eb6a8d5269b4',
'2bc6ebb8-a529-11ee-88ec-eb6a8d5269b4',
'7f09f6c6-a5b0-11ee-88ec-eb6a8d5269b4',
'f671c05c-a5e4-11ee-88ec-eb6a8d5269b4',
'90101c36-a621-11ee-88ec-eb6a8d5269b4']
# datain = []
# for groupid in groupids:
#     print(f"looking for {groupid}")
#     data = GrabMetaDataByGroupID(groupid)
#     datain.append(data)

# for data in datain:
#     print(f"groupid -> {len(data)} @ {data[0]['foldername']}")
#items = GrabMetaData()

items = GrabGPSDataSet(groupids[0],'')

count = 0
for item in items:#response['Items']:
    if(True):
    #if('1684733733' in item['filename']):
    #if('1674155613' in item['filename']):
    #if('20230522131549' in item['filename']):
    #if(item['time'] == 1684776202798360726):
        print(f"{count}: {item['_id']},{item['time']},{item['topic']},{item['latitude']},{item['longitude']},{item['heightMSL']}")

        #print(f"{count}: {item['_id']},{item['time']},{item['vehicleID']},{item['experimentID']},{item['foldername']},{item['filename']},{item['groupMetadataID']}") #{item['groupID']}
        count = count + 1
   
    #print(f"{count} {item['foldername']} {item['_id']}")
    #if('groupID' in item):
    #    print(item['groupID'])
    
    # if('jay' in item['foldername']):
    #     item['foldername'] = item['foldername'].replace("/home/jay/s3bucket/","") + "/"
    #     #newfilename = item['filename'].replace("/home/jay/s3bucket/","")
    #     #newfilename = item['filename'].replace("/home/jay/","")
    #     print(f"updating: {item['foldername']}")
    #     udpexp = "set foldername=:f"
    #     expattrv = {":f":item['foldername']}
    #     response = metatable.update_item(Key={"_id": item['_id'], "time": item['time']},
    #                         UpdateExpression=udpexp,
    #                         ExpressionAttributeValues=expattrv,
    #                         ReturnValues="UPDATED_NEW")
    #     print(response)
  

    