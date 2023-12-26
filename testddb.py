import boto3
import botocore.exceptions
from boto3.dynamodb.conditions import Key, Attr

from dotenv import find_dotenv, load_dotenv
from os import environ as env
from boto3.dynamodb.conditions import Key, Attr

import sys

ENV_FILE = find_dotenv()
if ENV_FILE:
    load_dotenv(ENV_FILE)
            
akey = env.get('access_key_id')
skey = env.get('secret_access_key')
EPURL = "http://localhost:8000"
#EPURL = "https://dynamodb.us-east-2.amazonaws.com:443"
dynamodb = boto3.resource('dynamodb', endpoint_url=EPURL,
                        aws_access_key_id=akey,
                        aws_secret_access_key=skey,
                        region_name="us-east-2", )
metatable = dynamodb.Table('ads_passenger_processed_metadata')
cybertable = dynamodb.Table('ads_passenger_processed')

#print(metatable.creation_date_time)

#response = table.scan()
#print(response)



def GrabCyberData(filterexp):
    response = metatable.scan(
        FilterExpression=Attr('experimentID').eq(13)
    )
    groupID = response['Items'][0]['groupID']
    print(groupID)

    # response = cybertable.scan(
    #     FilterExpression=Attr('topic').eq('/apollo/sensor/gnss/best_pose') & Key('groupMetadataID').eq(groupID)
    # )
    FilterExpression=Attr('topic').eq('/apollo/sensor/gnss/best_pose') & Key('groupMetadataID').eq(groupID)

    scan_kwargs = {
                "FilterExpression": FilterExpression,
                #"ProjectionExpression": "#yr, title, info.rating",
                #"ExpressionAttributeNames": {"#yr": "year"},
            }

    items = []
    items_scanned = 0
    item_count = 0
    try:
        done = False
        start_key = None
        while not done:
            if start_key:
                scan_kwargs["ExclusiveStartKey"] = start_key
            response = cybertable.scan(**scan_kwargs)
            items_scanned = items_scanned + response['ScannedCount']
            item_count = item_count = response['Count']
            #if(response['Count'] != 0):
            #    print(response['Items'][0])
            items.extend(response.get("Items", []))
            start_key = response.get("LastEvaluatedKey", None)
            print(f"{start_key} / {items_scanned} - {len(items)} + {item_count}")
            done = start_key is None
    except botocore.exceptions.ClientError as err:
        print(
                "Couldn't scan for movies. Here's why: %s: %s",
                err.response["Error"]["Code"],
                err.response["Error"]["Message"],
            )
    print(items_scanned)   
    #latitude
    #longitude
    
def GrabMetaData():
    items = []
    items_scanned = 0
    item_count = 0
    FilterExpression=Key('_id').gt(-1)
    #& Attr('time').gt(-1)
    FilterExpression=Attr('time').gt(0)

    scan_kwargs = {
                #'KeyConditionExpression': FilterExpression,
                "FilterExpression": FilterExpression,
                "ProjectionExpression": "#_id, #time, msgtime, dataid, filename, groupID, size, msgnum, foldername, vehicleID, insertDateTime, experimentID",
                "ExpressionAttributeNames": { "#_id": "_id" , "#time": "time"},

                #"ProjectionExpression": "#yr, title, info.rating",
                #"ExpressionAttributeNames": {"#yr": "year"},
            }
    try:
        done = False
        start_key = None
        while not done:
            if start_key:
                scan_kwargs["ExclusiveStartKey"] = start_key
            #response = metatable.query(KeyConditionExpression=Key('_id').eq('2aa5ca92-93ae-11ee-956e-9da2d070324c')
            #                           )#**scan_kwargs)
            response = metatable.scan(**scan_kwargs)
            items_scanned = items_scanned + response['ScannedCount']
            item_count = item_count = response['Count']
            #if(response['Count'] != 0):
            #    print(response['Items'][0])
            items.extend(response.get("Items", []))
            start_key = response.get("LastEvaluatedKey", None)
            print(f"{start_key} / {items_scanned} - {len(items)} + {item_count}")
            done = start_key is None
    except botocore.exceptions.ClientError as err:
        print(
                "Couldn't scan for movies. Here's why: %s: %s",
                err.response["Error"]["Code"],
                err.response["Error"]["Message"],
            )
    return items

# response = metatable.scan(
#         #FilterExpression=Attr('experimentID').eq(13)
#     )
ID_FIELD_NAME_EXP = "_id"
filter_to_find = Key(ID_FIELD_NAME_EXP).eq("8464b630-7da8-11ee-9f1c-2d3ffa529224")
result = metatable.query(KeyConditionExpression=filter_to_find,
                         #ExpressionAttributeNames= { "#_id": "_id" , "#time": "time"}
                        )
#aws dynamodb query --table-name ads_passenger_processed_metadata --key-condition-expression "#id = :_id" --expression-attribute-names '{"#id":"_id"}' --expression-attribute-values '{":_id":{"S":"8464b630-7da8-11ee-9f1c-2d3ffa529224"}}'
print(result)

items = GrabMetaData()
count = 0
for item in items:#response['Items']:
    if(True):
    #if('1684733733' in item['filename']):
    #if('1674155613' in item['filename']):
    #if('20230522131549' in item['filename']):
    #if(item['time'] == 1684776202798360726):
        print(f"{count}: {item['_id']},{item['time']},{item['vehicleID']},{item['experimentID']},{item['foldername']},{item['filename']}, {item['insertDateTime']}") #{item['groupID']}
        count = count + 1

    #if('groupID' in item):
    #    print(item['groupID'])
    
    # if('jay' in item['filename']):
    #     newfilename = item['filename'].replace("/home/jay/s3bucket/","")
    #     #newfilename = item['filename'].replace("/home/jay/","")
    #     print(f"updating: {newfilename}")
    #     udpexp = "set filename=:f"
    #     expattrv = {":f":newfilename}
    #     response = metatable.update_item(Key={"_id": item['_id'], "time": item['time']},
    #                         UpdateExpression=udpexp,
    #                         ExpressionAttributeValues=expattrv,
    #                         ReturnValues="UPDATED_NEW")
  

    