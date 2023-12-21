import boto3
import botocore.exceptions
from boto3.dynamodb.conditions import Key, Attr

from dotenv import find_dotenv, load_dotenv
from os import environ as env
from boto3.dynamodb.conditions import Key, Attr

ENV_FILE = find_dotenv()
if ENV_FILE:
    load_dotenv(ENV_FILE)
            
akey = env.get('access_key_id')
skey = env.get('secret_access_key')

dynamodb = boto3.resource('dynamodb', endpoint_url="https://dynamodb.us-east-2.amazonaws.com:443",
                        aws_access_key_id=akey,
                        aws_secret_access_key=skey,
                        region_name="us-east-2", )
metatable = dynamodb.Table('ads_passenger_processed_metadata')
cybertable = dynamodb.Table('ads_passenger_processed')

print(metatable.creation_date_time)

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
    
    
response = metatable.scan(
        #FilterExpression=Attr('experimentID').eq(13)
    )

for item in response['Items']:
    print(f"{item['time']},{item['vehicleID']},{item['experimentID']},{item['filename']}") #{item['groupID']}