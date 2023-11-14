import boto3
from dotenv import find_dotenv, load_dotenv
from os import environ as env

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


from boto3.dynamodb.conditions import Key, Attr
response = metatable.scan(
    FilterExpression=Attr('experimentID').eq(0)
)
items = response['Items']
#print(items)
metadataID = items[12]['_id']
print(metadataID)
#common tie between metadata and cyberdata is metadataID
response = cybertable.scan(
    FilterExpression=Attr('topic').eq('/apollo/sensor/gnss/best_pose') & Key('metadataID').eq(metadataID)
)
items = response['Items']
print(items[0])

#