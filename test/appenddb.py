import ast, json
import os
import sys
import glob

dblines = {"database":{
                "type": "dynamo",
                "uri": "https://dynamodb.us-east-2.amazonaws.com:443",
                "collection": "ads_passenger_processed",
                "databasename": "ads_passenger_processed",
                "metatablename": "ads_passenger_processed_metadata",
                "batch": True,
                "throughputSleep": 20   
            }}
channelList = {"channelList":{
        "deny": [
            "/apollo/sensor/camera/front_6mm/image",
            "/apollo/sensor/camera/front_6mm/image/compressed",
            "/apollo/sensor/camera/front_25mm/image",
            "/apollo/sensor/camera/front_25mm/image/compressed",
            "/apollo/sensor/velodyne32/PointCloud2",
            "/apollo/sensor/velodyne32/VelodyneScan",
            "/apollo/planning",
            "/apollo/prediction",
            "/apollo/perception",
            "/apollo/perception/obstacles"
        ]
        }}
def addtojson(fname, fix=False):
    fd = open(fname,'r')
    rawfilestr = fd.read()
    fd.close()
    dirty=False

    if('"batch": true,' in rawfilestr):
        rawfilestr=rawfilestr.replace('"batch": true,','"batch": True,')
        #dirty=True #no need to just write this part
    jsondata = ast.literal_eval(rawfilestr)
    if(isinstance(jsondata['metadata']['driver'], int)):
        jsondata['metadata']['driver']=str(jsondata['metadata']['driver'])
        dirty=True
    if(isinstance(jsondata['metadata']['experimentID'], int)):
        jsondata['metadata']['experimentID']=str(jsondata['metadata']['experimentID'])
        dirty=True
    if('Red Route' in jsondata['file']['folder']):
        jsondata['file']['folder']=jsondata['file']['folder'].replace('Red Route', 'RedRoute')
        dirty = True
    if(not 'database' in jsondata):
        print(f"missing db line")
        jsondata.update(dblines)
        dirty=True
    elif('uri' in jsondata['database']):
        if('localhost' in jsondata['database']['uri']):
            jsondata['database'] = dblines['database']
            dirty=True
    if(not 'channelList' in jsondata):
        print(f"missing channel line")
        jsondata.update(channelList)
        dirty=True
    
    #print(jsondata)
    if(dirty):
        print("UPDATING")
        fd = open(fname,'w')
        json.dump(jsondata,fd,ensure_ascii=False, indent=4)
        fd.flush()
        fd.close()

root = "/home/jay/s3bucket/Deployment_2_SEOhio/Blue Route/OU Pacifica/*"
root = "/home/jay/s3bucket/Deployment_2_SEOhio/RedRoute/OU Pacifica/*"

filelist = sorted(glob.glob(root))
#print(filelist)

scriptlist = []
for fname in filelist:
    morefiles = sorted(glob.glob(fname+"/*"))
    #print(morefiles)
    for mfname in morefiles:
        if(mfname.endswith(".json") and 'VanDrive_Comments' not in mfname):
            print(mfname)
            addtojson(mfname,fix=True)
            scriptlist.append(mfname)

for jname in scriptlist:
    print(f"python3 datainsert.py --config '{jname}' --rootdir /home/jay/s3bucket")