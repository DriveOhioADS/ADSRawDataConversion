import pandas as pd
import json
import sys
import cyberreaderlib as cyberreader
from google.protobuf.json_format import MessageToJson
import sensor_msgs.point_cloud2 as pc2
import zlib
import numpy as np
import base64

filename = "../delete/20221117125313.record.00000"

pbfactory = cyberreader.ProtobufFactory()
reader = cyberreader.RecordReader(filename)
for channel in reader.GetChannelList():
    desc = reader.GetProtoDesc(channel)
    pbfactory.RegisterMessage(desc)
                
message = cyberreader.RecordMessage()
num_msg = reader.header.message_number
msgcount = 0
while reader.ReadMessage(message):
    message_type = reader.GetMessageType(message.channel_name)
    if(message.channel_name == '/apollo/localization/pose'):
        msg = pbfactory.GenerateMessageByType(message_type)
        msg.ParseFromString(message.content)
        jsdata = MessageToJson(msg)                          
        df = pd.read_json(jsdata)
        df.to_csv('data.csv', mode='a', index=False, header=False)
    
    #check the LiDAR data process
    # if(message.channel_name == '/apollo/sensor/velodyne32/PointCloud2'):
    #     msg = pbfactory.GenerateMessageByType(message_type)
    #     msg.ParseFromString(message.content)
    #     jsdata = json.loads(MessageToJson(msg))  
    #     #remove the text from this dictionary
    #     lidarlist = np.empty([len(jsdata['point']), 5])
    #     idx = 0
    #     for xyzpoint in jsdata['point']:
    #         newline = np.array((xyzpoint['x'],xyzpoint['y'],xyzpoint['z'],xyzpoint['intensity'],xyzpoint['timestamp']))
    #         lidarlist[idx,:] = newline
    #         idx = idx + 1
    #     lidarbytes = lidarlist.tobytes()
    #     compressed_data = zlib.compress(lidarbytes)
    #     print(f"Raw {len(lidarbytes)} Compressed: {len(compressed_data)}")
    #     b64data = base64.b64encode(compressed_data)
        
    #     jsdata['point'] = b64data
    #     # check output
    #     rawbytes = base64.b64decode(b64data)
    #     decompress = zlib.decompress(rawbytes)
    #     check = np.frombuffer(decompress)
    #     check = check.reshape((-1,5))
    #     print(np.array_equal(check, lidarlist))                
    