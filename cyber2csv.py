import pandas as pd
import json
import sys
import cyberreaderlib as cyberreader
from google.protobuf.json_format import MessageToJson

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
    
    