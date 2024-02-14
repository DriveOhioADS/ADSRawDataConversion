import json
import sys
import cyberreaderlib as cyberreader
from google.protobuf.json_format import MessageToJson
#import sensor_msgs.point_cloud2 as pc2
import zlib
import lzma
import bz2
import numpy as np
import base64
import glob 
import os 
import jsonlines
import hashlib

def ReadLiDARFile(filename):
    lidarfile = open(filename,'r')
    JLreader = jsonlines.Reader(lidarfile)
    count = 0
    header = None
    jline = JLreader.read()
    while(jline is not None):
        #print(jline)
        if(count == 0):
            header = jline
        else:
            lidarb64ascii = jline['p']
            rawb64bytes = base64.b64decode(lidarb64ascii)
            rawdata = lzma.decompress(rawb64bytes)
            xyzlum = np.frombuffer(rawdata)
            xyzlum = xyzlum.reshape((-1,5))
            #data is ready here in xyzlum as np array n by 5
            print(xyzlum)
        

        count+=1
        jline = JLreader.read()


    JLreader.close()
    lidarfile.close()
    
def ProcessPoseMsg(msg):
    return
    # position = msg.pose.position
    # newdf = pd.DataFrame([[msg.measurement_time, position.x, position.y, position.z]],columns=['t','X','Y','Z'])
    # #data_dict = {}
    # #for field in msg.ListFields():
    # #    data_dict[field[0].name] = [field[1]]
    # #newdf = pd.DataFrame.from_dict(data_dict)
    # df = pd.concat([df, newdf])
    # #jsdata = MessageToJson(msg)
    # #df = pd.read_json(jsdata)
    # # if(os.path.isfile(xyzname)):
    # #     HeaderQ = False
    # # else:
    # #     HeaderQ = True
    # # df.to_csv(xyzname, mode='a', index=False, header=HeaderQ)
    # # HeaderQ=False
def ProcessBestPose(msg):
    return
    # print(count)
    # count = count + 1
    # data_dict = {}
    # for field in msg.ListFields():
    #     data_dict[field[0].name] = [field[1]]
    # newdf = pd.DataFrame.from_dict(data_dict)
    # #for col in newdf.columns:
    # #    print(col)
    # #print(newdf)
    # gpsdf = pd.concat([gpsdf, newdf])
    # #df = pd.DataFrame([[msg.measurement_time, position.x, position.y, position.z]],columns=['t','X','Y','Z'])
    # #jsdata = MessageToJson(msg)                          
    # #df = pd.read_json(jsdata)
    # #df.to_csv('gpsdata.csv', mode='a', index=False, header=HeaderQ)
    # #HeaderQ=False
    
def ProcessLidarMsg(msg, jsdata, channel_name, JLwriter):   
    #remove the text from this dictionary
    lidarlist = np.empty([len(jsdata['point']), 5])
    idx = 0
    for xyzpoint in jsdata['point']:
        newline = np.array((xyzpoint['x'],xyzpoint['y'],xyzpoint['z'],xyzpoint['intensity'],xyzpoint['timestamp']))
        lidarlist[idx,:] = newline
        idx = idx + 1
    lidarbytes = lidarlist.tobytes() 

    compressor = lzma.LZMACompressor()
    compressed_data = compressor.compress(lidarbytes)
    compressed_data += compressor.flush()
    print(f"Raw {len(lidarbytes)} Compressed: {len(compressed_data)}")
    # gives binary base64
    b64data = base64.b64encode(compressed_data)  
    # ascii base64 for json storage
    b64data = b64data.decode('ascii')    
    del jsdata['point']
    del jsdata['isDense']
    del jsdata['width']
    del jsdata['height']
    jsdata['ht'] = jsdata['header']['timestampSec']
    del jsdata['header']
    jsdata['mt'] = jsdata['measurementTime']
    del jsdata['measurementTime']
    jsdata['p'] = b64data
    jsdata['c'] = channel_name
    jsdata['t'] = msg.measurement_time
    jsdata['lt'] = msg.header.lidar_timestamp
    jsdata['s'] = msg.header.sequence_num
    JLwriter.write(jsdata)

#filename = "../20230421153705.record.00000"

def ProcessFile(file_path,root_dir='lidar',file_count=0):
    if(not os.path.isdir(root_dir)):
        os.mkdir(root_dir)
    filename = file_path
    justfilename = os.path.basename(filename)
    jsonfilename = justfilename.split(".")[0] #removes the 0000x extension
    jsonfilename = os.path.join(root_dir,jsonfilename+f".{file_count:05}.jslines")
    print(f"Making json file at: {jsonfilename}")
    lidarfile = open(jsonfilename,'w')
    JLwriter = jsonlines.Writer(lidarfile)
    JLwriter.write({
                "file": justfilename,
                "compression": "lzma",
                "encoding": "base64",
                "legend":{"mt":"measurementTime","ht":"header_timestamp","p":"encoded_points","c":"channel_name","t":"msg_time","lt":"lidar_timestamp","s":"seq_num"}
                }
    )
    pbfactory = cyberreader.ProtobufFactory()
    reader = cyberreader.RecordReader(filename)
    for channel in reader.GetChannelList():
        desc = reader.GetProtoDesc(channel)
        pbfactory.RegisterMessage(desc)
        #print(channel)

    message = cyberreader.RecordMessage()
    # num_msg = reader.header.message_number
    # msgcount = 0
    while reader.ReadMessage(message):
        message_type = reader.GetMessageType(message.channel_name)
        msg = pbfactory.GenerateMessageByType(message_type)
        msg.ParseFromString(message.content)
        #print(message.channel_name)
        if(message.channel_name == '/apollo/localization/pose'):
            ProcessPoseMsg(msg)
        elif(message.channel_name == '/apollo/sensor/gnss/best_pose'):
            ProcessBestPose(msg)
        #check the LiDAR data process
        elif(message.channel_name == '/apollo/sensor/velodyne32/PointCloud2' or
            message.channel_name == '/apollo/sensor/velodyne/fusion/PointCloud2'):
            jsdata = json.loads(MessageToJson(msg))  
            ProcessLidarMsg(msg, jsdata, message.channel_name, JLwriter)
    
    JLwriter.close()  
    lidarfile.flush()
    lidarfile.close()
    return jsonfilename   

def AddFileHash(fname):
    hash_md5 = hashlib.md5()
    with open(fname, "rb") as f:
        for chunk in iter(lambda: f.read(8192), b""):
            hash_md5.update(chunk)

    lidarfile = open(fname,'a')
    JLwriter = jsonlines.Writer(lidarfile)
    JLwriter.write({"md5":hash_md5.hexdigest()})
    JLwriter.close()  
    lidarfile.flush()
    lidarfile.close()  
    return hash_md5.hexdigest()
    
if __name__ == '__main__':    
    
    #folder_path = 's3bucket/Deployment_2_SEOhio/Blue\ Route/OU\ Pacifica/1696265320'
    # folder_path = ".."
    # xyzname = 'data.csv'
    # df = pd.DataFrame(columns=['t','X','Y','Z'])
    # gpsdf = pd.DataFrame()
    # search = '20230421160347'
    # search = '20230424103830'
    # search = '20230424104410'
    # search = '20230424104814'


    # 2 lines for adding
    folder_path = '/volumes/user/home/jay'
    #folder_path = '/s3bucket/Deployment_2_SEOhio/Blue Route/OU Pacifica/1692815820/'
    #search = '*.record.*'
    # 2 lines for reading
    search = '*.jslines'
    # folder_path += "lidar" #add to read the lidar files

    file_list = glob.glob(os.path.join(folder_path, search)) #+ glob.glob(os.path.join(folder_path, search))
    file_list.sort()
    print(file_list)
    count = 0
    numfiles = len(file_list)
    for file_path in file_list:
        print(f"{file_path} -> {count+1}/{numfiles}")
        ReadLiDARFile(file_path)   
        #jname = ProcessFile(file_path,root_dir=os.path.join(folder_path,"lidar"),file_count=count)
        #AddFileHash(jname)
        count = count + 1


# df.to_csv(search+xyzname)#,  index=False, header=True) #mode='a',
# gpsdf.to_csv(search+'gps.csv')


# notes:
# /apollo/sensor/velodyne32/PointCloud2
# /apollo/sensor/velodyne16/front/left/PointCloud2
# /apollo/sensor/velodyne16/front/right/PointCloud2
# /apollo/sensor/velodyne/fusion/PointCloud2
# /apollo/sensor/velodyne/fusion/PointCloud2Status

# 1599 MB raw (points direct from the message xyzlum)
# 336 MB zip of the raw
# 368 MB zip of LZMA+B64 lines of the points
# 272 MB zip of LZMA+B64

# 1 Message size -> Raw Points: 1245080 LZMA Compressed: 524732

# # check output
# rawbytes = base64.b64decode(b64data)
# decompress = zlib.decompress(rawbytes)
# check = np.frombuffer(decompress)
# check = check.reshape((-1,5))
# print(np.array_equal(check, lidarlist))  