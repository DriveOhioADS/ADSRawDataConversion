# ADSRosbagToMongoDB
This repo contains scripts to convert ROSBAG data from autonomous vehicles to MongoDB.
Not included currently: camera and LiDAR feeds

# Usage
``` python3 datainsert.py --mongodb http://localhost:27017 --rosbagb rosbag.bag --metadatafile meta.json --collection rosbag ```  


--dynamodb Dynamo URI string
OR
--mongodb Mongo URI string

--cyberfolder location of folder with cyber data
--cyber filebase filename without the .00000  
OR
--rosbag rosbag file for processing  

--metadatafile json file with metadata
--collection name of database table or collection
--lidar include lidar data
--force insert even if metadata is present
--channellist list of deny and accept channels (both cyber and rosbag)

# Setup
```
sudo apt install -y python3-rosbag
sudo apt install -y python3-numpy python3-scipy python3-matplotlib
sudo pip3 install boto3
sudo pip3 install pymongo
sudo pip3 install rospy_message_converter
sudo pip3 install pyprog
sudo apt install -y python3-sensor-msgs
```

# Metadata added by the program
```
starttime  
endtime  
duration  
filename  
size  
msgnum  
```

# Forced insert
The script will block data insert if the metadata already exists

# Ignored topics
These ROS topics are either too large or cause insert errors:  
```
'sensor_msgs/CompressedImage',
'sensor_msgs/Image',
'sensor_msgs/PointCloud2',
'velodyne_msgs/VelodyneScan',
'theora_image_transport/Packet',
'sensor_msgs/LaserScan',
'autoware_lanelet2_msgs/MapBin',  # issues with insert
'visualization_msgs/MarkerArray',  # breaks insert
'autoware_msgs/DetectedObjectArray'
```