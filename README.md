# ADSRosbagToMongoDB
This repo contains scripts to convert ROSBAG data from autonomous vehicles to MongoDB.
Not included currently: camera and LiDAR feeds

# Usage
``` python3 baginsert.py -d http://localhost:27017 -b rosbag.bag -v 1 -e 1 ```  

-d is the MongoDB URI  
-b is the rosbag file  
-v is the vehicle ID 	(metadata)  
-e is the experiment ID 	(metadata)  

Optional  
-c is the collection name for the data insert  
--force overrides the metadata block on insert  
# Metadata
```
vehicleID  
experimentnumber  
starttime  
endtime  
duration  
filename  
size  
msgnum  
```
Vehicle ID and Experiment ID are runtime inputs, the rest come from the rosbag file

# Forced insert
The script will block data insert if the metadata already exists

# Ignored topics
These topics are either too large or cause insert errors:  
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