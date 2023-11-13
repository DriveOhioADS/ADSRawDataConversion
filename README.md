# ADSRosbagToMongoDB
This repo contains scripts to convert ROSBAG and Cyber data from autonomous vehicles to MongoDB or dynamodb.
Not included currently: camera and LiDAR (upcoming with compression)

# Usage
``` python3 datainsert.py --config settings.json ```

Example settings file:

``` 
{
    "metadata":{
        "vehicleID": 8,
        "experimentID": 8,
        "other": 0
    },
    "file":
    {
        "type": "cyber",
        "folder": "cyberdata/",
        "filebase": "123456.record."
    },
    "database":
    {
        "type": "mongo",
        "uri": "mongodb://127.0.0.1:27017",
        "collection": "cyber",
        "databasename": "cyber"
    },
    "channelList":{
        "deny": [
            "/apollo/sensor/camera/front_6mm/image",
            "/apollo/sensor/camera/front_6mm/image/compressed",
            "/apollo/sensor/camera/front_25mm/image",
            "/apollo/sensor/camera/front_25mm/image/compressed",
            "/apollo/sensor/velodyne32/PointCloud2",
            "/apollo/sensor/velodyne32/VelodyneScan"
        ]
    }
 }
 ```
DynamoDB Example Config:
```
{
    "metadata":{
        "vehicleID": 0,
        "experimentID": 0,
        "other": "Test data"
    },
        "file":
    {
        "type": "cyber",
        "folder": "~/cdata",
        "filebase": "1234.record."
    },
    "database":
    {
        "type": "dynamo",
        "uri": "https://dynamodb.us-east-2.amazonaws.com:443",
        "collection": "ads_passenger_processed",
        "databasename": "ads_passenger_processed",
        "metatablename": "ads_passenger_processed_metadata",
        "batch": true,
        "throughputSleep": 20
    },
    "channelList":{
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
    }
 }

```
Database type of mongo or dynamo
File type of cyber or rosbag (only single file for rosbags)

Other switches:
```
--lidar include lidar data
--force insert even if metadata is present
```

# Setup
```
./setup.sh
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
