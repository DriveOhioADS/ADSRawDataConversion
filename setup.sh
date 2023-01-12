sudo apt install -y python3-rosbag
sudo apt install -y python3-numpy python3-scipy python3-matplotlib
sudo pip3 install boto3
sudo pip3 install pymongo
sudo pip3 install rospy_message_converter
sudo pip3 install pyprog
#sudo pip3 install sensor-msgs

#ROS2 for 22.04

# sudo apt install software-properties-common
# sudo add-apt-repository universe

# sudo apt update && sudo apt install curl
# sudo curl -sSL https://raw.githubusercontent.com/ros/rosdistro/master/ros.key -o /usr/share/keyrings/ros-archive-keyring.gpg
# echo "deb [arch=$(dpkg --print-architecture) signed-by=/usr/share/keyrings/ros-archive-keyring.gpg] http://packages.ros.org/ros2/ubuntu $(. /etc/os-release && echo $UBUNTU_CODENAME) main" | sudo tee /etc/apt/sources.list.d/ros2.list > /dev/null
# sudo apt update
# sudo apt install -y ros-dev-tools

sudo apt install -y python3-sensor-msgs