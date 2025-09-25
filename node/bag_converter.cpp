#include <ros/ros.h>
#include <rosbag/bag.h>
#include <rosbag/view.h>
#include <sensor_msgs/PointCloud2.h>
#include <sensor_msgs/point_cloud2_iterator.h>

#include "source/source_driver.hpp"
#include "msg/ros_msg/rslidar_packet.hpp"

#include <rs_driver/macro/version.hpp>

using namespace robosense::lidar;

std::mutex mtx;

inline Packet toRsMsg(const rslidar_msg::RslidarPacket& ros_msg)
{
  Packet rs_msg;
  rs_msg.timestamp = ros_msg.header.stamp.toSec();
  rs_msg.seq = ros_msg.header.seq;
  rs_msg.is_difop = ros_msg.is_difop;
  rs_msg.is_frame_begin = ros_msg.is_frame_begin; 

  for (size_t i = 0; i < ros_msg.data.size(); i++)
  {
    rs_msg.buf_.emplace_back(ros_msg.data[i]);
  }

  return rs_msg;
}

class SourcePacketRosBag : public SourceDriver
{ 
public: 

  virtual void init(const YAML::Node& config);

  SourcePacketRosBag();

  void putPacket(const rslidar_msg::RslidarPacket& msg);
private:

  std::unique_ptr<ros::NodeHandle> nh_;
};

SourcePacketRosBag::SourcePacketRosBag()
  : SourceDriver(SourceType::MSG_FROM_ROS_PACKET)
{
}

void SourcePacketRosBag::init(const YAML::Node& config)
{
  SourceDriver::init(config);

  nh_ = std::unique_ptr<ros::NodeHandle>(new ros::NodeHandle());
} 

void SourcePacketRosBag::putPacket(const rslidar_msg::RslidarPacket& msg)
{
  driver_ptr_->decodePacket(toRsMsg(msg));
}

inline rslidar_msg::RslidarPacket toRosMsg(const Packet& rs_msg, const std::string& frame_id)
{
  rslidar_msg::RslidarPacket ros_msg;
  ros_msg.header.stamp = ros_msg.header.stamp.fromSec(rs_msg.timestamp);
  ros_msg.header.seq = rs_msg.seq;
  ros_msg.header.frame_id = frame_id;
  ros_msg.is_difop = rs_msg.is_difop;
  ros_msg.is_frame_begin = rs_msg.is_frame_begin;

  for (size_t i = 0; i < rs_msg.buf_.size(); i++)
  {
    ros_msg.data.emplace_back(rs_msg.buf_[i]);
  }

  return ros_msg;
}

inline sensor_msgs::PointCloud2 toRosMsg(const LidarPointCloudMsg& rs_msg, const std::string& frame_id, bool send_by_rows)
{
  sensor_msgs::PointCloud2 ros_msg;

  int fields = 4;
#ifdef POINT_TYPE_XYZIF
  fields = 5;
#elif defined(POINT_TYPE_XYZIRT)
  fields = 6;
#elif defined(POINT_TYPE_XYZIRTF)
  fields = 7;
#endif
  ros_msg.fields.clear();
  ros_msg.fields.reserve(fields);

  if (send_by_rows)
  {
    ros_msg.width = rs_msg.width; 
    ros_msg.height = rs_msg.height; 
  }
  else
  {
    ros_msg.width = rs_msg.height; // exchange width and height to be compatible with pcl::PointCloud<>
    ros_msg.height = rs_msg.width; 
  }

  int offset = 0;
  offset = addPointField(ros_msg, "x", 1, sensor_msgs::PointField::FLOAT32, offset);
  offset = addPointField(ros_msg, "y", 1, sensor_msgs::PointField::FLOAT32, offset);
  offset = addPointField(ros_msg, "z", 1, sensor_msgs::PointField::FLOAT32, offset);
  offset = addPointField(ros_msg, "intensity", 1, sensor_msgs::PointField::FLOAT32, offset);
#if defined(POINT_TYPE_XYZIRT) || defined(POINT_TYPE_XYZIRTF)
  offset = addPointField(ros_msg, "ring", 1, sensor_msgs::PointField::UINT16, offset);
  offset = addPointField(ros_msg, "timestamp", 1, sensor_msgs::PointField::FLOAT64, offset);
#endif

#if defined(POINT_TYPE_XYZIF) || defined(POINT_TYPE_XYZIRTF) 
  offset = addPointField(ros_msg, "feature", 1, sensor_msgs::PointField::UINT8, offset);
#endif

#if 0
  std::cout << "off:" << offset << std::endl;
#endif

  ros_msg.point_step = offset;
  ros_msg.row_step = ros_msg.width * ros_msg.point_step;
  ros_msg.is_dense = rs_msg.is_dense;
  ros_msg.data.resize(ros_msg.point_step * ros_msg.width * ros_msg.height);

  sensor_msgs::PointCloud2Iterator<float> iter_x_(ros_msg, "x");
  sensor_msgs::PointCloud2Iterator<float> iter_y_(ros_msg, "y");
  sensor_msgs::PointCloud2Iterator<float> iter_z_(ros_msg, "z");
  sensor_msgs::PointCloud2Iterator<float> iter_intensity_(ros_msg, "intensity");

#if defined(POINT_TYPE_XYZIRT) || defined(POINT_TYPE_XYZIRTF)
  sensor_msgs::PointCloud2Iterator<uint16_t> iter_ring_(ros_msg, "ring");
  sensor_msgs::PointCloud2Iterator<double> iter_timestamp_(ros_msg, "timestamp");
#endif

#if defined(POINT_TYPE_XYZIF) || defined(POINT_TYPE_XYZIRTF) 
  sensor_msgs::PointCloud2Iterator<uint8_t> iter_feature_(ros_msg, "feature");
#endif

  if (send_by_rows)
  {
    for (size_t i = 0; i < rs_msg.height; i++)
    {
      for (size_t j = 0; j < rs_msg.width; j++)
      {
        const LidarPointCloudMsg::PointT& point = rs_msg.points[i + j * rs_msg.height];

        *iter_x_ = point.x;
        *iter_y_ = point.y;
        *iter_z_ = point.z;
        *iter_intensity_ = point.intensity;

        ++iter_x_;
        ++iter_y_;
        ++iter_z_;
        ++iter_intensity_;

#if defined(POINT_TYPE_XYZIRT) || defined(POINT_TYPE_XYZIRTF)
        *iter_ring_ = point.ring;
        *iter_timestamp_ = point.timestamp;

        ++iter_ring_;
        ++iter_timestamp_;
#endif

#if defined(POINT_TYPE_XYZIF) || defined(POINT_TYPE_XYZIRTF) 
        *iter_feature_ = point.feature;
        ++iter_feature_;
#endif
        
      }
    }
  }
  else
  {
    for (size_t i = 0; i < rs_msg.points.size(); i++)
    {
      const LidarPointCloudMsg::PointT& point = rs_msg.points[i];

      *iter_x_ = point.x;
      *iter_y_ = point.y;
      *iter_z_ = point.z;
      *iter_intensity_ = point.intensity;

      ++iter_x_;
      ++iter_y_;
      ++iter_z_;
      ++iter_intensity_;

#if defined(POINT_TYPE_XYZIRT) || defined(POINT_TYPE_XYZIRTF)
      *iter_ring_ = point.ring;
      *iter_timestamp_ = point.timestamp;

      ++iter_ring_;
      ++iter_timestamp_;
#endif

#if defined(POINT_TYPE_XYZIF) || defined(POINT_TYPE_XYZIRTF) 
        *iter_feature_ = point.feature;
        ++iter_feature_;
#endif
    }
  }

  ros_msg.header.seq = rs_msg.seq;
  ros_msg.header.stamp = ros_msg.header.stamp.fromSec(rs_msg.timestamp);
  ros_msg.header.frame_id = frame_id;

  return ros_msg;
}

class DestinationPointCloudRosBag : public DestinationPointCloud
{
public:

  virtual void init(const YAML::Node& config);
  virtual void sendPointCloud(const LidarPointCloudMsg& msg);
  virtual ~DestinationPointCloudRosBag() = default;
  void setOutputBag(std::shared_ptr<rosbag::Bag> bag);
  void setProcessingStart();
  bool isProcessing();

  private:
  std::shared_ptr<ros::NodeHandle> nh_;
  std::string frame_id_;
  bool send_by_rows_;

  std::shared_ptr<rosbag::Bag> output_bag_;
  bool in_progress_;
};

inline void DestinationPointCloudRosBag::setOutputBag(std::shared_ptr<rosbag::Bag> bag)
{
  output_bag_ = bag;
}

inline void DestinationPointCloudRosBag::init(const YAML::Node& config)
{
  yamlRead<bool>(config["ros"], 
      "ros_send_by_rows", send_by_rows_, false);

  bool dense_points;
  yamlRead<bool>(config["driver"], "dense_points", dense_points, false);
  if (dense_points)
    send_by_rows_ = false;

  yamlRead<std::string>(config["ros"], 
      "ros_frame_id", frame_id_, "rslidar");

  std::string ros_send_topic;
  yamlRead<std::string>(config["ros"], 
      "ros_send_point_cloud_topic", ros_send_topic, "rslidar_points");



  nh_ = std::unique_ptr<ros::NodeHandle>(new ros::NodeHandle());
  in_progress_ = false;
}

inline void DestinationPointCloudRosBag::setProcessingStart()
{
  in_progress_ = true;
}

inline bool DestinationPointCloudRosBag::isProcessing()
{
  return in_progress_;
}

inline void DestinationPointCloudRosBag::sendPointCloud(const LidarPointCloudMsg& msg)
{
  // Skip the first few frames to allow the lidar timestamp to stabilize
  static int count = 0;
  count++;
  if (count < 3)
    return;

  auto ros_msg = toRosMsg(msg, frame_id_, send_by_rows_);
  std::lock_guard<std::mutex> lock(mtx);
  output_bag_->write("/rslidar_points", ros::Time().fromSec(ros_msg.header.stamp.toSec() + 0.122), ros_msg);
}

int main(int argc, char** argv)
{
    ros::init(argc, argv, "bag_converter", ros::init_options::AnonymousName | ros::init_options::NoSigintHandler);

    if (argc != 4)
    {
        ROS_ERROR("Usage: rosrun rslidar_sdk bag_converter <config_path> <input_bag> <output_bag>");
        return 1;
    }

    std::string config_path = argv[1];
    std::string input_bag = argv[2];
    std::string output_bag = argv[3];

    ROS_INFO("********************************************************");
    ROS_INFO("**********    RSLidar Bag Converter        **********");
    ROS_INFO("**********    RSLidar_SDK Version: v%d.%d.%d    **********",
             RSLIDAR_VERSION_MAJOR, RSLIDAR_VERSION_MINOR, RSLIDAR_VERSION_PATCH);
    ROS_INFO("********************************************************");
    ROS_INFO("Input bag:  %s", input_bag.c_str());
    ROS_INFO("Output bag: %s", output_bag.c_str());

    // Open input and output bags
    rosbag::Bag input_bag_;
    std::shared_ptr<rosbag::Bag> output_bag_ = std::make_shared<rosbag::Bag>();
    try
    {
        input_bag_.open(input_bag, rosbag::bagmode::Read);
        output_bag_->open(output_bag, rosbag::bagmode::Write);
    }
    catch (const rosbag::BagException& e)
    {
        ROS_ERROR("Error opening bag files: %s", e.what());
        return 1;
    }

    YAML::Node config;
    config = YAML::LoadFile(config_path);
    YAML::Node lidar_config = yamlSubNodeAbort(config, "lidar");
    std::shared_ptr<SourcePacketRosBag> source;
    source = std::make_shared<SourcePacketRosBag>();
    source->init(lidar_config[0]);
    std::shared_ptr<DestinationPointCloudRosBag> dst = std::make_shared<DestinationPointCloudRosBag>();
    dst->init(lidar_config[0]);
    dst->setOutputBag(output_bag_);
    source->regPointCloudCallback(dst);
    source->start();

    // Iterate over messages in the input bag
    rosbag::View view(input_bag_);
    size_t total_messages = view.size();
    size_t processed_messages = 0;
    size_t packet_count = 0;

    ROS_INFO("Starting conversion of %zu messages...", total_messages);

    for (const rosbag::MessageInstance& m : view)
    {
        // Copy all original messages to output bag
        {
          std::lock_guard<std::mutex> lock(mtx);
          output_bag_->write(m.getTopic(), m.getTime(), m);
        }

        // If this is a lidar packet, also generate point cloud
        if (m.getTopic() == "/rslidar_packets" &&
            m.getDataType() == "rslidar_msg/RslidarPacket")
        {
            auto packet_msg = m.instantiate<rslidar_msg::RslidarPacket>();
            if (packet_msg)
            {
                source->putPacket(*packet_msg);
                packet_count++;
            }
            if (packet_count % 200 == 0)
            {
              // Sleep briefly to allow point cloud processing to catch up - since it is in a different thread
              ros::Duration(0.01).sleep();
            }
        }

        processed_messages++;

        // Progress update every 10000 messages
        if (processed_messages % 10000 == 0)
        {
            double progress = (double)processed_messages / total_messages * 100.0;
            ROS_INFO("Progress: %.1f%% (%zu/%zu messages, %zu packets processed)",
                      progress, processed_messages, total_messages, packet_count);
        }
    }

    // Extra sleep to ensure all point clouds are processed
    ros::Duration(1).sleep();

    input_bag_.close();
    output_bag_->close();

    ROS_INFO("Bag conversion completed successfully!");
    return 0;
}