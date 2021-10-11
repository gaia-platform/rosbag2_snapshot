/*********************************************************************
* Software License Agreement (BSD License)
*
*  Copyright (c) 2018, Open Source Robotics Foundation, Inc.
*  All rights reserved.
*
*  Redistribution and use in source and binary forms, with or without
*  modification, are permitted provided that the following conditions
*  are met:
*
*   * Redistributions of source code must retain the above copyright
*     notice, this list of conditions and the following disclaimer.
*   * Redistributions in binary form must reproduce the above
*     copyright notice, this list of conditions and the following
*     disclaimer in the documentation and/or other materials provided
*     with the distribution.
*   * Neither the name of Open Source Robotics Foundation, Inc. nor the
*     names of its contributors may be used to endorse or promote products
*     derived from this software without specific prior written permission.
*
*  THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS
*  "AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT
*  LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS
*  FOR A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE
*  COPYRIGHT OWNER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT,
*  INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING,
*  BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES;
*  LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER
*  CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT
*  LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN
*  ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE
*  POSSIBILITY OF SUCH DAMAGE.
********************************************************************/
#include <rclcpp/scope_exit.hpp>
#include <rclcpp/rclcpp.hpp>
#include <rosbag2_snapshot/snapshotter.hpp>

#include <cassert>
#include <chrono>
#include <ctime>
#include <filesystem>
#include <iomanip>
#include <memory>
#include <queue>
#include <sstream>
#include <string>
#include <utility>
#include <vector>

namespace rosbag2_snapshot
{
using namespace std::chrono_literals;

using rclcpp::Time;
using std::shared_ptr;
using std::string;

const rclcpp::Duration NO_DURATION_LIMIT = rclcpp::Duration(-1s);
const rclcpp::Duration INHERIT_DURATION_LIMIT = rclcpp::Duration(0s);

SnapshotterTopicOptions::SnapshotterTopicOptions(
  rclcpp::Duration duration_limit,
  int32_t memory_limit)
: duration_limit_(duration_limit), memory_limit_(memory_limit)
{
}

SnapshotterOptions::SnapshotterOptions(
  rclcpp::Duration default_duration_limit,
  int32_t default_memory_limit,
  rclcpp::Duration status_period)
: default_duration_limit_(default_duration_limit),
  default_memory_limit_(default_memory_limit),
  status_period_(status_period),
  topics_()
{
}

bool SnapshotterOptions::addTopic(
  std::string const & topic,
  std::string const & type,
  rclcpp::Duration duration,
  int32_t memory)
{
  SnapshotterTopicOptions ops(duration, memory);
  std::pair<topics_t::iterator, bool> ret;
  ret = topics_.insert(topics_t::value_type(std::make_pair(topic, type), ops));
  return ret.second;
}

SnapshotterClientOptions::SnapshotterClientOptions()
: action_(SnapshotterClientOptions::TRIGGER_WRITE)
{
}

SnapshotMessage::SnapshotMessage(
  std::shared_ptr<const rclcpp::SerializedMessage> _msg, Time _time)
: msg(_msg), time(_time)
{
}

MessageQueue::MessageQueue(const SnapshotterTopicOptions & options, const rclcpp::Logger & logger)
: options_(options), logger_(logger), size_(0)
{
}

void MessageQueue::setSubscriber(shared_ptr<rclcpp::GenericSubscription> sub)
{
  sub_ = sub;
}

void MessageQueue::clear()
{
  std::lock_guard<std::mutex> l(lock);
  _clear();
}

void MessageQueue::_clear()
{
  queue_.clear();
  size_ = 0;
}

rclcpp::Duration MessageQueue::duration() const
{
  // No duration if 0 or 1 messages
  if (queue_.size() <= 1) {
    return rclcpp::Duration(0s);
  }
  return queue_.back().time - queue_.front().time;
}

bool MessageQueue::preparePush(int32_t size, rclcpp::Time const & time)
{
  // If new message is older than back of queue, time has gone backwards and buffer must be cleared
  if (!queue_.empty() && time < queue_.back().time) {
    RCLCPP_WARN(logger_, "Time has gone backwards. Clearing buffer for this topic.");
    _clear();
  }

  // The only case where message cannot be addded is if size is greater than limit
  if (options_.memory_limit_ > SnapshotterTopicOptions::NO_MEMORY_LIMIT &&
    size > options_.memory_limit_)
  {
    return false;
  }

  // If memory limit is enforced, remove elements from front of queue until limit would be met once message is added
  if (options_.memory_limit_ > SnapshotterTopicOptions::NO_MEMORY_LIMIT) {
    while (queue_.size() != 0 && size_ + size > options_.memory_limit_) {
      _pop();
    }
  }

  // If duration limit is encforced, remove elements from front of queue until duration limit would be met once message
  // is added
  if (options_.duration_limit_ > SnapshotterTopicOptions::NO_DURATION_LIMIT && queue_.size() != 0) {
    rclcpp::Duration dt = time - queue_.front().time;
    while (dt > options_.duration_limit_) {
      _pop();
      if (queue_.empty()) {
        break;
      }
      dt = time - queue_.front().time;
    }
  }
  return true;
}
void MessageQueue::push(SnapshotMessage const & _out)
{
  auto ret = std::try_lock(lock);
  if (ret != -1) {
    RCLCPP_ERROR(logger_, "Failed to lock. Time %f", _out.time.seconds());
    return;
  }
  _push(_out);
}

SnapshotMessage MessageQueue::pop()
{
  std::lock_guard<std::mutex> l(lock);
  return _pop();
}

int64_t MessageQueue::getMessageSize(SnapshotMessage const & snapshot_msg) const
{
  return snapshot_msg.msg->size() + sizeof(SnapshotMessage);
}

void MessageQueue::_push(SnapshotMessage const & _out)
{
  int32_t size = _out.msg->size();
  // If message cannot be added without violating limits, it must be dropped
  if (!preparePush(size, _out.time)) {
    return;
  }
  queue_.push_back(_out);
  // Add size of new message to running count to maintain correctness
  size_ += getMessageSize(_out);
}

SnapshotMessage MessageQueue::_pop()
{
  SnapshotMessage tmp = queue_.front();
  queue_.pop_front();
  //  Remove size of popped message to maintain correctness of size_
  size_ -= getMessageSize(tmp);
  return tmp;
}

MessageQueue::range_t MessageQueue::rangeFromTimes(Time const & start, Time const & stop)
{
  range_t::first_type begin = queue_.begin();
  range_t::second_type end = queue_.end();

  // Increment / Decrement iterators until time contraints are met
  if (start.seconds() != 0.0 || start.nanoseconds() != 0) {
    while (begin != end && (*begin).time < start) {
      ++begin;
    }
  }
  if (stop.seconds() != 0.0 || stop.nanoseconds() != 0) {
    while (end != begin && (*(end - 1)).time > stop) {
      --end;
    }
  }
  return range_t(begin, end);
}

const int Snapshotter::QUEUE_SIZE = 10;

Snapshotter::Snapshotter(const rclcpp::NodeOptions & options)
: rclcpp::Node("snapshotter", options),
  recording_(true),
  writing_(false)
{
  // TODO(jwhitleywork): Declare parameters
  parseOptionsFromParams();

  // Create the queue for each topic and set up the subscriber to add to it on new messages
  for (SnapshotterOptions::topics_t::value_type & pair : options_.topics_) {
    string topic = pair.first.first;
    string type = pair.first.second;
    fixTopicOptions(pair.second);
    shared_ptr<MessageQueue> queue;
    queue.reset(new MessageQueue(pair.second, get_logger()));
    std::pair<buffers_t::iterator, bool> res =
      buffers_.insert(buffers_t::value_type(std::make_pair(topic, type), queue));
    assert(res.second);
    subscribe(topic, type, queue);
  }

  // Now that subscriptions are setup, setup service servers for writing and pausing
  trigger_snapshot_server_ = nh_.advertiseService(
    "trigger_snapshot",
    &Snapshotter::triggerSnapshotCb, this);
  enable_server_ = nh_.advertiseService("enable_snapshot", &Snapshotter::enableCB, this);

  // Start timer to poll for topics
  if (options_.all_topics_) {
    poll_topic_timer_ =
      create_wall_timer(
      rclcpp::Duration(1s),
      std::bind(&Snapshotter::pollTopics, this));
  }
}

Snapshotter::~Snapshotter()
{
  // Each buffer contains a pointer to the subscriber and vice versa, so we need to
  // shutdown the subscriber to allow garbage collection to happen
  for (std::pair<const std::string, std::shared_ptr<MessageQueue>> & buffer : buffers_) {
    buffer.second->sub_->shutdown();
  }
}

Snapshotter::parseOptionsFromParams()
{
}

void Snapshotter::fixTopicOptions(SnapshotterTopicOptions & options)
{
  if (options.duration_limit_ == SnapshotterTopicOptions::INHERIT_DURATION_LIMIT) {
    options.duration_limit_ = options_.default_duration_limit_;
  }
  if (options.memory_limit_ == SnapshotterTopicOptions::INHERIT_MEMORY_LIMIT) {
    options.memory_limit_ = options_.default_memory_limit_;
  }
}

bool Snapshotter::postfixFilename(string & file)
{
  size_t ind = file.rfind(".bag");
  // If requested ends in .bag, this is literal name do not append date
  if (ind != string::npos && ind == file.size() - 4) {
    return true;
  }
  // Otherwise treat as prefix and append datetime and extension
  file += timeAsStr() + ".bag";
  return true;
}

string Snapshotter::timeAsStr()
{
  std::stringstream msg;
  const auto now = std::chrono::system_clock::now();
  const auto now_in_t = std::chrono::system_clock::to_time_t(now);
  msg << std::put_time(std::localtime(&now_in_t), "%Y-%m-%d-%H-%M-%S");
  return msg.str();
}

void Snapshotter::topicCB(
  const ros::MessageEvent<topic_tools::ShapeShifter const> & msg_event,
  std::shared_ptr<MessageQueue> queue)
{
  // If recording is paused (or writing), exit
  {
    std::shared_lock<std::shared_mutex> lock(state_lock_);
    if (!recording_) {
      return;
    }
  }

  // Pack message and metadata into SnapshotMessage holder
  SnapshotMessage out(msg_event.getMessage(),
    msg_event.getConnectionHeaderPtr(), msg_event.getReceiptTime());
  queue->push(out);
}

void Snapshotter::subscribe(string const & topic, std::shared_ptr<MessageQueue> queue)
{
  RCLCPP_INFO(get_logger(), "Subscribing to %s", topic.c_str());

  auto ops = rclcpp::SubscriptionOptions;
  opts.topic_stats_options.state = rclcpp::TopicStatisticsState::Enable;
  opts.topic_stats_options.publish_topic = topic + "/statistics";

  auto sub = create_generic_subscription(
    topic,
  );

  ops.publish_topic = topic;
  ops.queue_size = QUEUE_SIZE;
  ops.md5sum = ros::message_traits::md5sum<topic_tools::ShapeShifter>();
  ops.datatype = ros::message_traits::datatype<topic_tools::ShapeShifter>();
  ops.helper =
    std::make_shared<ros::SubscriptionCallbackHelperT<const ros::MessageEvent<topic_tools::ShapeShifter const> &>>(
    std::bind(&Snapshotter::topicCB, this, _1, queue));
  *sub = nh_.subscribe(ops);
  queue->setSubscriber(sub);
}

bool Snapshotter::writeTopic(
  rosbag::Bag & bag, MessageQueue & message_queue, string const & topic,
  rosbag2_snapshot_msgs::TriggerSnapshot::Request & req,
  rosbag2_snapshot_msgs::TriggerSnapshot::Response & res)
{
  // acquire lock for this queue
  std::lock_guard l(message_queue.lock);

  MessageQueue::range_t range = message_queue.rangeFromTimes(req.start_time, req.stop_time);

  // open bag if this the first valid topic and there is data
  if (!bag.isOpen() && range.second > range.first) {
    try {
      bag.open(req.filename, rosbag::bagmode::Write);
    } catch (rosbag::BagException const & err) {
      res.success = false;
      res.message = string("failed to open bag: ") + err.what();
      return false;
    }
    RCLCPP_INFO(get_logger(), "Writing snapshot to %s", req.filename.c_str());
  }

  // write queue
  try {
    for (MessageQueue::range_t::first_type msg_it = range.first; msg_it != range.second; ++msg_it) {
      SnapshotMessage const & msg = *msg_it;
      bag.write(topic, msg.time, msg.msg);
    }
  } catch (rosbag::BagException const & err) {
    res.success = false;
    res.message = string("failed to write bag: ") + err.what();
  }
  return true;
}

bool Snapshotter::triggerSnapshotCb(
  rosbag2_snapshot_msgs::TriggerSnapshot::Request & req,
  rosbag2_snapshot_msgs::TriggerSnapshot::Response & res)
{
  if (!postfixFilename(req.filename)) {
    res.success = false;
    res.message = "invalid";
    return true;
  }
  bool recording_prior;  // Store if we were recording prior to write to restore this state after write
  {
    std::shared_lock<std::shared_mutex> read_lock(state_lock_);
    recording_prior = recording_;
    if (writing_) {
      res.success = false;
      res.message = "Already writing";
      return true;
    }
    std::unique_lock<std::shared_mutex> write_lock(state_lock_);
    if (recording_prior) {
      pause();
    }
    writing_ = true;
  }

  // Ensure that state is updated when function exits, regardlesss of branch path / exception events
  rclcpp::make_scope_exit(
    [&state_lock_, &writing_, recording_prior, this_]()
    {
      // Clear buffers beacuase time gaps (skipped messages) may have occured while paused
      std::unique_lock<std::shared_mutex> write_lock(state_lock_);
      // Turn off writing flag and return recording to its state before writing
      writing_ = false;
      if (recording_prior) {
        this_->resume();
      }
    }
  );

  // Create bag
  rosbag::Bag bag;

  // Write each selected topic's queue to bag file
  if (req.topics.size() && req.topics.at(0).size()) {
    for (std::string & topic : req.topics) {
      // Resolve and clean topic
      try {
        topic = ros::names::resolve(nh_.getNamespace(), topic);
      } catch (ros::InvalidNameException const & err) {
        RCLCPP_WARN(get_logger(), "Requested topic %s is invalid, skipping.", topic.c_str());
        continue;
      }

      // Find the message queue for this topic if it exsists
      buffers_t::iterator found = buffers_.find(topic);
      // If topic not found, error and exit
      if (found == buffers_.end()) {
        RCLCPP_WARN(get_logger(), "Requested topic %s is not subscribed, skipping.", topic.c_str());
        continue;
      }
      MessageQueue & message_queue = *(*found).second;
      if (!writeTopic(bag, message_queue, topic, req, res)) {
        return true;
      }
    }
  }
  // If topic list empty, record all buffered topics
  else {
    for (const buffers_t::value_type & pair : buffers_) {
      MessageQueue & message_queue = *(pair.second);
      std::string const & topic = pair.first;
      if (!writeTopic(bag, message_queue, topic, req, res)) {
        return true;
      }
    }
  }

  // If no topics were subscribed/valid/contained data, this is considered a non-success
  if (!bag.isOpen()) {
    res.success = false;
    res.message = res.NO_DATA_MESSAGE;
    return true;
  }

  res.success = true;
  return true;
}

void Snapshotter::clear()
{
  for (const buffers_t::value_type & pair : buffers_) {
    pair.second->clear();
  }
}

void Snapshotter::pause()
{
  RCLCPP_INFO(get_logger(), "Buffering paused");
  recording_ = false;
}

void Snapshotter::resume()
{
  clear();
  recording_ = true;
  RCLCPP_INFO(get_logger(), "Buffering resumed and old data cleared.");
}

bool Snapshotter::enableCB(std_srvs::SetBool::Request & req, std_srvs::SetBool::Response & res)
{
  std::shared_lock<std::shared_mutex> read_lock(state_lock_);
  if (req.data && writing_) { // Cannot enable while writing
    res.success = false;
    res.message = "cannot enable recording while writing.";
    return true;
  }
  // Obtain write lock and update state if requested state is different from current
  if (req.data && !recording_) {
    std::unique_lock<std::shared_mutex> write_lock(state_lock_);
    resume();
  } else if (!req.data && recording_) {
    std::unique_lock<std::shared_mutex> write_lock(state_lock_);
    pause();
  }
  res.success = true;
  return true;
}

void Snapshotter::pollTopics()
{
  const auto topic_names_types = get_topic_names_and_types();
  
  for (const auto & name_type : topic_names_and_types) {
    if (options_->addTopic(name_type.first)) {
      SnapshotterTopicOptions topic_options;
      fixTopicOptions(topic_options);
      std::shared_ptr<MessageQueue> queue;
      queue.reset(new MessageQueue(topic_options, get_logger()));
      std::pair<buffers_t::iterator,
        bool> res = buffers_.insert(buffers_t::value_type(topic, queue));
      assert(res.second);
      subscribe(name_type.first, name_type.second, queue);
    }
  }
}

SnapshotterClient::SnapshotterClient(const rclcpp::NodeOptions & options)
: rclcpp::Node("snapshotter_client", options)
{
}

void SnapshotterClient::setSnapshotterClientOptions(const SnapshotterClientOptions & opts)
{
  if (opts.action_ == SnapshotterClientOptions::TRIGGER_WRITE) {
    ros::ServiceClient client = nh_.serviceClient<rosbag2_snapshot_msgs::TriggerSnapshot>(
      "trigger_snapshot");
    if (!client.exists()) {
      RCLCPP_ERROR(
        get_logger(), 
        "Service %s does not exist. Is snapshot running in this namespace?",
        "trigger_snapshot");
      return 1;
    }
    rosbag2_snapshot_msgs::TriggerSnapshotRequest req;
    req.topics = opts.topics_;
    // Prefix mode
    if (opts.filename_.empty()) {
      req.filename = opts.prefix_;
      size_t ind = req.filename.rfind(".bag");
      if (ind != string::npos && ind == req.filename.size() - 4) {
        req.filename.erase(ind);
      }
    } else {
      req.filename = opts.filename_;
      size_t ind = req.filename.rfind(".bag");
      if (ind == string::npos || ind != req.filename.size() - 4) {
        req.filename += ".bag";
      }
    }

    // Resolve filename relative to clients working directory to avoid confusion
    if (req.filename.empty()) { // Special case of no specified file, ensure still in working directory of client
      req.filename = "./";
    }
    std::filesystem::path p(std::filesystem::absolute(req.filename));
    req.filename = p.string();

    rosbag2_snapshot_msgs::TriggerSnapshotResponse res;
    if (!client.call(req, res)) {
      RCLCPP_ERROR(get_logger(), "Failed to call service");
      return 1;
    }
    if (!res.success) {
      RCLCPP_ERROR(get_logger(), "%s", res.message.c_str());
      return 1;
    }
    return 0;
  } else if (opts.action_ == SnapshotterClientOptions::PAUSE ||
    opts.action_ == SnapshotterClientOptions::RESUME)
  {
    ros::ServiceClient client = nh_.serviceClient<std_srvs::SetBool>("enable_snapshot");
    if (!client.exists()) {
      RCLCPP_ERROR(
        get_logger(), 
        "Service %s does not exist. Is snapshot running in this namespace?",
        "enable_snapshot");
      return 1;
    }
    std_srvs::SetBoolRequest req;
    req.data = (opts.action_ == SnapshotterClientOptions::RESUME);
    std_srvs::SetBoolResponse res;
    if (!client.call(req, res)) {
      RCLCPP_ERROR(get_logger(), "Failed to call service.");
      return 1;
    }
    if (!res.success) {
      RCLCPP_ERROR(get_logger(), "%s", res.message.c_str());
      return 1;
    }
    return 0;
  } else {
    assert(false);
    return 1;
  }
}

}  // namespace rosbag2_snapshot

#include <rclcpp_components/register_node_macro.hpp>  // NOLINT
RCLCPP_COMPONENTS_REGISTER_NODE(rosbag2_snapshot::Snapshotter)
RCLCPP_COMPONENTS_REGISTER_NODE(rosbag2_snapshot::SnapshotterClient)
