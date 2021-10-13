// Copyright (c) 2018-2021, Open Source Robotics Foundation, Inc., GAIA Platform, Inc., All rights reserved.  // NOLINT
//
// Redistribution and use in source and binary forms, with or without
// modification, are permitted provided that the following conditions are met:
//
//    * Redistributions of source code must retain the above copyright
//      notice, this list of conditions and the following disclaimer.
//
//    * Redistributions in binary form must reproduce the above copyright
//      notice, this list of conditions and the following disclaimer in the
//      documentation and/or other materials provided with the distribution.
//
//    * Neither the name of the {copyright_holder} nor the names of its
//      contributors may be used to endorse or promote products derived from
//      this software without specific prior written permission.
//
// THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
// AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
// IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE
// ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT HOLDER OR CONTRIBUTORS BE
// LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR
// CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF
// SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS
// INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN
// CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE)
// ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE
// POSSIBILITY OF SUCH DAMAGE.

#include <rclcpp/scope_exit.hpp>
#include <rclcpp/rclcpp.hpp>
#include <rosbag2_snapshot/snapshotter.hpp>

#include <filesystem>

#include <cassert>
#include <chrono>
#include <ctime>
#include <iomanip>
#include <memory>
#include <queue>
#include <sstream>
#include <string>
#include <utility>
#include <vector>

namespace rosbag2_snapshot
{

using namespace std::chrono_literals;  // NOLINT

using rclcpp::Time;
using rosbag2_snapshot_msgs::srv::TriggerSnapshot;
using std::placeholders::_1;
using std::placeholders::_2;
using std::placeholders::_3;
using std::shared_ptr;
using std::string;
using std_srvs::srv::SetBool;

const rclcpp::Duration SnapshotterTopicOptions::NO_DURATION_LIMIT = rclcpp::Duration(-1s);
const int32_t SnapshotterTopicOptions::NO_MEMORY_LIMIT = -1;
const rclcpp::Duration SnapshotterTopicOptions::INHERIT_DURATION_LIMIT = rclcpp::Duration(0s);
const int32_t SnapshotterTopicOptions::INHERIT_MEMORY_LIMIT = 0;

SnapshotterTopicOptions::SnapshotterTopicOptions(
  rclcpp::Duration duration_limit,
  int32_t memory_limit)
: duration_limit_(duration_limit), memory_limit_(memory_limit)
{
}

SnapshotterOptions::SnapshotterOptions(
  rclcpp::Duration default_duration_limit,
  int32_t default_memory_limit)
: default_duration_limit_(default_duration_limit),
  default_memory_limit_(default_memory_limit),
  topics_()
{
}

bool SnapshotterOptions::addTopic(
  const std::string & topic,
  const std::string & type,
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

  // If memory limit is enforced, remove elements from front of queue until limit
  // would be met once message is added
  if (options_.memory_limit_ > SnapshotterTopicOptions::NO_MEMORY_LIMIT) {
    while (queue_.size() != 0 && size_ + size > options_.memory_limit_) {
      _pop();
    }
  }

  // If duration limit is encforced, remove elements from front of queue until duration limit
  // would be met once message is added
  if (options_.duration_limit_ > SnapshotterTopicOptions::NO_DURATION_LIMIT &&
    queue_.size() != 0)
  {
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
  auto ret = lock.try_lock();
  if (!ret) {
    RCLCPP_ERROR(logger_, "Failed to lock. Time %f", _out.time.seconds());
    return;
  }
  _push(_out);
  if (ret) {
    lock.unlock();
  }
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
  parseOptionsFromParams();

  // Create the queue for each topic and set up the subscriber to add to it on new messages
  for (SnapshotterOptions::topics_t::value_type & pair : options_.topics_) {
    string topic = pair.first.first;
    string type = pair.first.second;
    fixTopicOptions(pair.second);
    shared_ptr<MessageQueue> queue;
    queue.reset(new MessageQueue(pair.second, get_logger()));
    std::pair<buffers_t::iterator, bool> res =
      buffers_.insert(buffers_t::value_type(topic, queue));
    assert(res.second);
    subscribe(topic, type, queue);
  }

  // Now that subscriptions are setup, setup service servers for writing and pausing
  trigger_snapshot_server_ = create_service<TriggerSnapshot>(
    "trigger_snapshot", std::bind(&Snapshotter::triggerSnapshotCb, this, _1, _2, _3));
  enable_server_ = create_service<SetBool>(
    "enable_snapshot", std::bind(&Snapshotter::enableCb, this, _1, _2, _3));

  // Start timer to poll for topics
  if (options_.all_topics_) {
    poll_topic_timer_ =
      create_wall_timer(
      std::chrono::duration(1s),
      std::bind(&Snapshotter::pollTopics, this));
  }
}

void Snapshotter::parseOptionsFromParams()
{
  try {
    options_.default_duration_limit_ = rclcpp::Duration::from_seconds(
      declare_parameter<double>(
        "default_duration_limit", -1.0));
  } catch (const rclcpp::ParameterTypeException & ex) {
    RCLCPP_ERROR(get_logger(), "default_duration_limit of incorrect type.");
    throw ex;
  }

  try {
    options_.default_memory_limit_ =
      declare_parameter<double>("default_memory_limit", -1.0);
  } catch (const rclcpp::ParameterTypeException & ex) {
    RCLCPP_ERROR(get_logger(), "default_memory_limit of incorrect type.");
    throw ex;
  }

  const auto all_topics = declare_parameter<std::vector<std::string>>(
    "topics", std::vector<std::string>{});

  if (all_topics.size() > 0) {
    std::vector<std::string> topic_types{};

    try {
      topic_types = declare_parameter<std::vector<std::string>>("topic_types");
    } catch (const rclcpp::ParameterTypeException & ex) {
      RCLCPP_ERROR(
        get_logger(), "If topics are provided, a topic_types array must be provided also.");
      throw ex;
    }

    if (all_topics.size() != topic_types.size()) {
      RCLCPP_ERROR(get_logger(), "Number of topics does not match number of topic_types.");
      throw std::runtime_error{"Parameter mismatch."};
    }

    options_.all_topics_ = false;

    for (std::size_t i = 0; i < all_topics.size(); ++i) {
      options_.topics_.insert(
        SnapshotterOptions::topics_t::value_type(
          std::make_pair(all_topics[i], topic_types[i]), SnapshotterTopicOptions{}));
    }
  } else {
    RCLCPP_INFO(get_logger(), "No topics list provided. Logging all topics.");
    RCLCPP_WARN(get_logger(), "Logging all topics is very memory-intensive.");
    options_.all_topics_ = true;
  }
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

void Snapshotter::topicCb(
  std::shared_ptr<const rclcpp::SerializedMessage> msg,
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
  SnapshotMessage out(msg, now());
  queue->push(out);
}

void Snapshotter::subscribe(
  const string & topic, const string & type,
  std::shared_ptr<MessageQueue> queue)
{
  RCLCPP_INFO(get_logger(), "Subscribing to %s", topic.c_str());

  auto opts = rclcpp::SubscriptionOptions{};
  opts.topic_stats_options.state = rclcpp::TopicStatisticsState::Enable;
  opts.topic_stats_options.publish_topic = topic + "/statistics";

  auto sub = create_generic_subscription(
    topic, type, rclcpp::QoS{10}, std::bind(&Snapshotter::topicCb, this, _1, queue), opts
  );

  queue->setSubscriber(sub);
}

bool Snapshotter::writeTopic(
  rosbag2_cpp::Writer & bag_writer,
  MessageQueue & message_queue,
  string const & topic,
  TriggerSnapshot::Request::SharedPtr & req,
  TriggerSnapshot::Response::SharedPtr & res)
{
  // acquire lock for this queue
  std::lock_guard l(message_queue.lock);

  MessageQueue::range_t range = message_queue.rangeFromTimes(req->start_time, req->stop_time);

  /* TODO(jwhitleywork): FIX
  // open bag if this the first valid topic and there is data
  if (!bag.isOpen() && range.second > range.first) {
    try {
      bag.open(req->filename, rosbag::bagmode::Write);
    } catch (rosbag::BagException const & err) {
      res->success = false;
      res->message = string("failed to open bag: ") + err.what();
      return false;
    }
    RCLCPP_INFO(get_logger(), "Writing snapshot to %s", req->filename.c_str());
  }

  // write queue
  try {
    for (MessageQueue::range_t::first_type msg_it = range.first; msg_it != range.second; ++msg_it) {
      SnapshotMessage const & msg = *msg_it;
      bag.write(topic, msg.time, msg.msg);
    }
  } catch (rosbag::BagException const & err) {
    res->success = false;
    res->message = string("failed to write bag: ") + err.what();
  }
  */
  return true;
}

void Snapshotter::triggerSnapshotCb(
  const std::shared_ptr<rmw_request_id_t> request_header,
  const TriggerSnapshot::Request::SharedPtr req,
  TriggerSnapshot::Response::SharedPtr res)
{
  (void)request_header;

  if (req->filename.empty() || !postfixFilename(req->filename)) {
    res->success = false;
    res->message = "invalid filename";
    return;
  }

  // Store if we were recording prior to write to restore this state after write
  bool recording_prior{true};

  {
    std::shared_lock<std::shared_mutex> read_lock(state_lock_);
    recording_prior = recording_;
    if (writing_) {
      res->success = false;
      res->message = "Already writing";
      return;
    }
  }

  {
    std::unique_lock<std::shared_mutex> write_lock(state_lock_);
    if (recording_prior) {
      pause();
    }
    writing_ = true;
  }

  // Ensure that state is updated when function exits, regardlesss of branch path / exception events
  RCLCPP_SCOPE_EXIT(
    // Clear buffers beacuase time gaps (skipped messages) may have occured while paused
    std::unique_lock<std::shared_mutex> write_lock(state_lock_);
    // Turn off writing flag and return recording to its state before writing
    writing_ = false;
    if (recording_prior) {
      this->resume();
    }
  );

  /* TODO(jwhitleywork): FIX
  // Create bag
  rosbag::Bag bag;

  // Write each selected topic's queue to bag file
  if (req->topics.size() && req->topics.at(0).size()) {
    for (std::string & topic : req->topics) {
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
    res->success = false;
    res->message = res->NO_DATA_MESSAGE;
    return true;
  }
  */

  res->success = true;
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

void Snapshotter::enableCb(
  const std::shared_ptr<rmw_request_id_t> request_header,
  const SetBool::Request::SharedPtr req,
  SetBool::Response::SharedPtr res)
{
  (void)request_header;

  {
    std::shared_lock<std::shared_mutex> read_lock(state_lock_);
    // Cannot enable while writing
    if (req->data && writing_) {
      res->success = false;
      res->message = "cannot enable recording while writing.";
      return;
    }
  }

  // Obtain write lock and update state if requested state is different from current
  if (req->data && !recording_) {
    std::unique_lock<std::shared_mutex> write_lock(state_lock_);
    resume();
  } else if (!req->data && recording_) {
    std::unique_lock<std::shared_mutex> write_lock(state_lock_);
    pause();
  }

  res->success = true;
}

void Snapshotter::pollTopics()
{
  const auto topic_names_and_types = get_topic_names_and_types();

  for (const auto & name_type : topic_names_and_types) {
    if (name_type.second.size() < 1) {
      RCLCPP_ERROR(get_logger(), "Subscribed topic has no associated type.");
      return;
    }

    if (name_type.second.size() > 1) {
      RCLCPP_ERROR(get_logger(), "Subscribed topic has more than one associated type.");
      return;
    }

    if (options_.addTopic(name_type.first, name_type.second[0])) {
      SnapshotterTopicOptions topic_options;
      fixTopicOptions(topic_options);
      auto queue = std::make_shared<MessageQueue>(topic_options, get_logger());
      std::pair<buffers_t::iterator,
        bool> res = buffers_.insert(buffers_t::value_type(name_type.first, queue));
      assert(res.second);
      subscribe(name_type.first, name_type.second[0], queue);
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
    auto client = create_client<TriggerSnapshot>(
      "trigger_snapshot");
    if (!client->service_is_ready()) {
      RCLCPP_ERROR(
        get_logger(),
        "Service trigger_snapshot is not ready. Is snapshot running in this namespace?");
      return;
    }

    TriggerSnapshot::Request::SharedPtr req;
    req->topics = opts.topics_;

    // Prefix mode
    if (opts.filename_.empty()) {
      req->filename = opts.prefix_;
      size_t ind = req->filename.rfind(".bag");
      if (ind != string::npos && ind == req->filename.size() - 4) {
        req->filename.erase(ind);
      }
    } else {
      req->filename = opts.filename_;
      size_t ind = req->filename.rfind(".bag");
      if (ind == string::npos || ind != req->filename.size() - 4) {
        req->filename += ".bag";
      }
    }

    // Resolve filename relative to clients working directory to avoid confusion
    // Special case of no specified file, ensure still in working directory of client
    if (req->filename.empty()) {
      req->filename = "./";
    }
    std::filesystem::path p(std::filesystem::absolute(req->filename));
    req->filename = p.string();

    auto res = client->create_response();
    /* TODO (jwhitleywork) FIX
    if (!client->call(req, res)) {
      RCLCPP_ERROR(get_logger(), "Failed to call service");
      return;
    }
    if (!res.success) {
      RCLCPP_ERROR(get_logger(), "%s", res.message.c_str());
      return;
    }
    */
    return;
  } else if (  // NOLINT
    opts.action_ == SnapshotterClientOptions::PAUSE ||
    opts.action_ == SnapshotterClientOptions::RESUME)
  {
    auto client = create_client<SetBool>("enable_snapshot");
    if (!client->service_is_ready()) {
      RCLCPP_ERROR(
        get_logger(),
        "Service enable_snapshot does not exist. Is snapshot running in this namespace?");
      return;
    }
    SetBool::Request::SharedPtr req;
    req->data = (opts.action_ == SnapshotterClientOptions::RESUME);

    auto res = client->create_response();
    /* TODO(jwhitleywork) FIX
    if (!client->call(req, res)) {
      RCLCPP_ERROR(get_logger(), "Failed to call service.");
      return;
    }
    if (!res.success) {
      RCLCPP_ERROR(get_logger(), "%s", res.message.c_str());
      return;
    }
    return;
    */
  } else {
    throw std::runtime_error{"Invalid options received."};
  }
}

}  // namespace rosbag2_snapshot

#include <rclcpp_components/register_node_macro.hpp>  // NOLINT
RCLCPP_COMPONENTS_REGISTER_NODE(rosbag2_snapshot::Snapshotter)
RCLCPP_COMPONENTS_REGISTER_NODE(rosbag2_snapshot::SnapshotterClient)
