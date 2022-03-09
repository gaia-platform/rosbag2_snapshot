// Copyright (c) 2018-2022, Open Source Robotics Foundation, Inc., GAIA Platform, Inc., UPower Robotics USA, All rights reserved.  // NOLINT
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

#if __has_include(<experimental/filesystem>)
#include <experimental/filesystem>
namespace fs = std::experimental::filesystem;
#else
#include <filesystem>
namespace fs = std::filesystem;
#endif

#include <cassert>
#include <chrono>
#include <ctime>
#include <exception>
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
static constexpr uint32_t MB_TO_B = 1e6;

SnapshotterTopicOptions::SnapshotterTopicOptions(
  rclcpp::Duration duration_limit,
  int32_t memory_limit)
: duration_limit_(duration_limit), memory_limit_(memory_limit)
{
}

SnapshotterOptions::SnapshotterOptions(
  rclcpp::Duration default_duration_limit,
  int32_t default_memory_limit,
  u_int64_t system_wide_memory_limit)
: default_duration_limit_(default_duration_limit),
  default_memory_limit_(default_memory_limit),
  system_wide_memory_limit_(system_wide_memory_limit),
  topics_()
{
}

bool SnapshotterOptions::addTopic(
  const TopicDetails & topic_details,
  rclcpp::Duration duration,
  int32_t memory)
{
  SnapshotterTopicOptions ops(duration, memory);
  std::pair<topics_t::iterator, bool> ret;
  ret = topics_.emplace(topic_details, ops);
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

std::shared_ptr<MessageQueueCollectionManager>
  MessageQueueCollectionManager::instance_ = nullptr;

MessageQueueCollectionManager::MessageQueueCollectionManager(
  const SnapshotterTopicOptions & options, const rclcpp::Logger & logger
)
: options_(options), logger_(logger), p_queue_(), lock_()
{
  uint64_t mb_limit = options.system_wide_memory_limit_;

  this->size_ = 0;

  if(mb_limit > SnapshotterTopicOptions::NO_MEMORY_LIMIT)
    this->size_limit_ = mb_limit * MB_TO_B;
  else
    this->size_limit_ = SnapshotterTopicOptions::NO_MEMORY_LIMIT;
}

void MessageQueueCollectionManager::report_queue_creation(MessageQueue& queue){
  std::lock_guard<std::mutex> l(this->lock_);

  this->p_queue_.push_back(&queue);
  this->size_ += queue.size_;
}

void MessageQueueCollectionManager::report_queue_size_change(){
  std::lock_guard<std::mutex> l(this->lock_);

  int64_t new_total_size = 0;
  auto it = this->p_queue_.begin();

  while(it != this->p_queue_.end())
  {
    auto & p_current_queue = * it;
    new_total_size += p_current_queue->size_;
    it++;
  }

  this->size_ = new_total_size;
}

void MessageQueueCollectionManager::report_queue_size_change(int64_t delta_size){
  std::lock_guard<std::mutex> l(this->lock_);

  this->size_ += delta_size;
}

void MessageQueueCollectionManager::report_queue_size_change(u_int64_t old_size, u_int64_t new_size){
  this->report_queue_size_change(new_size - old_size);
}

void MessageQueueCollectionManager::report_queue_destruction(MessageQueue& queue)
{
  std::lock_guard<std::mutex> l(this->lock_);

  auto it = this->p_queue_.begin();

  while(it != this->p_queue_.end())
  {
    auto p_current_queue = *it;
    if(&queue == p_current_queue)
    {
      this->size_ -= queue.size_;
      this->p_queue_.erase(it);
      return;
    }

    it++;
  }
}

u_int64_t MessageQueueCollectionManager::get_total_queue_collection_size(){
  return this->size_;
}

MessageQueueCollectionManager & MessageQueueCollectionManager::Instance(const MessageQueue & msg_queue){
  if(MessageQueueCollectionManager::instance_ == nullptr){
    auto logger = msg_queue.logger_;
    auto options = msg_queue.options_;
    MessageQueueCollectionManager::instance_ = std::make_shared<MessageQueueCollectionManager>(options, logger);
  }

  auto the_instance = MessageQueueCollectionManager::instance_.get();

  return *the_instance;
}

void MessageQueueCollectionManager::free_oldest_messages(size_t free_bytes_required)
{
  rclcpp::Time oldest_message_t;
  MessageQueue * p_oldest_message_q;

  if(this->size_limit_ == SnapshotterTopicOptions::NO_MEMORY_LIMIT){
    return;
  }

  std::lock_guard<std::mutex> l(this->lock_);

  while(this->size_limit_ - this->size_ < free_bytes_required){
    p_oldest_message_q = NULL;

    auto it = this->p_queue_.begin();

    while(it != this->p_queue_.end())
    {
      auto p_current_queue = * it;
      rclcpp::Time q_msg_t;

      try{
        q_msg_t = p_current_queue->get_oldest_message_time();
      }
      catch(std::exception e){
        // the queue must be empty
        continue;
      }

      if(p_oldest_message_q == NULL){
        // first queue, nothing to compare with yet
        p_oldest_message_q = p_current_queue;
        oldest_message_t = q_msg_t;
      }
      else if(q_msg_t < oldest_message_t){
        // this queue has a message older than the
        // previous oldest message
        p_oldest_message_q = p_current_queue;
        oldest_message_t = q_msg_t;
      }

      it++;
    }

    if(p_oldest_message_q == NULL){
      // There were no queues with any messages in storage
      break;
    }

    p_oldest_message_q->pop();
  }
}

MessageQueue::MessageQueue(const SnapshotterTopicOptions & options, const rclcpp::Logger & logger)
: options_(options), logger_(logger), size_(0)
{
  MessageQueueCollectionManager::Instance(*this).report_queue_creation(*this);
}

MessageQueue::~MessageQueue(){
  MessageQueueCollectionManager::Instance(*this).report_queue_destruction(*this);
}

rclcpp::Time MessageQueue::get_oldest_message_time()
{
  return this->queue_.at(0).time;
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
  MessageQueueCollectionManager::Instance(*this).report_queue_size_change(size_, 0);
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

  if(options_.system_wide_memory_limit_ > SnapshotterTopicOptions::NO_MEMORY_LIMIT){
    if(size > options_.system_wide_memory_limit_){
      return false;
    }

    MessageQueueCollectionManager::Instance(*this).free_oldest_messages(size);
  }

  // The only case where message cannot be addded is if size is greater than limit
  if (options_.memory_limit_ > SnapshotterTopicOptions::NO_MEMORY_LIMIT) {
    if (size > options_.memory_limit_)
    {
      return false;
    }

    // If memory limit is enforced, remove elements from front of queue until limit
    // would be met once message is added
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
  MessageQueueCollectionManager::Instance(*this).report_queue_size_change(getMessageSize(_out));
  size_ += getMessageSize(_out);
}

SnapshotMessage MessageQueue::_pop()
{
  SnapshotMessage tmp = queue_.front();
  queue_.pop_front();
  //  Remove size of popped message to maintain correctness of size_
  MessageQueueCollectionManager::Instance(*this).report_queue_size_change(0 - getMessageSize(tmp));
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
  for (auto & pair : options_.topics_) {
    string topic{pair.first.name}, type{pair.first.type};
    fixTopicOptions(pair.second);
    shared_ptr<MessageQueue> queue;
    queue.reset(new MessageQueue(pair.second, get_logger()));

    TopicDetails details{};
    details.name = topic;
    details.type = type;
    std::pair<buffers_t::iterator, bool> res =
      buffers_.emplace(details, queue);
    assert(res.second);

    subscribe(details, queue);
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

Snapshotter::~Snapshotter()
{
  for (auto & buffer : buffers_) {
    buffer.second->sub_.reset();
  }
}

void Snapshotter::parseOptionsFromParams()
{
  std::vector<std::string> topics{};

  try {
    options_.default_duration_limit_ = rclcpp::Duration::from_seconds(
      declare_parameter<double>("default_duration_limit", -1.0));
  } catch (const rclcpp::ParameterTypeException & ex) {
    RCLCPP_ERROR(get_logger(), "default_duration_limit is of incorrect type.");
    throw ex;
  }

  try {
    options_.default_memory_limit_ =
      declare_parameter<double>("default_memory_limit", -1.0);
  } catch (const rclcpp::ParameterTypeException & ex) {
    RCLCPP_ERROR(get_logger(), "default_memory_limit is of incorrect type.");
    throw ex;
  }

  try {
    options_.system_wide_memory_limit_ =
      declare_parameter<double>("system_wide_memory_limit", -1.0);
  } catch (const rclcpp::ParameterTypeException & ex) {
    RCLCPP_ERROR(get_logger(), "system_wide_memory_limit is of incorrect type.");
    throw ex;
  }

  // Convert memory limit in MB to B
  if (options_.default_memory_limit_ != -1.0) {
    options_.default_memory_limit_ *= MB_TO_B;
  }

  try {
    topics = declare_parameter<std::vector<std::string>>(
      "topics", std::vector<std::string>{});
  } catch (const rclcpp::ParameterTypeException & ex) {
    if (std::string{ex.what()}.find("not set") == std::string::npos) {
      RCLCPP_ERROR(get_logger(), "topics must be an array of strings.");
      throw ex;
    }
  }

  if (topics.size() > 0) {
    options_.all_topics_ = false;

    for (const auto & topic : topics) {
      std::string prefix = "topic_details." + topic;
      std::string topic_type{};
      SnapshotterTopicOptions opts{};

      try {
        topic_type = declare_parameter<std::string>(prefix + ".type");
      } catch (const rclcpp::ParameterTypeException & ex) {
        if (std::string{ex.what()}.find("not set") == std::string::npos) {
          RCLCPP_ERROR(get_logger(), "Topic type must be a string.");
        } else {
          RCLCPP_ERROR(get_logger(), "Topic %s is missing a type.", topic.c_str());
        }

        throw ex;
      }

      try {
        opts.duration_limit_ = rclcpp::Duration::from_seconds(
          declare_parameter<double>(prefix + ".duration")
        );
      } catch (const rclcpp::ParameterTypeException & ex) {
        if (std::string{ex.what()}.find("not set") == std::string::npos) {
          RCLCPP_ERROR(
            get_logger(), "Duration limit for topic %s must be a double.", topic.c_str());
          throw ex;
        }
      }

      try {
        opts.memory_limit_ = declare_parameter<double>(prefix + ".memory");
      } catch (const rclcpp::ParameterTypeException & ex) {
        if (std::string{ex.what()}.find("not set") == std::string::npos) {
          RCLCPP_ERROR(
            get_logger(), "Memory limit for topic %s is of the wrong type.", topic.c_str());
          throw ex;
        }
      }

      TopicDetails dets{};
      dets.name = topic;
      dets.type = topic_type;

      options_.topics_.insert(
        SnapshotterOptions::topics_t::value_type(dets, opts));
    }
  } else {
    options_.all_topics_ = true;
    RCLCPP_INFO(get_logger(), "No topics list provided. Logging all topics.");
    RCLCPP_WARN(get_logger(), "Logging all topics is very memory-intensive.");
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
  const TopicDetails & topic_details,
  std::shared_ptr<MessageQueue> queue)
{
  RCLCPP_INFO(get_logger(), "Subscribing to %s", topic_details.name.c_str());

  auto opts = rclcpp::SubscriptionOptions{};
  opts.topic_stats_options.state = rclcpp::TopicStatisticsState::Enable;
  opts.topic_stats_options.publish_topic = topic_details.name + "/statistics";

  auto sub = create_generic_subscription(
    topic_details.name,
    topic_details.type,
    rclcpp::QoS{10},
    std::bind(&Snapshotter::topicCb, this, _1, queue),
    opts
  );

  queue->setSubscriber(sub);
}

bool Snapshotter::writeTopic(
  rosbag2_cpp::Writer & bag_writer,
  MessageQueue & message_queue,
  const TopicDetails & topic_details,
  const TriggerSnapshot::Request::SharedPtr & req,
  const TriggerSnapshot::Response::SharedPtr & res)
{
  // acquire lock for this queue
  std::lock_guard l(message_queue.lock);

  MessageQueue::range_t range = message_queue.rangeFromTimes(req->start_time, req->stop_time);

  rosbag2_storage::TopicMetadata tm;
  tm.name = topic_details.name;
  tm.type = topic_details.type;
  tm.serialization_format = "cdr";

  bag_writer.create_topic(tm);

  for (auto msg_it = range.first; msg_it != range.second; ++msg_it) {
    // Create BAG message
    auto bag_message = std::make_shared<rosbag2_storage::SerializedBagMessage>();
    auto ret = rcutils_system_time_now(&bag_message->time_stamp);
    if (ret != RCL_RET_OK) {
      RCLCPP_ERROR(get_logger(), "Failed to assign time to rosbag message.");
      return false;
    }

    bag_message->topic_name = tm.name;
    bag_message->time_stamp = msg_it->time.nanoseconds();
    bag_message->serialized_data = std::make_shared<rcutils_uint8_array_t>(
      msg_it->msg->get_rcl_serialized_message()
    );

    bag_writer.write(bag_message);
  }

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
    res->message = "Invalid filename";
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

  rosbag2_cpp::Writer bag_writer{};

  try {
    bag_writer.open(req->filename);
  } catch (const std::exception & ex) {
    res->success = false;
    res->message = "Unable to open file for writing.";
    return;
  }

  // Write each selected topic's queue to bag file
  if (req->topics.size() && req->topics.at(0).name.size() && req->topics.at(0).type.size()) {
    for (auto & topic : req->topics) {
      TopicDetails details{topic.name, topic.type};
      // Find the message queue for this topic if it exsists
      auto found = buffers_.find(details);

      if (found == buffers_.end()) {
        RCLCPP_WARN(
          get_logger(), "Requested topic %s is not subscribed, skipping.", topic.name.c_str());
        continue;
      }

      MessageQueue & message_queue = *(found->second);

      if (!writeTopic(bag_writer, message_queue, details, req, res)) {
        res->success = false;
        res->message = "Failed to write topic " + topic.type + " to bag file.";
        return;
      }
    }
  } else {  // If topic list empty, record all buffered topics
    for (const buffers_t::value_type & pair : buffers_) {
      MessageQueue & message_queue = *(pair.second);
      if (!writeTopic(bag_writer, message_queue, pair.first, req, res)) {
        res->success = false;
        res->message = "Failed to write topic " + pair.first.name + " to bag file.";
        return;
      }
    }
  }

  /*
  // If no topics were subscribed/valid/contained data, this is considered a non-success
  if (!bag.isOpen()) {
    res->success = false;
    res->message = res->NO_DATA_MESSAGE;
    return;
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

    TopicDetails details{};
    details.name = name_type.first;
    details.type = name_type.second[0];

    if (options_.addTopic(details)) {
      SnapshotterTopicOptions topic_options;
      fixTopicOptions(topic_options);
      auto queue = std::make_shared<MessageQueue>(topic_options, get_logger());

      std::pair<buffers_t::iterator,
        bool> res = buffers_.emplace(details, queue);
      assert(res.second);
      subscribe(details, queue);
    }
  }
}

SnapshotterClient::SnapshotterClient(const rclcpp::NodeOptions & options)
: rclcpp::Node("snapshotter_client", options)
{
  std::string action_str{};

  SnapshotterClientOptions opts{};

  try {
    action_str = declare_parameter<std::string>("action_type");
  } catch (const rclcpp::ParameterTypeException & ex) {
    RCLCPP_ERROR(get_logger(), "action_type parameter is missing or of incorrect type.");
    throw ex;
  }

  if (action_str == "trigger_write") {
    opts.action_ = SnapshotterClientOptions::TRIGGER_WRITE;
  } else if (action_str == "resume") {
    opts.action_ = SnapshotterClientOptions::RESUME;
  } else if (action_str == "pause") {
    opts.action_ = SnapshotterClientOptions::PAUSE;
  } else {
    RCLCPP_ERROR(get_logger(), "action_type must be one of: trigger_write, resume, or pause");
    throw std::invalid_argument{"Invalid value for action_type parameter."};
  }

  std::vector<std::string> topic_names{};

  try {
    topic_names = declare_parameter<std::vector<std::string>>("topics");
  } catch (const rclcpp::ParameterTypeException & ex) {
    if (std::string{ex.what()}.find("not set") == std::string::npos) {
      RCLCPP_ERROR(get_logger(), "topics must be an array of strings.");
      throw ex;
    }
  }

  if (topic_names.size() > 0) {
    for (const auto & topic : topic_names) {
      std::string prefix = "topic_details." + topic;
      std::string topic_type{};

      try {
        topic_type = declare_parameter<std::string>(prefix + ".type");
      } catch (const rclcpp::ParameterTypeException & ex) {
        if (std::string{ex.what()}.find("not set") == std::string::npos) {
          RCLCPP_ERROR(get_logger(), "Topic type must be a string.");
        } else {
          RCLCPP_ERROR(get_logger(), "Topic %s is missing a type.", topic.c_str());
        }

        throw ex;
      }

      TopicDetails details{};
      details.name = topic;
      details.type = topic_type;
      opts.topics_.push_back(details);
    }
  }

  try {
    opts.filename_ = declare_parameter<std::string>(std::string("filename"));
  } catch (const rclcpp::ParameterTypeException & ex) {
    if (opts.action_ == SnapshotterClientOptions::TRIGGER_WRITE &&
      std::string{ex.what()}.find("not set") == std::string::npos)
    {
      RCLCPP_ERROR(get_logger(), "filename must be a string.");
      throw ex;
    }
  }

  try {
    opts.prefix_ = declare_parameter<std::string>(std::string("prefix"));
  } catch (const rclcpp::ParameterTypeException & ex) {
    if (opts.action_ == SnapshotterClientOptions::TRIGGER_WRITE &&
      std::string{ex.what()}.find("not set") == std::string::npos)
    {
      RCLCPP_ERROR(get_logger(), "prefix must be a string.");
      throw ex;
    }
  }

  if (opts.action_ == SnapshotterClientOptions::TRIGGER_WRITE && opts.topics_.size() == 0) {
    RCLCPP_INFO(get_logger(), "No topics provided - logging all topics.");
    RCLCPP_WARN(get_logger(), "Logging all topics is very memory-intensive.");
  }

  setSnapshotterClientOptions(opts);
}

void SnapshotterClient::setSnapshotterClientOptions(const SnapshotterClientOptions & opts)
{
  if (opts.action_ == SnapshotterClientOptions::TRIGGER_WRITE) {
    auto client = create_client<TriggerSnapshot>("trigger_snapshot");
    if (!client->service_is_ready()) {
      throw std::runtime_error{
              "Service trigger_snapshot is not ready. "
              "Is snapshot running in this namespace?"
      };
    }

    TriggerSnapshot::Request::SharedPtr req;

    for (const auto & topic : opts.topics_) {
      req->topics.push_back(topic.asMessage());
    }

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
    fs::path p(fs::absolute(req->filename));
    req->filename = p.string();

    auto result_future = client->async_send_request(req);
    auto future_result =
      rclcpp::spin_until_future_complete(this->get_node_base_interface(), result_future);

    if (future_result == rclcpp::FutureReturnCode::SUCCESS) {
      RCLCPP_ERROR(get_logger(), "Calling the service failed.");
    } else {
      auto result = result_future.get();
      RCLCPP_INFO(
        get_logger(),
        "Service returned: [%s] %s",
        (result->success ? "SUCCESS" : "FAILURE"),
        result->message.c_str()
      );
    }

    return;
  } else if (  // NOLINT
    opts.action_ == SnapshotterClientOptions::PAUSE ||
    opts.action_ == SnapshotterClientOptions::RESUME)
  {
    auto client = create_client<SetBool>("enable_snapshot");
    if (!client->service_is_ready()) {
      throw std::runtime_error{
              "Service enable_snapshot does not exist. "
              "Is snapshot running in this namespace?"
      };
    }

    SetBool::Request::SharedPtr req;
    req->data = (opts.action_ == SnapshotterClientOptions::RESUME);

    auto result_future = client->async_send_request(req);
    auto future_result =
      rclcpp::spin_until_future_complete(this->get_node_base_interface(), result_future);

    if (future_result == rclcpp::FutureReturnCode::SUCCESS) {
      RCLCPP_ERROR(get_logger(), "Calling the service failed.");
    } else {
      auto result = result_future.get();
      RCLCPP_INFO(
        get_logger(),
        "Service returned: [%s] %s",
        (result->success ? "SUCCESS" : "FAILURE"),
        result->message.c_str()
      );
    }

    return;
  } else {
    throw std::runtime_error{"Invalid options received."};
  }
}

}  // namespace rosbag2_snapshot

#include <rclcpp_components/register_node_macro.hpp>  // NOLINT
RCLCPP_COMPONENTS_REGISTER_NODE(rosbag2_snapshot::Snapshotter)
RCLCPP_COMPONENTS_REGISTER_NODE(rosbag2_snapshot::SnapshotterClient)
