# rosbag2_snapshot

Solution for [this rosbag2 issue](https://github.com/ros2/rosbag2/issues/663) which acts similarly to [`rosbag_snapshot`](https://github.com/ros/rosbag2_snapshot). It is added as a new package here rather than patching `rosbag2` because that's how they did it in ROS 1 ;).

It subscribes to topics and maintains a buffer of recent messages like a dash cam. This is useful in live testing where unexpected events can occur which would be useful to have data on but the opportunity is missed if `rosbag record` was not running (disk space limits make always running `rosbag record` impracticable). Instead, users may run snapshot in the background and save data from the recent past to disk as needed.


## Usage

`rosbag2_snapshot` can be configured through ROS params for more granular control. The `snapshotter` command will run the buffer server and the `snapshotter_client` command can be used as a client to request that the server write data to disk or freeze the buffer to preserve interesting data until a user can decide what to write.

### Server

```
$ ros2 run rosbag2_snapshot snapshotter

Buffer recent messages until triggered to write or trigger an already running instance.

### Example param file
```yaml
/**:
  ros__parameters:
    default_duration_limit: 10.0           # [Optional, default=-1] Maximum time difference between newest and oldest message in seconds
    default_memory_limit: 64.0             # [Optional, default=-1] Maximum memory used by messages in each topic's buffer, in MB
    topics: ["/topic1", "/topic2"]         # [Optional] List of topics to buffer. If empty, buffer all topics.
    topic_details:
      /topic1:
        type: "sensor_msgs/msg/NavSatFix"  # [Required if topic is specified] Topic type
      /topic2:
        type: "sensor_msgs/msg/Odometry"
        duration: 15.0                     # [Optional] Override duration limit, inherit memory limit
      /topic3:
        type: "sensor_msgs/msg/Image"
        duration: 2.0                      # [Optional] Override both limits
        memory: -1                         # Negative value means no limit
```

### Client

###### Write all buffered data to `<datetime>.bag`
`ros2 run rosbag2_snapshot snapshotter_client --ros-params -p action_type:=trigger_write`

###### Write buffered data from selected topics to `new_lighting<datetime>.bag`
`ros2 run rosbag2_snapshot snapshotter_client --ros-params -p filename:=new_lighting -p topics:=["/camera/image_raw", "/camera/camera_info"]`

###### Write all buffered data to `/home/user/crashed_into_wall.bag`
`ros2 run rosbag2_snapshot snapshotter_client --ros-params -p filename:="/home/user/crashed_into_wall.bag"`

###### Pause buffering of new data, holding current buffer in memory until resumed or write is triggered
`ros2 run rosbag2_snapshot snapshotter_client --ros-params -p action_type:=pause`

###### Resume buffering new data
`ros2 run rosbag2_snapshot snapshotter_client --ros-params -p action_type:=resume`

###### Call trigger service manually

```
$ ros2 service call /trigger_snapshot rosbag2_snapshot_msgs/srv/TriggerSnapshot "{filename: '', topics: [], start_time: {sec: 0, nanosec: 0}, stop_time: {sec: 0, nanosec: 0}}"
requester: making request: rosbag2_snapshot_msgs.srv.TriggerSnapshot_Request(filename='', topics=[], start_time=builtin_interfaces.msg.Time(sec=0, nanosec=0), stop_time=builtin_interfaces.msg.Time(sec=0, nanosec=0))

response:
rosbag2_snapshot_msgs.srv.TriggerSnapshot_Response(success=True, message='')
```

###### Call pause/resume service manually

```
$ ros2 service call /enable_snapshot std_srvs/srv/SetBool "{data: false}"
requester: making request: std_srvs.srv.SetBool_Request(data=False)

response:
std_srvs.srv.SetBool_Response(success=True, message='')
```
