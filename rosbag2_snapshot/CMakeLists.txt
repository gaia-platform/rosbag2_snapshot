cmake_minimum_required(VERSION 3.5)
project(rosbag2_snapshot)

find_package(ament_cmake_auto REQUIRED)
ament_auto_find_build_dependencies()

ament_auto_add_library(${PROJECT_NAME}
# src/snapshot.cpp
  src/snapshotter.cpp
)

rclcpp_components_register_node(snapshotter
  EXECUTABLE snapshotter
  PLUGIN rosbag2_snapshot::Snapshotter
)

rclcpp_components_register_node(snapshotter_client
  EXECUTABLE snapshotter_client
  PLUGIN rosbag2_snapshot::SnapshotterClient
)

if(BUILD_TESTING)
  find_package(ament_lint_auto REQUIRED)
  ament_lint_auto_find_test_dependencies()
endif()

ament_auto_package()
