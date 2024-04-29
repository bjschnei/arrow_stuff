cmake_minimum_required(VERSION 3.22)

project(
  QueryArrow
  DESCRIPTION "Play around with querying apache arrow"
  LANGUAGES CXX)
set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

# Include FetchContent module
include(FetchContent)

find_package(Arrow REQUIRED)
find_package(ArrowDataset REQUIRED)
find_package(ArrowFlight REQUIRED)
find_package(ArrowSubstrait REQUIRED)

# Absl
#set(ABSL_PROPAGATE_CXX_STD ON)
#FetchContent_Declare(
  #absl
  #GIT_REPOSITORY "https://github.com/abseil/abseil-cpp.git"
  #GIT_TAG 20240116.1)
#FetchContent_MakeAvailable(absl)

add_executable(table_basics table_basics.cc)
target_link_libraries(table_basics PRIVATE Arrow::arrow_static)

add_executable(acero_basics acero_basics.cc)
target_link_libraries(acero_basics PRIVATE Arrow::arrow_static 
                                           ArrowDataset::arrow_dataset_static)

add_executable(acero_sharded_source acero_sharded_source.cc)
target_link_libraries(acero_sharded_source PRIVATE Arrow::arrow_static 
                                           ArrowDataset::arrow_dataset_static)

add_executable(flight_server_basics flight_server_basics.cc)
target_link_libraries(flight_server_basics PRIVATE Arrow::arrow_static 
                                                   ArrowDataset::arrow_dataset_static
                                                   ArrowFlight::arrow_flight_static
                                                   ArrowSubstrait::arrow_substrait_static)

add_executable(flight_client_basics flight_client_basics.cc)
target_link_libraries(flight_client_basics PRIVATE Arrow::arrow_static 
                                                   ArrowDataset::arrow_dataset_static
                                                   ArrowFlight::arrow_flight_static
                                                   ArrowSubstrait::arrow_substrait_static)
