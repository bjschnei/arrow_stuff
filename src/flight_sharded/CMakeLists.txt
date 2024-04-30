#Include FetchContent module
include(FetchContent)
set(ABSL_PROPAGATE_CXX_STD ON)

set(BUILD_TESTING OFF) # don't test absl
# Absl
# Must match the version used by the statically linked arrow
# in order to avoid duplicate definition linker errors.
FetchContent_Declare(
  absl
  GIT_REPOSITORY "https://github.com/abseil/abseil-cpp.git"
  GIT_TAG 20211102.0)  
FetchContent_MakeAvailable(absl)

add_executable(flight_server_shard flight_server_shard.cc)
target_link_libraries(flight_server_shard PRIVATE absl::flags 
                                                  absl::strings
                                                  Arrow::arrow_static 
                                                  ArrowDataset::arrow_dataset_static
                                                  ArrowFlight::arrow_flight_static
                                                  ArrowSubstrait::arrow_substrait_static)


file(COPY data DESTINATION ${CMAKE_CURRENT_BINARY_DIR})
