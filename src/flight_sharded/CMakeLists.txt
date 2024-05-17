add_executable(flight_server_shard flight_server_shard.cc)
target_link_libraries(flight_server_shard PRIVATE absl::flags 
                                                  absl::flags_parse
                                                  absl::strings
                                                  Arrow::arrow_static 
                                                  ArrowDataset::arrow_dataset_static
                                                  ArrowFlight::arrow_flight_static
                                                  ArrowSubstrait::arrow_substrait_static)


file(COPY data DESTINATION ${CMAKE_CURRENT_BINARY_DIR} 
    FILE_PERMISSIONS OWNER_READ OWNER_WRITE GROUP_WRITE GROUP_READ WORLD_READ 
    DIRECTORY_PERMISSIONS OWNER_READ OWNER_WRITE OWNER_EXECUTE GROUP_WRITE GROUP_READ 
                          GROUP_EXECUTE WORLD_READ WORLD_EXECUTE
    )

add_executable(client client.cc)
target_link_libraries(client PRIVATE absl::flat_hash_map
                                     absl::flags
                                     absl::flags_parse
                                     absl::strings
                                     Arrow::arrow_static 
                                     ArrowDataset::arrow_dataset_static
                                     ArrowFlight::arrow_flight_static
                                     ArrowSubstrait::arrow_substrait_static
                                     duckdb_static
                                     )
