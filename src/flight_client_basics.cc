/**
 * For use with fligh_server_basics
 */
#include <iostream>

#include "arrow/flight/client.h"

arrow::Result<std::unique_ptr<arrow::flight::FlightClient>> CreateClient() {
  constexpr std::string_view location_str = "grpc://localhost:12345";
  ARROW_ASSIGN_OR_RAISE(
      arrow::flight::Location location,
      arrow::flight::Location::Parse(std::string(location_str)));
  return arrow::flight::FlightClient::Connect(location);
}

arrow::Status ListFlights(arrow::flight::FlightClient& client) {
  ARROW_ASSIGN_OR_RAISE(auto flights_result, client.ListFlights())
  while (true) {
    ARROW_ASSIGN_OR_RAISE(auto flight_info, flights_result->Next());
    if (!flight_info) {
        break;
    }
    arrow::ipc::DictionaryMemo memo;
    ARROW_ASSIGN_OR_RAISE(auto schema, flight_info->GetSchema(&memo));
    std::cout << "schema:" << std::endl << schema->ToString() << std::endl;
  }
  return arrow::Status::OK();
}

int main(int argc, char** argv) {
  auto client = CreateClient();
  if (!client.ok()) {
    std::cout << "Failed to create client: " << client.status() << std::endl;
    return 1;
  }
  ListFlights(**client);
  return 0;
}