#include <cstdlib>
#include <iostream>
#include <optional>
#include <string_view>
#include <unordered_map>

#include "arrow/acero/api.h"
#include "arrow/acero/util.h"
#include "arrow/api.h"
#include "arrow/compute/api.h"
#include "arrow/csv/api.h"
#include "arrow/dataset/api.h"
#include "arrow/dataset/plan.h"
#include "arrow/engine/api.h"
#include "arrow/flight/server.h"
#include "arrow/io/interfaces.h"
#include "arrow/io/memory.h"

/**
 * Demonstrate a flight grpc server capable of interacting with arrow.
 */
constexpr std::string_view kCsvTable = R"csv(
description,priority,price
"obj1",1,10
"obj2",1,15
"obj3",3,20
"obj4",3,25
"obj5",7,30
)csv";

constexpr std::string_view kTableName = "table";

const auto kNullConsumer =
    std::make_shared<arrow::acero::NullSinkNodeConsumer>();

arrow::Result<std::shared_ptr<arrow::Table>> CreateTableFromCSVData(
    std::string_view csv) {
  const arrow::io::IOContext& io_context = arrow::io::default_io_context();
  std::shared_ptr<arrow::io::InputStream> input;
  input = arrow::io::BufferReader::FromString(std::string(csv));
  auto read_options = arrow::csv::ReadOptions::Defaults();
  auto parse_options = arrow::csv::ParseOptions::Defaults();
  auto convert_options = arrow::csv::ConvertOptions::Defaults();
  ARROW_ASSIGN_OR_RAISE(
      std::shared_ptr<arrow::csv::TableReader> table_reader,
      arrow::csv::TableReader::Make(io_context, input, read_options,
                                    parse_options, convert_options));

  return table_reader->Read();
}

arrow::acero::Declaration CreateNamedTableSource(
    std::shared_ptr<arrow::Table> table) {
  auto table_source_options = arrow::acero::NamedTableNodeOptions(
      {std::string(kTableName)}, table->schema());
  arrow::acero::Declaration source{"named_table",
                                   std::move(table_source_options)};
  return source;
}

arrow::acero::Declaration CreateTableSource(
    std::shared_ptr<arrow::Table> table) {
  auto table_source_options = arrow::acero::TableSourceNodeOptions(table);
  arrow::acero::Declaration source{"table_source",
                                   std::move(table_source_options)};
  return source;
}

class QueryTableServer : public arrow::flight::FlightServerBase {
 public:
  explicit QueryTableServer(std::shared_ptr<arrow::Table> table)
      : arrow::flight::FlightServerBase(), table_(std::move(table)) {}

  // Describe the table(s) we have available to query.
  arrow::Status ListFlights(
      const arrow::flight::ServerCallContext& context,
      const arrow::flight::Criteria* criteria,
      std::unique_ptr<arrow::flight::FlightListing>* listings) override {
    std::vector<arrow::flight::FlightInfo> flights;
    auto info = MakeFlightInfoForTable(CreateTableSource(table_),
                                       CreateNamedTableSource(table_));
    if (!info.ok()) {
      return info.status();
    }
    flights.push_back(std::move(*info));
    *listings = std::unique_ptr<arrow::flight::FlightListing>(
        new arrow::flight::SimpleFlightListing(std::move(flights)));
    return arrow::Status::OK();
  }

  arrow::Status GetFlightInfo(
      const arrow::flight::ServerCallContext&,
      const arrow::flight::FlightDescriptor& descriptor,
      std::unique_ptr<arrow::flight::FlightInfo>* info) override {
    ARROW_ASSIGN_OR_RAISE(
        auto flight_info,
        MakeFlightInfoForTable(CreateTableSource(table_),
                               CreateNamedTableSource(table_)));
    *info = std::make_unique<arrow::flight::FlightInfo>(std::move(flight_info));
    return arrow::Status::OK();
  }

  arrow::Status DoGet(
      const arrow::flight::ServerCallContext&,
      const arrow::flight::Ticket& request,
      std::unique_ptr<arrow::flight::FlightDataStream>* stream) override {
    arrow::Buffer buf(request.ticket);
    arrow::engine::ConversionOptions conversion_options;
    conversion_options.named_table_provider =
        [this](const std::vector<std::string>& v, const arrow::Schema& s) {
          return CreateTableSource(table_);
        };
    ARROW_ASSIGN_OR_RAISE(auto sink_decls,
                          arrow::engine::DeserializePlans(
                              buf, [] { return kNullConsumer; }, nullptr,
                              nullptr, conversion_options));
    auto& other_declrs =
        std::get<arrow::acero::Declaration>(sink_decls[0].inputs[0]);
    ARROW_ASSIGN_OR_RAISE(auto batch_reader,
                          arrow::acero::DeclarationToReader(other_declrs));
    std::shared_ptr<arrow::RecordBatchReader> shared = std::move(batch_reader);
    *stream = std::make_unique<arrow::flight::RecordBatchStream>(shared);
    return arrow::Status::OK();
  }

 private:
  arrow::Result<arrow::flight::FlightInfo> MakeFlightInfoForTable(
      arrow::acero::Declaration plan, arrow::acero::Declaration named_plan) {
    ARROW_ASSIGN_OR_RAISE(auto schema, arrow::acero::DeclarationToSchema(plan));
    arrow::engine::ExtensionSet empty_extension_set;
    ARROW_ASSIGN_OR_RAISE(
        std::shared_ptr<arrow::Buffer> plan_substrait,
        arrow::engine::SerializePlan(named_plan, &empty_extension_set));
    arrow::flight::FlightEndpoint endpoint;
    endpoint.ticket.ticket = plan_substrait->ToString();
    return arrow::flight::FlightInfo::Make(
        *schema, arrow::flight::FlightDescriptor::Command(""), {endpoint}, -1,
        -1);
  }

  std::shared_ptr<arrow::Table> table_;
};

arrow::Status StartServer() {
  constexpr std::string_view location_str = "grpc://localhost:12345";
  ARROW_ASSIGN_OR_RAISE(
      arrow::flight::Location location,
      arrow::flight::Location::Parse(std::string(location_str)));
  arrow::flight::FlightServerOptions options(location);
  ARROW_ASSIGN_OR_RAISE(auto table, CreateTableFromCSVData(kCsvTable));
  std::unique_ptr<arrow::flight::FlightServerBase> server =
      std::make_unique<QueryTableServer>(table);
  ARROW_RETURN_NOT_OK(server->Init(options));
  ARROW_RETURN_NOT_OK(server->SetShutdownOnSignals({SIGTERM}));
  std::cout << "Listening on " << location.ToString() << std::endl;
  ARROW_RETURN_NOT_OK(server->Serve());
  return arrow::Status::OK();
}

int main(int argc, char** argv) { return StartServer().ok() ? 0 : 1; }