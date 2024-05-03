
#include "arrow/flight/client.h"

#include <iostream>
#include <span>

#include "absl/container/flat_hash_map.h"
#include "absl/flags/flag.h"
#include "absl/flags/parse.h"
#include "arrow/acero/util.h"
#include "arrow/api.h"
#include "arrow/engine/api.h"

ABSL_FLAG(std::vector<std::string>, shard_locations,
          std::vector<std::string>({"grpc://localhost:12345"}),
          "comma-separated list of shard locations");

// Lazy convert ticket to a RecordBatchReader on first iteration
class ShardedFlightRecordBatchReader : public arrow::RecordBatchReader {
 public:
  ShardedFlightRecordBatchReader(arrow::flight::FlightClient& client,
                                 std::shared_ptr<arrow::Schema> schema,
                                 arrow::flight::Ticket ticket)
      : RecordBatchReader(),
        client_(client),
        schema_(std::move(schema)),
        ticket_(std::move(ticket)) {}

  arrow::Status ReadNext(
      std::shared_ptr<arrow::RecordBatch>* batch) override {
    if (!reader_) {
      RETURN_NOT_OK(InitReader());
    }
    return reader_->ReadNext(batch);
  }

  std::shared_ptr<arrow::Schema> schema() const override {
    return schema_;
  }

 private:
  arrow::Status InitReader() {
    ARROW_ASSIGN_OR_RAISE(auto flight_stream_reader, client_.DoGet(ticket_));
    std::shared_ptr<arrow::flight::FlightStreamReader> shared_reader =
        std::move(flight_stream_reader);
    ARROW_ASSIGN_OR_RAISE(reader_,
                          arrow::flight::MakeRecordBatchReader(shared_reader));
    return arrow::Status::OK();
  }
  arrow::flight::FlightClient& client_;
  arrow::flight::Ticket ticket_;
  std::shared_ptr<arrow::RecordBatchReader> reader_;
  std::shared_ptr<arrow::Schema> schema_;
};

class ShardedNamedTableProvider {
 public:
  explicit ShardedNamedTableProvider(
      std::vector<std::unique_ptr<arrow::flight::FlightClient>> clients)
      : clients_(std::move(clients)) {}

  arrow::Result<arrow::acero::Declaration> operator()(
      const std::vector<std::string>& names,
      const arrow::Schema& schema) const {
    // TODO: Figure out how to add filters and projections to the substrait
    ARROW_ASSIGN_OR_RAISE(const std::string named_table_substrait,
                          CreateSubstrait(CreateTableDecl(names, schema)));
    arrow::acero::Declaration union_decl{"union",
                                         arrow::acero::ExecNodeOptions{}};
    const arrow::flight::Ticket ticket{std::move(named_table_substrait)};
    for (auto& client : clients_) {
      auto reader = std::make_shared<ShardedFlightRecordBatchReader>(
          *client, std::make_shared<arrow::Schema>(schema), ticket);
      arrow::acero::Declaration table_source{
          "record_batch_reader_source",
          arrow::acero::RecordBatchReaderSourceNodeOptions{reader}};
      union_decl.inputs.push_back(std::move(table_source));
    }
    return union_decl;
  }

 private:
  arrow::acero::Declaration CreateTableDecl(std::vector<std::string> names,
                                            const arrow::Schema& schema) const {
    auto table_source_options = arrow::acero::NamedTableNodeOptions(
        std::move(names), std::make_shared<arrow::Schema>(schema));
    arrow::acero::Declaration source{"named_table",
                                     std::move(table_source_options)};
    return source;
  }

  arrow::Result<std::string> CreateSubstrait(
      const arrow::acero::Declaration& declaration) const {
    arrow::engine::ExtensionSet empty_extension_set;
    ARROW_ASSIGN_OR_RAISE(
        std::shared_ptr<arrow::Buffer> plan_substrait,
        arrow::engine::SerializePlan(declaration, &empty_extension_set));
    return plan_substrait->ToString();
  }

  std::vector<std::unique_ptr<arrow::flight::FlightClient>> clients_;
};

arrow::Result<std::shared_ptr<arrow::Table>> RunSubstraitQuery(
    const ShardedNamedTableProvider& named_table_provider,
    std::string_view substrait) {
  const std::shared_ptr<arrow::acero::NullSinkNodeConsumer> null_consumer =
      std::make_shared<arrow::acero::NullSinkNodeConsumer>();
  arrow::Buffer buf(substrait);
  arrow::engine::ConversionOptions conversion_options;
  conversion_options.named_table_provider =
      [&named_table_provider](const std::vector<std::string>& names,
                              const arrow::Schema& schema)
      -> arrow::Result<arrow::acero::Declaration> {
    return named_table_provider(names, schema);
  };
  ARROW_ASSIGN_OR_RAISE(auto sink_decls,
                        arrow::engine::DeserializePlans(
                            buf, [&null_consumer] { return null_consumer; },
                            nullptr, nullptr, conversion_options));
  auto& other_declrs =
      std::get<arrow::acero::Declaration>(sink_decls[0].inputs[0]);
  return arrow::acero::DeclarationToTable(other_declrs);
}

arrow::Result<std::unique_ptr<arrow::flight::FlightClient>> CreateClient(
    std::string_view location_str) {
  ARROW_ASSIGN_OR_RAISE(
      arrow::flight::Location location,
      arrow::flight::Location::Parse(std::string(location_str)));
  return arrow::flight::FlightClient::Connect(location);
}

arrow::Result<std::vector<std::unique_ptr<arrow::flight::FlightClient>>>
CreateClients(std::span<std::string> locations) {
  std::vector<std::unique_ptr<arrow::flight::FlightClient>> clients;
  for (auto location : locations) {
    ARROW_ASSIGN_OR_RAISE(auto client, CreateClient(location));
    clients.push_back(std::move(client));
  }
  return clients;
}

arrow::Result<std::vector<std::shared_ptr<arrow::Schema>>> GetSchemas(
    arrow::flight::FlightClient& client) {
  ARROW_ASSIGN_OR_RAISE(auto flights_result, client.ListFlights())
  std::vector<std::shared_ptr<arrow::Schema>> schemas;
  while (true) {
    ARROW_ASSIGN_OR_RAISE(auto flight_info, flights_result->Next());
    if (!flight_info) {
      break;
    }
    arrow::ipc::DictionaryMemo memo;
    ARROW_ASSIGN_OR_RAISE(auto schema, flight_info->GetSchema(&memo));
    schemas.push_back(std::move(schema));
  }
  return schemas;
}

arrow::Result<absl::flat_hash_map<std::string, std::shared_ptr<arrow::Schema>>>
GetTableSchemas(
    std::span<std::unique_ptr<arrow::flight::FlightClient>> clients) {
  absl::flat_hash_map<std::string, std::shared_ptr<arrow::Schema>>
      table_schemas;
  for (auto& client : clients) {
    ARROW_ASSIGN_OR_RAISE(auto schemas, GetSchemas(*client));
    for (auto& schema : schemas) {
      ARROW_ASSIGN_OR_RAISE(auto name, schema->metadata()->Get("name"));
      table_schemas.insert({std::move(name), schema});
    }
  }
  return table_schemas;
}

arrow::Result<std::string> GetSelectAllProductsSubstrait(
    std::shared_ptr<arrow::Schema> schema) {
  auto table_source_options =
      arrow::acero::NamedTableNodeOptions({"products"}, schema);
  arrow::acero::Declaration source{"named_table",
                                   std::move(table_source_options)};
  arrow::engine::ExtensionSet empty_extension_set;
  ARROW_ASSIGN_OR_RAISE(
      std::shared_ptr<arrow::Buffer> plan_substrait,
      arrow::engine::SerializePlan(source, &empty_extension_set));
  arrow::flight::FlightEndpoint endpoint;
  return plan_substrait->ToString();
}

int main(int argc, char** argv) {
  absl::ParseCommandLine(argc, argv);
  std::vector<std::string> shard_locations =
      absl::GetFlag(FLAGS_shard_locations);
  auto clients = CreateClients(shard_locations);
  if (!clients.ok()) {
    std::cerr << "Failed to create clients: " << clients.status() << std::endl;
    return 1;
  }
  auto schemas = GetTableSchemas(*clients);
  if (!clients.ok()) {
    std::cerr << "Failed to get schemas: " << schemas.status() << std::endl;
    return 1;
  }

  auto substrait = GetSelectAllProductsSubstrait((*schemas)["products"]);
  if (!substrait.ok()) {
    std::cerr << "Failed to get substrait: " << substrait.status() << std::endl;
    return 1;
  }
  ShardedNamedTableProvider provider(std::move(*clients));
  auto table = RunSubstraitQuery(provider, *substrait);
  if (!table.ok()) {
    std::cerr << "Failed to get table: " << table.status() << std::endl;
    return 1;
  }
  std::cout << (*table)->ToString();
  return 0;
}