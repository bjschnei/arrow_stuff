#include <iostream>

#include "absl/container/flat_hash_map.h"
#include "absl/flags/flag.h"
#include "absl/strings/ascii.h"
#include "absl/strings/str_cat.h"
#include "arrow/acero/api.h"
#include "arrow/acero/util.h"
#include "arrow/api.h"
#include "arrow/csv/api.h"
#include "arrow/engine/api.h"
#include "arrow/flight/server.h"
#include "arrow/io/api.h"

ABSL_FLAG(std::string, data_dir, "data",
          "Directory holding shard table data in CSV format");
ABSL_FLAG(uint16_t, shard_num, 0,
          "Shard identifier, indicating which data is loaded");
ABSL_FLAG(std::string, table_name, "Products",
          "Shard identifier, indicating which data is loaded");
ABSL_FLAG(std::string, grpc_location, "grpc://localhost:12345",
          "Where this server/shard binds/listens.");

struct TableWithName {
  std::string name;
  std::shared_ptr<arrow::Table> table;
};

arrow::Result<TableWithName> ReadTableShardData(std::string_view data_dir,
                                                std::string_view table_name,
                                                uint16_t shard_num) {
  std::string lower_table_name = absl::AsciiStrToLower(table_name);
  const std::string path =
      absl::StrCat(data_dir, "/", lower_table_name, "_", shard_num, ".csv");
  ARROW_ASSIGN_OR_RAISE(auto csv_file, arrow::io::ReadableFile::Open(path));
  arrow::io::IOContext context;
  ARROW_ASSIGN_OR_RAISE(
      auto reader, arrow::csv::TableReader::Make({}, csv_file, {}, {}, {}));
  ARROW_ASSIGN_OR_RAISE(auto table, reader->Read());
  return TableWithName{std::move(lower_table_name), table};
}

// TODO: Refactor.  Copied from fligh_server_basics.
arrow::acero::Declaration CreateTableSource(
    std::shared_ptr<arrow::Table> table) {
  auto table_source_options = arrow::acero::TableSourceNodeOptions(table);
  arrow::acero::Declaration source{"table_source",
                                   std::move(table_source_options)};
  return source;
}

class ShardedTableServer : public arrow::flight::FlightServerBase {
 public:
  explicit ShardedTableServer(
      absl::flat_hash_map<std::string, std::shared_ptr<arrow::Table>>
          named_tables)
      : arrow::flight::FlightServerBase(),
        named_tables_(std::move(named_tables)) {}

  arrow::Status DoGet(
      const arrow::flight::ServerCallContext&,
      const arrow::flight::Ticket& request,
      std::unique_ptr<arrow::flight::FlightDataStream>* stream) override {
    arrow::Buffer buf(request.ticket);
    arrow::engine::ConversionOptions conversion_options;
    conversion_options.named_table_provider =
        [this](const std::vector<std::string>& names, const arrow::Schema& s)
        -> arrow::Result<arrow::acero::Declaration> {
      for (const auto& name : names) {
        const auto lower_name = absl::AsciiStrToLower(name);
        if (const auto it = named_tables_.find(lower_name);
            it != named_tables_.end()) {
          return CreateTableSource(it->second);
        }
      }
      return arrow::Status::KeyError("Unable to find table");
    };
    ARROW_ASSIGN_OR_RAISE(auto sink_decls,
                          arrow::engine::DeserializePlans(
                              buf, [this] { return null_consumer_; }, nullptr,
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
  absl::flat_hash_map<std::string, std::shared_ptr<arrow::Table>> named_tables_;
  const std::shared_ptr<arrow::acero::NullSinkNodeConsumer> null_consumer_ =
      std::make_shared<arrow::acero::NullSinkNodeConsumer>();
};

arrow::Status StartServer() {
  ARROW_ASSIGN_OR_RAISE(
      arrow::flight::Location location,
      arrow::flight::Location::Parse(absl::GetFlag(FLAGS_grpc_location)));
  arrow::flight::FlightServerOptions options(location);
  ARROW_ASSIGN_OR_RAISE(auto table_with_name,
                        ReadTableShardData(absl::GetFlag(FLAGS_data_dir),
                                           absl::GetFlag(FLAGS_table_name),
                                           absl::GetFlag(FLAGS_shard_num)));
  std::cout << table_with_name.table->ToString() << std::endl;
  /*std::unique_ptr<arrow::flight::FlightServerBase> server =
      std::make_unique<QueryTableServer>(table);
  ARROW_RETURN_NOT_OK(server->Init(options));
  ARROW_RETURN_NOT_OK(server->SetShutdownOnSignals({SIGTERM}));
  std::cout << "Listening on " << location.ToString() << std::endl;
  ARROW_RETURN_NOT_OK(server->Serve());
  */
  return arrow::Status::OK();
}

int main(int argc, char** argv) {
  auto status = StartServer();
  if (!status.ok()) {
    std::cerr << "Failed with status: " << status << std::endl;
    return 1;
  }
  return 0;
}