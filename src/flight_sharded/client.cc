#include "arrow/flight/client.h"

#include "arrow/api.h"
#include "arrow/engine/api.h"

class ShardedNamedTableProvider {
 public:
  explicit ShardedNamedTableProvider(
      std::vector<std::unique_ptr<arrow::flight::FlightClient>> clients)
      : clients_(std::move(clients)) {}

  arrow::Result<arrow::acero::Declaration> operator()(
      const std::vector<std::string>& names, const arrow::Schema& schema) {
    // TODO: Figure out how to add filters and projections to the substrait
    ARROW_ASSIGN_OR_RAISE(const std::string named_table_substrait,
                          CreateSubstrait(CreateTableDecl(names, schema)));
    arrow::acero::Declaration union_decl{"union",
                                         arrow::acero::ExecNodeOptions{}};
    const arrow::flight::Ticket ticket{std::move(named_table_substrait)};
    for (const auto& client : clients_) {
      ARROW_ASSIGN_OR_RAISE(auto flight_stream_reader, client->DoGet(ticket));
      // TODO: Should actually use RecordBatchReader instead of
      // Reading in the entire result into memory right here.
      ARROW_ASSIGN_OR_RAISE(auto table, flight_stream_reader->ToTable());
      arrow::acero::Declaration table_source(
          "table_source", arrow::acero::TableSourceNodeOptions(table));
      union_decl.inputs.push_back(std::move(table_source));
    }
    return union_decl;
  }

 private:
  arrow::acero::Declaration CreateTableDecl(std::vector<std::string> names,
                                            const arrow::Schema& schema) {
    auto table_source_options = arrow::acero::NamedTableNodeOptions(
        std::move(names), std::make_shared<arrow::Schema>(schema));
    arrow::acero::Declaration source{"named_table",
                                     std::move(table_source_options)};
    return source;
  }

  arrow::Result<std::string> CreateSubstrait(
      const arrow::acero::Declaration& declaration) {
    arrow::engine::ExtensionSet empty_extension_set;
    ARROW_ASSIGN_OR_RAISE(
        std::shared_ptr<arrow::Buffer> plan_substrait,
        arrow::engine::SerializePlan(declaration, &empty_extension_set));
    return plan_substrait->ToString();
  }

  std::vector<std::unique_ptr<arrow::flight::FlightClient>> clients_;
};

int main(int argc, char** argv) { return 0; }