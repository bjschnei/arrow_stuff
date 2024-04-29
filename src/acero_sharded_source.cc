/**
 * Demonstrate acero operations over a table spread over multiple shards.
 */

#include <iostream>
#include <string_view>

#include "arrow/acero/api.h"
#include "arrow/api.h"
#include "arrow/compute/api.h"
#include "arrow/csv/api.h"
#include "arrow/dataset/api.h"
#include "arrow/dataset/plan.h"
#include "arrow/io/interfaces.h"
#include "arrow/io/memory.h"
#include "arrow/result.h"

constexpr std::string_view kCsvTable1 = R"csv(
description,priority,price
"obj1",1,10
"obj2",1,15
"obj3",3,20
"obj4",3,25
"obj5",7,30
)csv";

constexpr std::string_view kCsvTable2 = R"csv(
description,priority,price
"obj6",2,11
"obj7",2,16
"obj8",4,21
"obj9",4,26
"obj10",8,31
)csv";

// Copied from acero_basics
// TODO: move to common file.
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

arrow::Result<std::shared_ptr<arrow::RecordBatchReader>>
CreateRecordBatchReaderFromCSVs(
    const std::vector<std::string_view>& csv_shards) {
  // Notice: Any filters on the table_source should be added to the table source
  // declaration.
  arrow::acero::Declaration union_decl{"union",
                                       arrow::acero::ExecNodeOptions{}};
  for (const auto csv : csv_shards) {
    ARROW_ASSIGN_OR_RAISE(auto table, CreateTableFromCSVData(csv));
    arrow::acero::Declaration table_source(
        "table_source", arrow::acero::TableSourceNodeOptions(table));
    union_decl.inputs.push_back(std::move(table_source));
  }
  return arrow::acero::DeclarationToReader(union_decl);
}

arrow::Status QueryFullTable(
    std::shared_ptr<arrow::RecordBatchReader> record_batch_reader) {
  arrow::acero::Declaration declaration{
      "record_batch_reader_source",
      arrow::acero::RecordBatchReaderSourceNodeOptions{record_batch_reader}};
  ARROW_ASSIGN_OR_RAISE(auto table,
                        arrow::acero::DeclarationToTable(declaration));
  std::cout << table->ToString() << std::endl;
  return arrow::Status::OK();
}

arrow::Status SumPrice(
    std::shared_ptr<arrow::RecordBatchReader> record_batch_reader) {
  std::shared_ptr<arrow::dataset::ScannerBuilder> builder =
      arrow::dataset::ScannerBuilder::FromRecordBatchReader(
          record_batch_reader);
  ARROW_RETURN_NOT_OK(builder->Project({
      "price",
  }));
  ARROW_ASSIGN_OR_RAISE(auto scanner, builder->Finish());
  auto aggregate_options = arrow::acero::AggregateNodeOptions{
      /*aggregates=*/{{"sum", nullptr, "price", "sum(price)"}}};
  auto plan = arrow::acero::Declaration::Sequence(
      {{"record_batch_reader_source",
        arrow::acero::RecordBatchReaderSourceNodeOptions{record_batch_reader}},
       {"aggregate", std::move(aggregate_options)}});
  ARROW_ASSIGN_OR_RAISE(auto table,
                        arrow::acero::DeclarationToTable(std::move(plan)));
  std::cout << table->ToString() << std::endl;
  return arrow::Status::OK();
}

int main(int argc, char** argv) {
  auto record_batch_reader =
      CreateRecordBatchReaderFromCSVs({kCsvTable1, kCsvTable2});
  if (!record_batch_reader.ok()) {
    std::cout << "Failed to create record batch reader with status: "
              << record_batch_reader.status() << std::endl;
    return 1;
  }
  std::cout << "QUERY FULL TABLE" << std::endl;
  auto status = QueryFullTable(*record_batch_reader);
  if (!status.ok()) {
    std::cout << "Failed to query full table with status: " << status
              << std::endl;
    return 1;
  }
  std::cout << "SUM PRICE" << std::endl;
  record_batch_reader =
      CreateRecordBatchReaderFromCSVs({kCsvTable1, kCsvTable2});
  status = SumPrice(*record_batch_reader);
  if (!status.ok()) {
    std::cout << "Failed to sum price with status: " << status << std::endl;
    return 1;
  }
  return 0;
}
