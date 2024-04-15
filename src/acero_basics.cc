#include <arrow/acero/api.h>
#include <arrow/api.h>
#include <arrow/compute/api.h>
#include <arrow/csv/api.h>
#include <arrow/dataset/api.h>
#include <arrow/dataset/plan.h>
#include <arrow/io/interfaces.h>
#include <arrow/io/memory.h>

#include <iostream>
#include <string_view>

constexpr std::string_view kCsvTable = R"csv(
description,priority,price
"obj1",1,10
"obj2",1,15
"obj3",3,20
"obj4",3,25
"obj5",7,30
)csv";

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

arrow::Result<arrow::acero::Declaration> CreateSumPricePlan(
    std::shared_ptr<arrow::Table> table) {
  auto dataset = std::make_shared<arrow::dataset::InMemoryDataset>(table);
  auto options = std::make_shared<arrow::dataset::ScanOptions>();
  options->projection = arrow::compute::project(
      {
          arrow::compute::field_ref("price"),
      },
      {});
  auto scan_node_options = arrow::dataset::ScanNodeOptions{dataset, options};
  auto aggregate_options = arrow::acero::AggregateNodeOptions{
      /*aggregates=*/{{"sum", nullptr, "price", "sum(price)"}}};
  auto plan = arrow::acero::Declaration::Sequence(
      {{"scan", std::move(scan_node_options)},
       {"aggregate", std::move(aggregate_options)}});
  return plan;
}

int main(int argc, char** argv) {
  arrow::dataset::internal::Initialize();
  auto table = CreateTableFromCSVData(kCsvTable);
  if (!table.ok()) {
    std::cout << "Failed to create table: " << table.status() << std::endl;
    return 1;
  }
  std::cout << (*table)->ToString() << std::endl;
  auto sum_plan = CreateSumPricePlan(*table);
  auto response_table = arrow::acero::DeclarationToTable(std::move(*sum_plan));
  if (!response_table.ok()) {
    std::cout << "Failed to compute table: " << response_table.status()
              << std::endl;
  }
  std::cout << "Results : " << (*response_table)->ToString() << std::endl;
  return 0;
}