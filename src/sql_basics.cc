#include <iostream>

#include "absl/strings/str_cat.h"
#include "arrow/api.h"
#include "duckdb.hpp"

/**
 * Demonstrate executing a sql query an arrow table.
*/
constexpr std::string_view kCsvTable = R"csv(
description,priority,price
"obj1",1,10
"obj2",1,15
"obj3",3,20
"obj4",3,25
"obj5",7,30
)csv";

// Do some of the basics
arrow::Result<std::string> ToSql(const arrow::DataType& type) {
  switch (type.id()) {
    case arrow::Type::BOOL:
      return "BOOL";
    case arrow::Type::INT8:
      return "TINYINT";
    case arrow::Type::INT16:
      return "SMALLINT";
    case arrow::Type::INT32:
      return "INTEGER";
    case arrow::Type::INT64:
      return "BIGINT";
    case arrow::Type::UINT8:
      return "UTINYINT";
    case arrow::Type::UINT16:
      return "USMALLINT";
    case arrow::Type::UINT32:
      return "UINTEGER";
    case arrow::Type::UINT64:
      return "UBIGINT";
    case arrow::Type::FLOAT:
      return "FLOAT";
    case arrow::Type::DOUBLE:
      return "DOUBLE";
    case arrow::Type::STRING:
      return "VARCHAR(255)";
  }
  return arrow::Status::Invalid(absl::StrCat("Unknown type ", type.id()));
}

arrow::Result<std::string> ToSql(std::string_view name,
                                 const arrow::Schema& schema) {
  std::string head_statement = absl::StrCat("CREATE TABLE ", name, "(");
  std::string body_statement;
  for (const auto& field : schema.fields()) {
    auto field_name = field->name();
    auto field_type = field->type();
    if (!body_statement.empty()) {
      body_statement += ", ";
    }
    ARROW_ASSIGN_OR_RAISE(auto type_str, ToSql(*field_type));
    absl::StrAppend(&body_statement, field_name, " ", type_str);
  }
  return absl::StrCat(head_statement, body_statement, ")");
}

std::shared_ptr<arrow::Schema> MakeTestSchema() {
  std::vector<std::shared_ptr<arrow::Field>> schema_vector = {
      arrow::field("description", arrow::utf8()),
      arrow::field("priority", arrow::uint32()),
      arrow::field("price", arrow::uint32())};
  return std::make_shared<arrow::Schema>(schema_vector);
}

int main(int argc, char** argv) {
  duckdb::DuckDB db(nullptr);
  duckdb::Connection con(db);
  con.Query("INSTALL substrait");
  con.Query("LOAD substrait");
  auto schema = MakeTestSchema();
  auto create_sql = ToSql("mytable", *schema);
  if (!create_sql.ok()) {
    std::cout << "failed to create sql: " << create_sql.status() << std::endl;
    return 1;
  }
  std::cout << *create_sql << std::endl;

  std::string result;
  try {
    con.Query(*create_sql);
    result = con.GetSubstraitJSON(
        "SELECT sum(price) as total_price FROM mytable group by priority");
  } catch (duckdb::Exception e) {
    std::cout << "caught exception " << e.what() << std::endl;
    return 1;
  }
  std::cout << result << std::endl;
  return 0;
}