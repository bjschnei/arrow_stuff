#include <iostream>

#include "absl/container/flat_hash_map.h"
#include "absl/strings/ascii.h"
#include "absl/strings/match.h"
#include "absl/strings/str_cat.h"
#include "arrow/api.h"
#include "arrow/type_fwd.h"
#include "sqlite3.h"

absl::flat_hash_map<uint8_t, arrow::Type::type> kSqliteToArrowTypes{};

int InsertRowToTable(sqlite3* db, std::string_view sku, int price,
                     std::string_view category) {
  std::string sql =
      absl::StrCat("INSERT INTO Products (sku, price, category) VALUES(\"", sku,
                   "\",", price, ",\"", category, "\")");
  return sqlite3_exec(db, sql.data(), nullptr, 0, nullptr);
}

sqlite3* CreateDbAndTable() {
  sqlite3* db;

  // Open in-memory database
  if (const int rc = sqlite3_open(":memory:", &db); rc != SQLITE_OK) {
    std::cerr << "Can't open database: " << sqlite3_errmsg(db) << std::endl;
    return nullptr;
  }
  // Create a table
  constexpr std::string_view sql_create_table =
      "CREATE TABLE Products (sku VARCHAR(10), price INT, category "
      "VARCHAR(10));";
  if (const int rc =
          sqlite3_exec(db, sql_create_table.data(), nullptr, 0, nullptr);
      rc != SQLITE_OK) {
    std::cerr << "SQL error: " << sqlite3_errmsg(db) << std::endl;
    sqlite3_close(db);
    return nullptr;
  }
  std::vector<std::tuple<std::string, int, std::string>> rows = {
      {"SKU1", 1, "food"},
      {"SKU2", 2, "clothes"},
  };
  for (const auto [sku, price, category] : rows) {
    if (auto rc = InsertRowToTable(db, sku, price, category); rc != SQLITE_OK) {
      std::cerr << "SQL error: " << sqlite3_errmsg(db) << std::endl;
      sqlite3_close(db);
      return nullptr;
    }
  }
  return db;
}

arrow::Result<std::shared_ptr<arrow::DataType>> GetArrowType(
    std::string_view sqlite_type) {
  if (sqlite_type.empty()) {
    // SQLite may not know the column type yet.
    return arrow::null();
  }
  std::string lower_type = absl::AsciiStrToLower(sqlite_type);

  if (lower_type == "int" || lower_type == "integer") {
    return arrow::int64();
  } else if (lower_type == "real") {
    return arrow::float64();
  } else if (lower_type == "blob") {
    return arrow::binary();
  } else if (lower_type == "text" || lower_type == "date" ||
             absl::StartsWith(lower_type, "char") ||
             absl::StartsWith(lower_type, "varchar")) {
    return arrow::utf8();
  }
  return arrow::Status::Invalid("Invalid SQLite type: ", sqlite_type);
}

arrow::Result<std::shared_ptr<arrow::DataType>> GetArrowType(int data_type) {
  switch (data_type) {
    case SQLITE_INTEGER:
      return arrow::int64();
    case SQLITE_FLOAT:
      return arrow::float64();
    case SQLITE_BLOB:
      return arrow::binary();
    case SQLITE_NULL:
      return arrow::null();
    case SQLITE3_TEXT:
      return arrow::utf8();
    default:
      return arrow::Status::Invalid(
          absl::StrCat("unknown data type: ", data_type));
  }
}

std::shared_ptr<arrow::DataType> GetUnknownColumnDataType() {
  return arrow::dense_union({
      arrow::field("string", arrow::utf8()),
      arrow::field("bytes", arrow::binary()),
      arrow::field("bigint", arrow::int64()),
      arrow::field("double", arrow::float64()),
  });
}

arrow::Result<arrow::Schema> GetSchema(sqlite3_stmt* stmt) {
  const int num_columns = sqlite3_column_count(stmt);
  if (num_columns == 0) {
    return arrow::Status::Invalid("No table returned");
  }
  std::vector<std::shared_ptr<arrow::Field>> schema_vector;
  for (int i = 0; i < num_columns; i++) {
    const auto data_type = sqlite3_column_type(stmt, i);
    std::string name = sqlite3_column_name(stmt, i);
    auto arrow_type = GetArrowType(data_type);

    if (!arrow_type.ok()) {
      // Do what they do in flight/sql/example/sqlite_statement.cc
      // Try to retrieve column type from sqlite3_column_decltype
      const char* column_decltype = sqlite3_column_decltype(stmt, i);
      if (column_decltype == NULLPTR) {
        schema_vector.push_back(arrow::field(name, GetUnknownColumnDataType()));
        continue;
      }
      arrow_type = GetArrowType(column_decltype);
      if (!arrow_type.ok()) {
        return arrow_type.status();
      }
    }
    schema_vector.push_back(arrow::field(name, *arrow_type));
  }
  return arrow::Schema(schema_vector);
}

arrow::Status ReadData(sqlite3_stmt* stmt, const arrow::Schema& schema) {
  const int num_fields = schema.num_fields();
  std::vector<std::unique_ptr<arrow::ArrayBuilder>> builders(num_fields);
  for (int i = 0; i < num_fields; i++) {
    const std::shared_ptr<arrow::Field>& field = schema.field(i);
    const std::shared_ptr<arrow::DataType>& field_type = field->type();
    ARROW_RETURN_NOT_OK(arrow::MakeBuilder(arrow::default_memory_pool(), field_type, &builders[i]));
  }
  return arrow::Status::Invalid("unimplemented");
}

arrow::Result<std::shared_ptr<arrow::Table>> RunQuery(sqlite3* db,
                                                      std::string_view query) {
  sqlite3_stmt* stmt;
  if (int rc = sqlite3_prepare(db, query.data(), query.size(), &stmt, nullptr);
      rc != SQLITE_OK) {
    return arrow::Status::Invalid(absl::StrCat("Invalid statement: ", query));
  }
  if (auto result = sqlite3_step(stmt); result != SQLITE_ROW) {
    return arrow::Status::Invalid(
        absl::StrCat("Expected row result, got: ", result));
  }

  ARROW_ASSIGN_OR_RAISE(auto schema, GetSchema(stmt));
  std::cout << schema.ToString() << std::endl;
  return arrow::Status::Invalid("unimplemented");
}

int main(int argc, char** argv) {
  sqlite3* db = CreateDbAndTable();
  if (!db) {
    return 1;
  }
  auto rv = RunQuery(db, "select * from Products;");
  if (!rv.ok()) {
    std::cout << "Got error " << rv.status() << std::endl;
    return 1;
  }
  return 0;
}