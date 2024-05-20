#include <iostream>

#include "absl/container/flat_hash_map.h"
#include "absl/flags/flag.h"
#include "absl/flags/parse.h"
#include "absl/strings/ascii.h"
#include "absl/strings/match.h"
#include "absl/strings/str_cat.h"
#include "arrow/api.h"
#include "arrow/builder.h"
#include "arrow/type_fwd.h"
#include "sqlite3.h"

ABSL_FLAG(std::string, sql, "select sku, price, category from Products",
          "query to run");

/**
 * Simplified version of what arrow/flight/sql/examples sqlite_statement and
 * sqlite_statement_batch_reader does.  Just loads results into a single
 * in-memory table. For a stream of record batches, use the
 * sqlite_statement_batch_reader.
 */

arrow::Status BuildString(arrow::StringBuilder* builder, sqlite3_stmt* stmt,
                          int column) {
  const int bytes = sqlite3_column_bytes(stmt, column);
  const uint8_t* string =
      reinterpret_cast<const uint8_t*>(sqlite3_column_text(stmt, column));
  if (string == nullptr) {
    return builder->AppendNull();
  }
  return builder->Append(string, bytes);
}

arrow::Status BuildBinary(arrow::BinaryBuilder* builder, sqlite3_stmt* stmt,
                          int column) {
  const int bytes = sqlite3_column_bytes(stmt, column);
  const uint8_t* blob =
      reinterpret_cast<const uint8_t*>(sqlite3_column_blob(stmt, column));
  if (blob == nullptr) {
    return builder->AppendNull();
  }
  return builder->Append(blob, bytes);
}

arrow::Status BuildInt64(arrow::Int64Builder* builder, sqlite3_stmt* stmt,
                         int column) {
  const sqlite3_int64 value = sqlite3_column_int64(stmt, column);
  return builder->Append(static_cast<int64_t>(value));
}

arrow::Status BuildFloat(arrow::FloatBuilder* builder, sqlite3_stmt* stmt,
                         int column) {
  const double value = sqlite3_column_double(stmt, column);
  return builder->Append(static_cast<float>(value));
}

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

arrow::Result<std::shared_ptr<arrow::Schema>> GetSchema(sqlite3_stmt* stmt) {
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
  return std::make_shared<arrow::Schema>(schema_vector);
}

// Simplified from sqlite_statement_batch_reader
// get all the columns in a single batch.
arrow::Result<std::shared_ptr<arrow::Table>> ReadData(
    sqlite3_stmt* stmt, std::shared_ptr<arrow::Schema> schema) {
  const int num_fields = schema->num_fields();
  std::vector<std::unique_ptr<arrow::ArrayBuilder>> builders(num_fields);
  for (int i = 0; i < num_fields; i++) {
    const std::shared_ptr<arrow::Field>& field = schema->field(i);
    const std::shared_ptr<arrow::DataType>& field_type = field->type();
    ARROW_RETURN_NOT_OK(arrow::MakeBuilder(arrow::default_memory_pool(),
                                           field_type, &builders[i]));
  }
  int rc = SQLITE_ROW;
  while (rc == SQLITE_ROW) {
    for (int i = 0; i < num_fields; i++) {
      const std::shared_ptr<arrow::Field>& field = schema->field(i);
      const std::shared_ptr<arrow::DataType>& field_type = field->type();
      arrow::ArrayBuilder* array_builder = builders[i].get();

      if (sqlite3_column_type(stmt, i) == SQLITE_NULL) {
        ARROW_RETURN_NOT_OK(array_builder->AppendNull());
        continue;
      }

      switch (field_type->id()) {
        case arrow::Int64Type::type_id:
          ARROW_RETURN_NOT_OK(BuildInt64(
              reinterpret_cast<arrow::Int64Builder*>(array_builder), stmt, i));
          break;
        case arrow::FloatType::type_id:
          ARROW_RETURN_NOT_OK(BuildFloat(
              reinterpret_cast<arrow::FloatBuilder*>(array_builder), stmt, i));
          break;
        case arrow::BinaryType::type_id:
          ARROW_RETURN_NOT_OK(BuildBinary(
              reinterpret_cast<arrow::BinaryBuilder*>(array_builder), stmt, i));
          break;
        case arrow::StringType::type_id:
          ARROW_RETURN_NOT_OK(BuildString(
              reinterpret_cast<arrow::StringBuilder*>(array_builder), stmt, i));
          break;
        default:
          return arrow::Status::NotImplemented(
              "Not implemented SQLite data conversion to ", field_type->name());
      }
    }
    rc = sqlite3_step(stmt);
  }
  if (rc != SQLITE_DONE) {
    return arrow::Status::Invalid("Reading result failed");
  }
  std::vector<std::shared_ptr<arrow::Array>> arrays(builders.size());
  for (int i = 0; i < num_fields; i++) {
    ARROW_RETURN_NOT_OK(builders[i]->Finish(&arrays[i]));
  }
  return arrow::Table::Make(schema, arrays);
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
  std::cout << schema->ToString() << std::endl;
  return ReadData(stmt, schema);
}

int main(int argc, char** argv) {
  absl::ParseCommandLine(argc, argv);
  sqlite3* db = CreateDbAndTable();
  if (!db) {
    return 1;
  }
  auto rv = RunQuery(db, absl::GetFlag(FLAGS_sql));
  if (!rv.ok()) {
    std::cout << "Got error " << rv.status() << std::endl;
    return 1;
  }
  std::cout << (*rv)->ToString() << std::endl;
  return 0;
}