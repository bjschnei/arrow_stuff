// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

#include <arrow/api.h>
#include <arrow/compute/api.h>
#include <arrow/status.h>

#include <array>
#include <iostream>
#include <span>

struct Data {
    std::string_view description;
    uint32_t priority;
    uint32_t price;
};

constexpr std::array<Data, 5> kTable1 = {
    Data{"obj1", 1, 10}, Data{"obj2", 1, 15}, Data{"obj3", 3, 20},
    Data{"obj4", 3, 25}, Data{"obj5", 7, 30}};

template <typename T>
// todo: make a span or generic iterable
arrow::Result<std::shared_ptr<arrow::Table>> CreateTable(const T& rows) {
    arrow::MemoryPool* pool = arrow::default_memory_pool();
    arrow::StringBuilder description_builder(pool);
    arrow::UInt32Builder priority_builder(pool);
    arrow::UInt32Builder price_builder(pool);

    // Append row values to each of the builders
    for (const auto& row : rows) {
        ARROW_RETURN_NOT_OK(description_builder.Append(row.description));
        ARROW_RETURN_NOT_OK(priority_builder.Append(row.priority));
        ARROW_RETURN_NOT_OK(price_builder.Append(row.price));
    }

    // Convert the builders to arrow arrays.
    std::shared_ptr<arrow::Array> description_array;
    ARROW_RETURN_NOT_OK(description_builder.Finish(&description_array));
    std::shared_ptr<arrow::Array> priority_array;
    ARROW_RETURN_NOT_OK(priority_builder.Finish(&priority_array));
    std::shared_ptr<arrow::Array> price_array;
    ARROW_RETURN_NOT_OK(price_builder.Finish(&price_array));

    // Define our schema for each column and their types
    std::vector<std::shared_ptr<arrow::Field>> schema_vector = {
        arrow::field("description", arrow::utf8()),
        arrow::field("priority", arrow::uint32()),
        arrow::field("price", arrow::uint32())};

    auto schema = std::make_shared<arrow::Schema>(schema_vector);

    // Create a table with the schema, and columns we've built.
    std::shared_ptr<arrow::Table> table = arrow::Table::Make(
        schema, {description_array, priority_array, price_array});

    return table;
}

// Aggregate the Price column.
arrow::Result<uint64_t> SumPrice(const arrow::Table& table) {
    ARROW_ASSIGN_OR_RAISE(
        auto sum, arrow::compute::Sum({table.GetColumnByName("price")}));
    return sum.scalar_as<arrow::UInt64Scalar>().value;
}

arrow::Result<std::shared_ptr<arrow::RecordBatch>> SumGroupByPriority(
    const arrow::Table& table) {
    ARROW_ASSIGN_OR_RAISE(auto record_batch, table.CombineChunksToBatch());
    std::vector<arrow::Datum> priority_keys = {
        record_batch->GetColumnByName("priority")};
    ARROW_ASSIGN_OR_RAISE(
        auto batch, arrow::compute::ExecBatch::Make(std::move(priority_keys)));
    ARROW_ASSIGN_OR_RAISE(auto grouper,
                          arrow::compute::Grouper::Make(batch.GetTypes()));
    ARROW_ASSIGN_OR_RAISE(arrow::Datum id_batch,
                          grouper->Consume(arrow::compute::ExecSpan(batch)));
    ARROW_ASSIGN_OR_RAISE(
        auto groupings,
        arrow::compute::Grouper::MakeGroupings(
            *id_batch.array_as<arrow::UInt32Array>(), grouper->num_groups()));
    arrow::Datum price_datum = {record_batch->GetColumnByName("price")};
    ARROW_ASSIGN_OR_RAISE(auto groups,
                          arrow::compute::Grouper::ApplyGroupings(
                              *groupings, *price_datum.make_array()));

    // Create table of priority and prices
    ARROW_ASSIGN_OR_RAISE(auto uniques, grouper->GetUniques());
    std::vector<std::shared_ptr<arrow::Field>> schema_vector = {
        arrow::field("priority", arrow::uint32())};
    auto schema = std::make_shared<arrow::Schema>(schema_vector);
    ARROW_ASSIGN_OR_RAISE(auto priority_batch, uniques.ToRecordBatch(schema));
    arrow::MemoryPool* pool = arrow::default_memory_pool();
    arrow::UInt64Builder price_builder(pool);
    for (auto i = 0; i < groups->length(); i++) {
        const auto& group = groups->value_slice(i);
        if (!group->length()) {
            continue;
        }
        ARROW_ASSIGN_OR_RAISE(auto sum, arrow::compute::Sum(group));
        ARROW_RETURN_NOT_OK(price_builder.Append(sum.scalar_as<arrow::UInt64Scalar>().value));
    }
    std::shared_ptr<arrow::Array> price_array;
    ARROW_RETURN_NOT_OK(price_builder.Finish(&price_array));
    std::vector<std::shared_ptr<arrow::Field>> result_schema_vector = {
        arrow::field("priority", arrow::uint32()),
        arrow::field("price", arrow::uint64())};
    auto result_schema = std::make_shared<arrow::Schema>(result_schema_vector);
    auto result_batch = arrow::RecordBatch::Make(result_schema, priority_batch->num_rows(), {
        priority_batch->column(0),
        price_array});
    return result_batch;
}

int main(int argc, char** argv) {
    auto t1 = CreateTable(kTable1);
    if (!t1.ok()) {
        std::cout << "failed to make table with status: " << t1.status()
                  << std::endl;
        return 1;
    }
    std::cout << "Created table successfully with row count: "
              << (*t1)->num_rows() << std::endl;

    auto s = SumPrice(**t1);
    if (!s.ok()) {
        std::cout << "failed to compute price sum: " << s.status() << std::endl;
        return 1;
    }
    std::cout << "Sum of the prices is: " << *s << std::endl;
    auto group_by_result = SumGroupByPriority(**t1);
    if (!group_by_result.ok()) {
        std::cout << "failed to sum by priority: " << group_by_result.status()
                  << std::endl;
    }
    std::cout<<(*group_by_result)->ToString()<<std::endl;
    return 0;
}