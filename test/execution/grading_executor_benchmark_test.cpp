//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// executor_benchmark_test.cpp
//
// Identification: test/execution/executor_benchmark_test.cpp
//
// Copyright (c) 2015-2019, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <chrono>  // NOLINT
#include <cstdio>
#include <future>  // NOLINT
#include <iostream>
#include <memory>
#include <string>
#include <thread>  // NOLINT
#include <unordered_set>
#include <utility>
#include <vector>
#include "execution/plans/delete_plan.h"
#include "execution/plans/limit_plan.h"
#include "include/execution/plans/nested_index_join_plan.h"

#include "buffer/buffer_pool_manager.h"
#include "catalog/table_generator.h"
#include "concurrency/transaction_manager.h"
#include "execution/execution_engine.h"
#include "execution/executor_context.h"
#include "execution/executors/aggregation_executor.h"
#include "execution/executors/insert_executor.h"
#include "execution/executors/nested_loop_join_executor.h"
#include "execution/expressions/aggregate_value_expression.h"
#include "execution/expressions/column_value_expression.h"
#include "execution/expressions/comparison_expression.h"
#include "execution/expressions/constant_value_expression.h"
#include "execution/plans/seq_scan_plan.h"
#include "gtest/gtest.h"
#include "storage/b_plus_tree_test_util.h"  // NOLINT
#include "type/value_factory.h"

// Macro for time out mechanism
#define TEST_TIMEOUT_BEGIN                           \
  std::promise<bool> promisedFinished;               \
  auto futureResult = promisedFinished.get_future(); \
                              std::thread([](std::promise<bool>& finished) {
#define TEST_TIMEOUT_FAIL_END(X)                                                                  \
  finished.set_value(true);                                                                       \
  }, std::ref(promisedFinished)).detach();                                                        \
  EXPECT_TRUE(futureResult.wait_for(std::chrono::milliseconds(X)) != std::future_status::timeout) \
      << "Test Failed Due to Time Out";

namespace bustub {

// The below helper functions are useful for testing.

const AbstractExpression *MakeColumnValueExpression(
    const Schema &schema, uint32_t tuple_idx, const std::string &col_name,
    std::vector<std::unique_ptr<AbstractExpression>> *allocated_exprs_) {
  uint32_t col_idx = schema.GetColIdx(col_name);
  auto col_type = schema.GetColumn(col_idx).GetType();
  allocated_exprs_->emplace_back(std::make_unique<ColumnValueExpression>(tuple_idx, col_idx, col_type));
  return allocated_exprs_->back().get();
}

const AbstractExpression *MakeConstantValueExpression(
    const Value &val, std::vector<std::unique_ptr<AbstractExpression>> *allocated_exprs_) {
  allocated_exprs_->emplace_back(std::make_unique<ConstantValueExpression>(val));
  return allocated_exprs_->back().get();
}

const AbstractExpression *MakeComparisonExpression(const AbstractExpression *lhs, const AbstractExpression *rhs,
                                                   ComparisonType comp_type,
                                                   std::vector<std::unique_ptr<AbstractExpression>> *allocated_exprs_) {
  allocated_exprs_->emplace_back(std::make_unique<ComparisonExpression>(lhs, rhs, comp_type));
  return allocated_exprs_->back().get();
}

const AbstractExpression *MakeAggregateValueExpression(
    bool is_group_by_term, uint32_t term_idx, std::vector<std::unique_ptr<AbstractExpression>> *allocated_exprs_) {
  allocated_exprs_->emplace_back(
      std::make_unique<AggregateValueExpression>(is_group_by_term, term_idx, TypeId::INTEGER));
  return allocated_exprs_->back().get();
}

const Schema *MakeOutputSchema(const std::vector<std::pair<std::string, const AbstractExpression *>> &exprs,
                               std::vector<std::unique_ptr<Schema>> *allocated_output_schemas_, uint32_t varchar_size) {
  std::vector<Column> cols;
  cols.reserve(exprs.size());
  for (const auto &input : exprs) {
    if (input.second->GetReturnType() != TypeId::VARCHAR) {
      cols.emplace_back(input.first, input.second->GetReturnType(), input.second);
    } else {
      cols.emplace_back(input.first, input.second->GetReturnType(), varchar_size, input.second);
    }
  }
  allocated_output_schemas_->emplace_back(std::make_unique<Schema>(cols));
  return allocated_output_schemas_->back().get();
}

// NOLINTNEXTLINE
void ExecutorBenmark() {
  // Insert into test_1 and empty_table2 to make each size 50000
  // SELECT empty_table2.colB, COUNT(empty_table2.colA), MAX(empty_table2.colA), MIN(empty_table2.colA),
  // SUM(empty_table2.colA) FROM test_1 JOIN empty_table2 ON test_1.colB = empty_table2.colB GROUP BY state.id

  // local var
  std::unique_ptr<TransactionManager> txn_mgr_;
  Transaction *txn_{nullptr};
  std::unique_ptr<DiskManager> disk_manager_;
  std::unique_ptr<LogManager> log_manager_ = nullptr;
  std::unique_ptr<LockManager> lock_manager_ = nullptr;
  std::unique_ptr<BufferPoolManager> bpm_;
  std::unique_ptr<Catalog> catalog_;
  std::unique_ptr<ExecutorContext> exec_ctx_;
  std::unique_ptr<ExecutionEngine> execution_engine_;
  std::vector<std::unique_ptr<AbstractExpression>> allocated_exprs_;
  std::vector<std::unique_ptr<Schema>> allocated_output_schemas_;
  static constexpr uint32_t MAX_VARCHAR_SIZE = 128;
  std::stringstream ss;

  // set up
  //  we create a new DiskManager, BufferPoolManager, TransactionManager, and Catalog.
  disk_manager_ = std::make_unique<DiskManager>("executor_test.db");
  bpm_ = std::make_unique<BufferPoolManager>(2560, disk_manager_.get());
  txn_mgr_ = std::make_unique<TransactionManager>(lock_manager_.get(), log_manager_.get());
  catalog_ = std::make_unique<Catalog>(bpm_.get(), lock_manager_.get(), log_manager_.get());
  // Begin a new transaction, along with its executor context.
  txn_ = txn_mgr_->Begin();
  exec_ctx_ = std::make_unique<ExecutorContext>(txn_, catalog_.get(), bpm_.get());
  // Generate some test tables.
  TableGenerator gen{exec_ctx_.get()};
  gen.GenerateTestTables();

  execution_engine_ = std::make_unique<ExecutionEngine>(bpm_.get(), txn_mgr_.get(), catalog_.get());

  // insert into empty table
  size_t benchmark_size = 50000;
  std::vector<std::vector<Value>> raw_vals;
  raw_vals.reserve(benchmark_size);
  for (size_t i = 0; i < benchmark_size; ++i) {
    std::vector<Value> val1{ValueFactory::GetIntegerValue(benchmark_size - i), ValueFactory::GetIntegerValue(i % 53)};
    raw_vals.emplace_back(val1);
  }
  // Create insert plan node
  auto table_info = exec_ctx_->GetCatalog()->GetTable("empty_table2");
  InsertPlanNode insert_plan{std::move(raw_vals), table_info->oid_};
  execution_engine_->Execute(&insert_plan, nullptr, txn_, exec_ctx_.get());

  // insert into test1

  raw_vals.clear();
  raw_vals.reserve(benchmark_size);
  for (size_t i = 4999; i < benchmark_size; ++i) {
    std::vector<Value> val1{ValueFactory::GetIntegerValue(i), ValueFactory::GetIntegerValue(i % 53),
                            ValueFactory::GetIntegerValue(i), ValueFactory::GetIntegerValue(i % 2)};
    raw_vals.emplace_back(val1);
  }
  // Create insert plan node
  auto table_info_1 = exec_ctx_->GetCatalog()->GetTable("test_1");
  InsertPlanNode insert_plan1{std::move(raw_vals), table_info_1->oid_};
  execution_engine_->Execute(&insert_plan1, nullptr, txn_, exec_ctx_.get());

  Schema *key_schema = ParseCreateStatement("a bigint");
  GenericComparator<8> comparator(key_schema);
  auto index_info = exec_ctx_->GetCatalog()->CreateIndex<GenericKey<8>, RID, GenericComparator<8>>(
      txn_, "index1", "empty_table2", table_info->schema_, *key_schema, {0}, 8);

  auto t_start = std::chrono::high_resolution_clock::now();

  std::unique_ptr<AbstractPlanNode> scan_plan1;
  const Schema *outer_schema1;
  auto &schema_outer = exec_ctx_->GetCatalog()->GetTable("test_1")->schema_;
  auto outer_colA = MakeColumnValueExpression(schema_outer, 0, "colA", &allocated_exprs_);
  auto outer_colB = MakeColumnValueExpression(schema_outer, 0, "colB", &allocated_exprs_);
  auto outer_colC = MakeColumnValueExpression(schema_outer, 0, "colC", &allocated_exprs_);
  auto outer_colD = MakeColumnValueExpression(schema_outer, 0, "colD", &allocated_exprs_);
  const Schema *outer_out_schema1 =
      MakeOutputSchema({{"colA", outer_colA}, {"colB", outer_colB}, {"colC", outer_colC}, {"colD", outer_colD}},
                       &allocated_output_schemas_, MAX_VARCHAR_SIZE);

  {
    auto table_info_test1 = exec_ctx_->GetCatalog()->GetTable("test_1");
    auto &schema = table_info_test1->schema_;
    auto colA = MakeColumnValueExpression(schema, 0, "colA", &allocated_exprs_);
    auto colB = MakeColumnValueExpression(schema, 0, "colB", &allocated_exprs_);
    outer_schema1 = MakeOutputSchema({{"colA", colA}, {"colB", colB}}, &allocated_output_schemas_, MAX_VARCHAR_SIZE);
    scan_plan1 = std::make_unique<SeqScanPlanNode>(outer_out_schema1, nullptr, table_info_test1->oid_);
  }
  const Schema *out_schema2;
  {
    auto &schema = table_info->schema_;
    auto colA = MakeColumnValueExpression(schema, 0, "colA", &allocated_exprs_);
    auto colB = MakeColumnValueExpression(schema, 0, "colB", &allocated_exprs_);
    out_schema2 = MakeOutputSchema({{"colA", colA}, {"colB", colB}}, &allocated_output_schemas_, MAX_VARCHAR_SIZE);
  }
  std::unique_ptr<NestedIndexJoinPlanNode> join_plan;
  const Schema *out_final;
  {
    // colA and colB have a tuple index of 0 because they are the left side of the join
    auto colA = MakeColumnValueExpression(*outer_schema1, 0, "colA", &allocated_exprs_);
    auto colB = MakeColumnValueExpression(*outer_schema1, 0, "colB", &allocated_exprs_);
    // col1 and col2 have a tuple index of 1 because they are the right side of the join
    auto colA_empty = MakeColumnValueExpression(*out_schema2, 1, "colA", &allocated_exprs_);
    auto colB_empty = MakeColumnValueExpression(*out_schema2, 1, "colB", &allocated_exprs_);
    auto predicate = MakeComparisonExpression(colA, colA_empty, ComparisonType::Equal, &allocated_exprs_);
    out_final =
        MakeOutputSchema({{"colA", colA}, {"colB", colB}, {"colA_empty", colA_empty}, {"colB_empty", colB_empty}},
                         &allocated_output_schemas_, MAX_VARCHAR_SIZE);

    auto inner_table_oid = table_info->oid_;

    join_plan = std::make_unique<NestedIndexJoinPlanNode>(
        out_final, std::vector<const AbstractPlanNode *>{scan_plan1.get()}, predicate, inner_table_oid,
        index_info->name_, outer_schema1, out_schema2);
  }

  // agg plan
  std::unique_ptr<AbstractPlanNode> agg_plan;
  const Schema *agg_schema;
  {
    const AbstractExpression *state_id = MakeColumnValueExpression(*out_final, 0, "colB_empty", &allocated_exprs_);
    const AbstractExpression *val = MakeColumnValueExpression(*out_final, 0, "colA_empty", &allocated_exprs_);
    // Make group bys
    std::vector<const AbstractExpression *> group_by_cols{state_id};
    const AbstractExpression *groupby_state = MakeAggregateValueExpression(true, 0, &allocated_exprs_);
    // Make aggregates
    std::vector<const AbstractExpression *> aggregate_cols{val, val, val, val};
    std::vector<AggregationType> agg_types{AggregationType::CountAggregate, AggregationType::MaxAggregate,
                                           AggregationType::MinAggregate, AggregationType::SumAggregate};
    const AbstractExpression *countVal = MakeAggregateValueExpression(false, 0, &allocated_exprs_);
    const AbstractExpression *maxVal = MakeAggregateValueExpression(false, 1, &allocated_exprs_);
    const AbstractExpression *minVal = MakeAggregateValueExpression(false, 2, &allocated_exprs_);
    const AbstractExpression *sumVal = MakeAggregateValueExpression(false, 3, &allocated_exprs_);

    // Create plan
    agg_schema = MakeOutputSchema({{"state_id", groupby_state},
                                   {"countVal", countVal},
                                   {"maxVal", maxVal},
                                   {"minVal", minVal},
                                   {"sumVal", sumVal}},
                                  &allocated_output_schemas_, MAX_VARCHAR_SIZE);
    agg_plan = std::make_unique<AggregationPlanNode>(agg_schema, join_plan.get(), nullptr, std::move(group_by_cols),
                                                     std::move(aggregate_cols), std::move(agg_types));
  }

  // Execute
  std::vector<Tuple> result_set;
  execution_engine_->Execute(agg_plan.get(), &result_set, txn_, exec_ctx_.get());
  // the work...
  auto t_end = std::chrono::high_resolution_clock::now();
  auto elapsed_time_ms = std::chrono::duration<double, std::milli>(t_end - t_start).count();
  // Verify

  int total = 0;
  for (const auto &tuple : result_set) {
    total += tuple.GetValue(agg_schema, agg_schema->GetColIdx("countVal")).GetAs<int32_t>();
  }
  auto success = total == 46000;

  if (success) {
    ss << elapsed_time_ms;
  } else {
    ss << "FAIL";
  }
  std::cout << ss.str() << std::endl;

  // Commit our transaction.
  txn_mgr_->Commit(txn_);
  // Shut down the disk manager and clean up the transaction.
  disk_manager_->ShutDown();
  delete key_schema;
  delete txn_;
}

TEST(BenchmarkTest, ExecutorBenchmarkTest) {
  TEST_TIMEOUT_BEGIN
  ExecutorBenmark();
  remove("executor_test.db");
  TEST_TIMEOUT_FAIL_END(1000 * 200)
}

}  // namespace bustub
