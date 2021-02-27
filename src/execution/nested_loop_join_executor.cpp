//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// nested_loop_join_executor.cpp
//
// Identification: src/execution/nested_loop_join_executor.cpp
//
// Copyright (c) 2015-19, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "execution/executors/nested_loop_join_executor.h"
#include <include/execution/plans/seq_scan_plan.h>

namespace bustub {

NestedLoopJoinExecutor::NestedLoopJoinExecutor(ExecutorContext *exec_ctx, const NestedLoopJoinPlanNode *plan,
                                               std::unique_ptr<AbstractExecutor> &&left_executor,
                                               std::unique_ptr<AbstractExecutor> &&right_executor)
    : AbstractExecutor(exec_ctx),
      plan_(plan),
      left_executor_(std::move(left_executor)),
      right_exeuctor_(std::move(right_executor)) {}

void NestedLoopJoinExecutor::Init() {
  auto left_plan = dynamic_cast<const SeqScanPlanNode *>(plan_->GetLeftPlan());
  auto right_plan = dynamic_cast<const SeqScanPlanNode *>(plan_->GetRightPlan());

  left_meta_data_ = exec_ctx_->GetCatalog()->GetTable(left_plan->GetTableOid());
  right_meta_data_ = exec_ctx_->GetCatalog()->GetTable(right_plan->GetTableOid());


  left_executor_->Init();
  right_exeuctor_->Init();
  inner_loop_ended = true;
}

bool NestedLoopJoinExecutor::Next(Tuple *tuple, RID *rid) {
  auto predicate = plan_->Predicate();
  if (predicate == nullptr) {
    LOG_DEBUG("predicate in NestedLoopJoinExecutor is nullptr");
    throw Exception("NestedLoopJoinExecutor not predicate");
  }

  bool founded = false;
  while (true) {
    if (inner_loop_ended) {
      if (!left_executor_->Next(&outer_tuple, &outer_rid)) {
        return false;
      }
      right_exeuctor_->Init();
      inner_loop_ended = false;
    }

    Tuple inner_tuple;
    RID inner_rid;

    while (right_exeuctor_->Next(&inner_tuple, &inner_rid)) {
      auto res =
          predicate->EvaluateJoin(&outer_tuple, &left_meta_data_->schema_,
            &inner_tuple, &right_meta_data_->schema_);
      if (res.GetAs<bool>()) {
        *tuple = GetNestedLoopJoinTuple(&outer_tuple, &left_meta_data_->schema_, &inner_tuple,
                                        &right_meta_data_->schema_, GetOutputSchema());
        // *rid = inner_rid;
        founded = true;
      }
      if (founded) {
        return true;
      }
    }
    inner_loop_ended = true;
  }
}

// 不支持 两个tuple的某一列同名
Tuple NestedLoopJoinExecutor::GetNestedLoopJoinTuple(const Tuple *left_tuple, const Schema *left_schema,
                                                     const Tuple *right_tuple, const Schema *right_schema,
                                                     const Schema *output_schema) {
  std::vector<Value> vals;
  for (const auto &col : output_schema->GetColumns()) {
    bool found = false;
    auto left_columns = left_schema->GetColumns();
    for (size_t i = 0; i < left_schema->GetColumnCount(); i++) {
      if (left_columns[i].GetName() == col.GetName()) {
        auto val = col.GetExpr()->Evaluate(left_tuple, left_schema);
        vals.push_back(val);
        found = true;
        break;
      }
    }
    if (found) {
      continue;
    }
    auto right_columns = right_schema->GetColumns();
    for (size_t i = 0; i < right_schema->GetColumnCount(); i++) {
      if (right_columns[i].GetName() == col.GetName()) {
        auto val = col.GetExpr()->Evaluate(right_tuple, right_schema);
        vals.push_back(val);
        break;
      }
    }
  }

  return Tuple(vals, output_schema);
}

}  // namespace bustub
