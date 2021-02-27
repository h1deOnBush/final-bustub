//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// nested_index_join_executor.cpp
//
// Identification: src/execution/nested_index_join_executor.cpp
//
// Copyright (c) 2015-19, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "execution/executors/nested_index_join_executor.h"

namespace bustub {

NestIndexJoinExecutor::NestIndexJoinExecutor(ExecutorContext *exec_ctx, const NestedIndexJoinPlanNode *plan,
                                             std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan), outer_executor_(std::move(child_executor)) {}

void NestIndexJoinExecutor::Init() {
  outer_executor_->Init();
  inner_metadata_ = exec_ctx_->GetCatalog()->GetTable(plan_->GetInnerTableOid());
}

// 不支持 两个tuple的某一列同名
bool NestIndexJoinExecutor::GetOutputTuple(const Tuple *outer_tuple, const Tuple *inner_tuple, Tuple *output_tuple) {
  std::vector<Value> vals;
  for (const auto &col : GetOutputSchema()->GetColumns()) {
    bool found = false;
    auto outer_schema = plan_->OuterTableSchema();
    auto outer_columns = outer_schema->GetColumns();
    for (size_t i = 0; i < outer_schema->GetColumnCount(); i++) {
      if (outer_columns[i].GetName() == col.GetName()) {
        auto val = col.GetExpr()->Evaluate(outer_tuple, outer_schema);
        vals.push_back(val);
        found = true;
        break;
      }
    }
    if (found) {
      continue;
    }
    auto inner_schema = plan_->InnerTableSchema();
    auto right_columns = inner_schema->GetColumns();
    for (size_t i = 0; i < inner_schema->GetColumnCount(); i++) {
      if (right_columns[i].GetName() == col.GetName()) {
        auto val = col.GetExpr()->Evaluate(inner_tuple, inner_schema);
        vals.push_back(val);
        break;
      }
    }
  }
  if (vals.size() != GetOutputSchema()->GetColumns().size()) {
    return false;
  }
  *output_tuple = Tuple(vals, GetOutputSchema());
  return true;
}
bool NestIndexJoinExecutor::Next(Tuple *tuple, RID *rid) {
  bool found = false;

  Tuple outer_tuple;
  RID outer_rid;

  while (outer_executor_->Next(&outer_tuple, &outer_rid)) {
    Tuple inner_tuple;
    auto indexes = exec_ctx_->GetCatalog()->GetTableIndexes(inner_metadata_->name_);

    auto join_val = plan_->Predicate()->GetChildAt(0)->EvaluateJoin(&outer_tuple, plan_->OuterTableSchema(), nullptr,
                                                                    nullptr);  // to get the join val

    for (auto index : indexes) {
      std::vector<Value> vals;
      vals.push_back(join_val);
      auto key_schema = exec_ctx_->GetCatalog()->GetIndex(index->index_oid_)->key_schema_;
      std::vector<RID> result;
      Tuple key_tuple = Tuple(vals, &key_schema);
      index->index_->ScanKey(key_tuple, &result, exec_ctx_->GetTransaction());
      // 因为我们这里用的索引都不支持范围查找，只是单点查找，所以不需要考虑那么多
      if (result.empty()) {
        continue;
      }
      inner_metadata_->table_->GetTuple(result[0], &inner_tuple, exec_ctx_->GetTransaction());
      found = true;
    }

    if (found && GetOutputTuple(&outer_tuple, &inner_tuple, tuple)) {
      break;
    }
  }

  return found;
}

}  // namespace bustub
