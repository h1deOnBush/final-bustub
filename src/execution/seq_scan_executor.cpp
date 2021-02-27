//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// seq_scan_executor.cpp
//
// Identification: src/execution/seq_scan_executor.cpp
//
// Copyright (c) 2015-19, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//
#include "execution/executors/seq_scan_executor.h"

namespace bustub {

SeqScanExecutor::SeqScanExecutor(ExecutorContext *exec_ctx, const SeqScanPlanNode *plan)
    : AbstractExecutor(exec_ctx), plan_(plan), it_({nullptr, RID(), nullptr}) {}

void SeqScanExecutor::Init() {
  metadata_ = exec_ctx_->GetCatalog()->GetTable(plan_->GetTableOid());
  it_ = metadata_->table_->Begin(exec_ctx_->GetTransaction());
}

bool SeqScanExecutor::Next(Tuple *tuple, RID *rid) {
  Tuple t;
  RID id;
  bool not_ended = false;
  while (it_ != metadata_->table_->End()) {
    bool value = false;
    t = *it_;
    id = it_->GetRid();
    ++it_;
    auto predicate = plan_->GetPredicate();
    if (predicate != nullptr) {
      value = plan_->GetPredicate()->Evaluate(&t, GetOutputSchema()).GetAs<bool>();
    } else {
      value = true;
    }

    if (!value) {
      continue;
    }
    not_ended = true;

    break;
  }

  if (not_ended) {
    *tuple = GetOutputTuple(&t);
    *rid = id;
  }
  return not_ended;
}

Tuple SeqScanExecutor::GetOutputTuple(const Tuple *t) {
  std::vector<Value> values;
  auto output_schema = GetOutputSchema();
  for (const auto &col : output_schema->GetColumns()) {
    auto val = col.GetExpr()->Evaluate(t, &metadata_->schema_);

    values.push_back(val);
  }
  return Tuple(values, output_schema);
}

}  // namespace bustub
