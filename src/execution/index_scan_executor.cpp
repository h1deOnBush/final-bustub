//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// index_scan_executor.cpp
//
// Identification: src/execution/index_scan_executor.cpp
//
// Copyright (c) 2015-19, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//
#include "execution/executors/index_scan_executor.h"

namespace bustub {
IndexScanExecutor::IndexScanExecutor(ExecutorContext *exec_ctx, const IndexScanPlanNode *plan)
    : AbstractExecutor(exec_ctx), plan_(plan) {}

void IndexScanExecutor::Init() {
  indexInfo_ = exec_ctx_->GetCatalog()->GetIndex(plan_->GetIndexOid());
  metadata_ = exec_ctx_->GetCatalog()->GetTable(indexInfo_->table_name_);
  b_plus_tree_index_ =
      reinterpret_cast<BPlusTreeIndex<GenericKey<8>, RID, GenericComparator<8>> *>(indexInfo_->index_.get());
  index_iterator_ =
      std::make_unique<IndexIterator<GenericKey<8>, RID, GenericComparator<8>>>(b_plus_tree_index_->GetBeginIterator());
}

bool IndexScanExecutor::Next(Tuple *tuple, RID *rid) {
  bool hasvalue = false;
  bool not_ended = false;
  Tuple t{};
  RID id;
  while (*index_iterator_ != b_plus_tree_index_->GetEndIterator()) {
    auto cur = **index_iterator_;
    id = cur.second;
    ++(*index_iterator_);

    auto res = metadata_->table_->GetTuple(cur.second, &t, exec_ctx_->GetTransaction());
    if (!res) {
      LOG_DEBUG("IndexScanExecutor not found the tuple whose rid is %s", cur.second.ToString().c_str());
      continue;
    }

    auto predicate = plan_->GetPredicate();
    if (predicate != nullptr) {  // if have predicate
      hasvalue = predicate->Evaluate(&t, GetOutputSchema()).GetAs<bool>();
      if (!hasvalue) {
        continue;
      }
    }
    not_ended = true;

    break;
  }

  if (not_ended) {
    *tuple = GetOutputTuple(&metadata_->schema_, GetOutputSchema(), &t);
    *rid = id;
  }
  return not_ended;
}

Tuple IndexScanExecutor::GetOutputTuple(const Schema *input_schema, const Schema *output_schema, const Tuple *t) {
  std::vector<Value> values;
  for (const auto &col : output_schema->GetColumns()) {
    auto val = col.GetExpr()->Evaluate(t, input_schema);
    values.push_back(val);
  }
  return Tuple(values, output_schema);
}

}  // namespace bustub
