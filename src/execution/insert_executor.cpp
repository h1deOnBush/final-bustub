//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// insert_executor.cpp
//
// Identification: src/execution/insert_executor.cpp
//
// Copyright (c) 2015-19, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//
#include <include/execution/executor_factory.h>
#include <memory>

#include "execution/executors/insert_executor.h"

namespace bustub {

InsertExecutor::InsertExecutor(ExecutorContext *exec_ctx, const InsertPlanNode *plan,
                               std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan) {}

void InsertExecutor::Init() { metadata_ = exec_ctx_->GetCatalog()->GetTable(plan_->TableOid()); }

bool InsertExecutor::Next([[maybe_unused]] Tuple *tuple, RID *rid) {
  // bool success = false;
  auto indexes = exec_ctx_->GetCatalog()->GetTableIndexes(metadata_->name_);

  if (plan_->IsRawInsert()) {
    for (const auto &val : plan_->RawValues()) {
      Tuple t = Tuple(val, &metadata_->schema_);
      metadata_->table_->InsertTuple(t, rid, exec_ctx_->GetTransaction());
      for (auto &index_i : indexes) {
        auto key = t.KeyFromTuple(metadata_->schema_, index_i->key_schema_, index_i->index_->GetKeyAttrs());
        index_i->index_->InsertEntry(key, *rid, exec_ctx_->GetTransaction());
      }
    }

  } else {
    // child plan insert
    // "Insert should have at most one child plan." Thus only one child node here
    auto child_plan = plan_->GetChildPlan();
    auto child_executor = ExecutorFactory::CreateExecutor(exec_ctx_, child_plan);

    // prepare
    child_executor->Init();

    // execute
    try {
      Tuple child_tuple;
      RID child_rid;
      while (child_executor->Next(&child_tuple, &child_rid)) {
        metadata_->table_->InsertTuple(child_tuple, &child_rid, exec_ctx_->GetTransaction());

        for (auto &index_i : indexes) {
          auto key = child_tuple.KeyFromTuple(metadata_->schema_, index_i->key_schema_, index_i->index_->GetKeyAttrs());
          index_i->index_->InsertEntry(key, child_rid, exec_ctx_->GetTransaction());
        }
      }
    } catch (Exception &e) {
      // TODO(student): handle exceptions
    }
  }
  return false;
}

}  // namespace bustub
