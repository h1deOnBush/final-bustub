//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// delete_executor.cpp
//
// Identification: src/execution/delete_executor.cpp
//
// Copyright (c) 2015-19, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//
#include <include/execution/executor_factory.h>
#include <memory>

#include "execution/executors/delete_executor.h"

namespace bustub {

DeleteExecutor::DeleteExecutor(ExecutorContext *exec_ctx, const DeletePlanNode *plan,
                               std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan), child_executor_(std::move(child_executor)) {}

void DeleteExecutor::Init() {
  child_executor_->Init();
  metadata_ = exec_ctx_->GetCatalog()->GetTable(plan_->TableOid());
}

bool DeleteExecutor::Next([[maybe_unused]] Tuple *tuple, RID *rid) {
  auto indexes = exec_ctx_->GetCatalog()->GetTableIndexes(metadata_->name_);
  Tuple child_tuple;
  RID child_rid;
  while (child_executor_->Next(&child_tuple, &child_rid)) {
    auto res = metadata_->table_->MarkDelete(child_rid, exec_ctx_->GetTransaction());
    if (!res) {
      LOG_DEBUG("delete executor failure!!! throw an exception");
      throw Exception("Delete Tuple fail in delete executor");
    }
    for (auto &index : indexes) {
      auto key_tuple = child_tuple.KeyFromTuple(metadata_->schema_, index->key_schema_, index->index_->GetKeyAttrs());
      index->index_->DeleteEntry(key_tuple, child_rid, exec_ctx_->GetTransaction());
    }
  }

  return false;
}

}  // namespace bustub
