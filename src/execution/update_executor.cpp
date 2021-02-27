//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// update_executor.cpp
//
// Identification: src/execution/update_executor.cpp
//
// Copyright (c) 2015-20, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//
#include <include/execution/executor_factory.h>
#include <memory>

#include "execution/executors/update_executor.h"

namespace bustub {

UpdateExecutor::UpdateExecutor(ExecutorContext *exec_ctx, const UpdatePlanNode *plan,
                               std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan), child_executor_(std::move(child_executor)) {}

void UpdateExecutor::Init() {
  table_info_ = exec_ctx_->GetCatalog()->GetTable(plan_->TableOid());
  child_executor_->Init();
}

bool UpdateExecutor::Next([[maybe_unused]] Tuple *tuple, RID *rid) {
  Tuple child_tuple;
  RID child_rid;
  bool res = false;
  if (child_executor_->Next(&child_tuple, &child_rid)) {  // 如果有值
    child_tuple = GenerateUpdatedTuple(child_tuple);

    res = table_info_->table_->UpdateTuple(child_tuple, child_rid, exec_ctx_->GetTransaction());
  }

  if (!res) {
    return false;
  }

  *tuple = child_tuple;
  *rid = child_rid;
  return true;
}

}  // namespace bustub
