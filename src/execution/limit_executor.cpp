//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// limit_executor.cpp
//
// Identification: src/execution/limit_executor.cpp
//
// Copyright (c) 2015-19, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "execution/executors/limit_executor.h"

namespace bustub {

LimitExecutor::LimitExecutor(ExecutorContext *exec_ctx, const LimitPlanNode *plan,
                             std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan), child_executor_(std::move(child_executor)) {}

void LimitExecutor::Init() {
  child_executor_->Init();
  curNum = 0;

  Tuple child_tuple;
  RID child_rid;
  int offset = plan_->GetOffset();
  while (--offset >= 0 && Next(&child_tuple, &child_rid)) {
  }
}

bool LimitExecutor::Next(Tuple *tuple, RID *rid) {
  bool found = false;
  Tuple child_tuple;
  RID child_rid;
  if (++curNum <= plan_->GetLimit() && Next(&child_tuple, &child_rid)) {
    found = true;
  }

  if (found) {
    *tuple = child_tuple;
    *rid = child_rid;
  }
  return found;
}

}  // namespace bustub
