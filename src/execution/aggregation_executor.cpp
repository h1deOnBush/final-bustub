//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// aggregation_executor.cpp
//
// Identification: src/execution/aggregation_executor.cpp
//
// Copyright (c) 2015-19, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//
#include <memory>
#include <vector>

#include "execution/executors/aggregation_executor.h"

namespace bustub {

AggregationExecutor::AggregationExecutor(ExecutorContext *exec_ctx, const AggregationPlanNode *plan,
                                         std::unique_ptr<AbstractExecutor> &&child)
    : AbstractExecutor(exec_ctx),
      plan_(plan),
      child_(std::move(child)),
      aht_({plan->GetAggregates(), plan->GetAggregateTypes()}),
      aht_iterator_(aht_.Begin()) {}

const AbstractExecutor *AggregationExecutor::GetChildExecutor() const { return child_.get(); }

void AggregationExecutor::Init() {
  child_->Init();

  std::vector<Tuple> child_tuple_vec;
  Tuple tuple;
  RID rid;

  while (child_->Next(&tuple, &rid)) {
    child_tuple_vec.push_back(tuple);
  }

  for (const auto &child_tuple : child_tuple_vec) {
    aht_.InsertCombine(MakeKey(&child_tuple), MakeVal(&child_tuple));
  }
  aht_iterator_ = aht_.Begin();

}

bool AggregationExecutor::Next(Tuple *tuple, RID *rid) {
  bool found = false;

  Tuple agg_tuple;
  // auto count = 1;
  while (aht_iterator_ != aht_.End()) {
    std::vector<Value> vals;

    if (plan_->GetHaving() != nullptr) {
      auto res = plan_->GetHaving()->EvaluateAggregate(aht_iterator_.Key().group_bys_, aht_iterator_.Val().aggregates_);
      if (res.GetAs<bool>()) {
        for (const auto &col : GetOutputSchema()->GetColumns()) {
          vals.push_back(
              col.GetExpr()->EvaluateAggregate(aht_iterator_.Key().group_bys_, aht_iterator_.Val().aggregates_));
        }
        // std::cout << "vals' size is " << vals.size() << std::endl;
        agg_tuple = Tuple(vals, plan_->OutputSchema());
        // agg_tuple = Tuple(aht_iterator_.Val().aggregates_, plan_->OutputSchema());
        found = true;
      }
    } else {
      for (const auto &col : GetOutputSchema()->GetColumns()) {
        vals.push_back(
            col.GetExpr()->EvaluateAggregate(aht_iterator_.Key().group_bys_, aht_iterator_.Val().aggregates_));
      }
      // std::cout << "vals' size is " << vals.size() << std::endl;
      agg_tuple = Tuple(vals, plan_->OutputSchema());
      found = true;
      // std::cout << "just for check "<< ++count << std::endl;
    }
    ++aht_iterator_;
    if (found) {
      break;
    }
  }
  *tuple = agg_tuple;
  return found;
}

}  // namespace bustub
