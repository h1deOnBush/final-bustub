//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// index_scan_executor.h
//
// Identification: src/include/execution/executors/index_scan_executor.h
//
// Copyright (c) 2015-20, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include <memory>  // added
#include <vector>
#include "common/rid.h"
#include "execution/executor_context.h"
#include "execution/executors/abstract_executor.h"
#include "execution/plans/index_scan_plan.h"
#include "storage/table/tuple.h"

namespace bustub {

/**
 * IndexScanExecutor executes an index scan over a table.
 */

class IndexScanExecutor : public AbstractExecutor {
 public:
  /**
   * Creates a new index scan executor.
   * @param exec_ctx the executor context
   * @param plan the index scan plan to be executed
   */
  IndexScanExecutor(ExecutorContext *exec_ctx, const IndexScanPlanNode *plan);

  const Schema *GetOutputSchema() override { return plan_->OutputSchema(); };

  void Init() override;

  bool Next(Tuple *tuple, RID *rid) override;

 private:
  /** added helper function */
  Tuple GetOutputTuple(const Schema *input_schema, const Schema *output_schema, const Tuple *t);

  /** The index scan plan node to be executed. */
  const IndexScanPlanNode *plan_;

  // added
  IndexInfo *indexInfo_;
  TableMetadata *metadata_;
  BPlusTreeIndex<GenericKey<8>, RID, GenericComparator<8>> *b_plus_tree_index_;
  std::unique_ptr<IndexIterator<GenericKey<8>, RID, GenericComparator<8>>> index_iterator_;
};
}  // namespace bustub
