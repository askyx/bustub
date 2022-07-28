//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// seq_scan_executor.cpp
//
// Identification: src/execution/seq_scan_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "execution/executors/seq_scan_executor.h"
#include <vector>

namespace bustub {

SeqScanExecutor::SeqScanExecutor(ExecutorContext *exec_ctx, const SeqScanPlanNode *plan)
    : AbstractExecutor(exec_ctx),
      plan_(plan),
      table_(exec_ctx->GetCatalog()->GetTable(plan_->GetTableOid())),
      it_(table_->table_->Begin(exec_ctx_->GetTransaction())) {}

void SeqScanExecutor::Init() {}

bool SeqScanExecutor::Next(Tuple *tuple, RID *rid) {
  if (it_ == table_->table_->End()) {
    return false;
  }

  auto cols = plan_->OutputSchema()->GetColumnCount();
  std::vector<Value> value(cols);
  for (size_t i = 0; i < cols; i++) {
    value[i] = GetOutputSchema()->GetColumn(i).GetExpr()->Evaluate(&(*it_), GetOutputSchema());
  }

  auto r = it_->GetRid();
  Tuple t(value, GetOutputSchema());
  auto predicet = plan_->GetPredicate();

  ++it_;

  if (!predicet || predicet->Evaluate(&t, GetOutputSchema()).GetAs<bool>()) {
    *tuple = t;
    *rid = r;
    return true;
  }

  return Next(tuple, rid);
}

}  // namespace bustub
