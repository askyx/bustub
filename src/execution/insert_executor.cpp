//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// insert_executor.cpp
//
// Identification: src/execution/insert_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <memory>

#include "execution/executor_factory.h"
#include "execution/executors/insert_executor.h"

namespace bustub {

InsertExecutor::InsertExecutor(ExecutorContext *exec_ctx, const InsertPlanNode *plan,
                               std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan), table_(exec_ctx->GetCatalog()->GetTable(plan_->TableOid())) {}

void InsertExecutor::Init() {}

bool InsertExecutor::Next([[maybe_unused]] Tuple *tuple, RID *rid) {
  if (plan_->IsRawInsert() && plan_->RawValues().size() > 0) {
    for (auto &v : plan_->RawValues()) {
      RID rid;
      Tuple t(v, &(table_->schema_));
      if (!table_->table_->InsertTuple(t, &rid, exec_ctx_->GetTransaction())) {
        throw Exception(ExceptionType::OUT_OF_MEMORY, "InsertExecutor:: InsertTuple() out of meme");
      }

      for (auto &&index : exec_ctx_->GetCatalog()->GetTableIndexes(table_->name_)) {
        auto key = t.KeyFromTuple(table_->schema_, index->key_schema_, index->index_->GetKeyAttrs());
        index->index_->InsertEntry(key, rid, exec_ctx_->GetTransaction());
      }
    }
    return true;
  }

  auto child_executor = ExecutorFactory::CreateExecutor(exec_ctx_, plan_->GetChildPlan());
  child_executor->Init();

  std::vector<Tuple> result_set{};
  Tuple child_tuple;
  RID child_rid;
  while (child_executor->Next(&child_tuple, &child_rid)) {
    result_set.push_back(child_tuple);
  }

  for (auto &&tuple : result_set) {
    RID rid;
    if (!table_->table_->InsertTuple(tuple, &rid, exec_ctx_->GetTransaction())) {
      throw Exception(ExceptionType::OUT_OF_MEMORY, "InsertExecutor:: InsertTuple() out of meme");
    }

    for (auto &&index : exec_ctx_->GetCatalog()->GetTableIndexes(table_->name_)) {
      auto key = tuple.KeyFromTuple(table_->schema_, index->key_schema_, index->index_->GetKeyAttrs());
      index->index_->InsertEntry(key, rid, exec_ctx_->GetTransaction());
    }
  }

  return true;
}

}  // namespace bustub
