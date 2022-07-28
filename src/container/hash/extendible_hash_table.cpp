//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// extendible_hash_table.cpp
//
// Identification: src/container/hash/extendible_hash_table.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <iostream>
#include <string>
#include <utility>
#include <vector>

#include "common/exception.h"
#include "common/logger.h"
#include "common/rid.h"
#include "container/hash/extendible_hash_table.h"

namespace bustub {

template <typename KeyType, typename ValueType, typename KeyComparator>
HASH_TABLE_TYPE::ExtendibleHashTable(const std::string &name, BufferPoolManager *buffer_pool_manager,
                                     const KeyComparator &comparator, HashFunction<KeyType> hash_fn)
    : buffer_pool_manager_(buffer_pool_manager), comparator_(comparator), hash_fn_(std::move(hash_fn)) {
  //  implement me!
  auto *dirpage = buffer_pool_manager_->NewPage(&directory_page_id_);
  page_id_t bucket_page = 0;
  buffer_pool_manager_->NewPage(&bucket_page);
  HashTableDirectoryPage *dir_page = reinterpret_cast<HashTableDirectoryPage *>(dirpage->GetData());
  dir_page->SetPageId(directory_page_id_);
  dir_page->SetLSN(0);
  dir_page->SetBucketPageId(0, bucket_page);

  dir_page->VerifyIntegrity();

  buffer_pool_manager_->UnpinPage(directory_page_id_, false);
  buffer_pool_manager_->UnpinPage(bucket_page, false);
}

/*****************************************************************************
 * HELPERS
 *****************************************************************************/
/**
 * Hash - simple helper to downcast MurmurHash's 64-bit hash to 32-bit
 * for extendible hashing.
 *
 * @param key the key to hash
 * @return the downcasted 32-bit hash
 */
template <typename KeyType, typename ValueType, typename KeyComparator>
uint32_t HASH_TABLE_TYPE::Hash(KeyType key) {
  return static_cast<uint32_t>(hash_fn_.GetHash(key));
}

template <typename KeyType, typename ValueType, typename KeyComparator>
inline uint32_t HASH_TABLE_TYPE::KeyToDirectoryIndex(KeyType key, HashTableDirectoryPage *dir_page) {
  return Hash(key) & dir_page->GetGlobalDepthMask();
}

template <typename KeyType, typename ValueType, typename KeyComparator>
inline uint32_t HASH_TABLE_TYPE::KeyToPageId(KeyType key, HashTableDirectoryPage *dir_page) {
  return dir_page->GetBucketPageId(KeyToDirectoryIndex(key, dir_page));
}

template <typename KeyType, typename ValueType, typename KeyComparator>
HashTableDirectoryPage *HASH_TABLE_TYPE::FetchDirectoryPage() {
  return reinterpret_cast<HashTableDirectoryPage *>(buffer_pool_manager_->FetchPage(directory_page_id_)->GetData());
}

template <typename KeyType, typename ValueType, typename KeyComparator>
HASH_TABLE_BUCKET_TYPE *HASH_TABLE_TYPE::FetchBucketPage(page_id_t bucket_page_id) {
  return reinterpret_cast<HASH_TABLE_BUCKET_TYPE *>(buffer_pool_manager_->FetchPage(bucket_page_id)->GetData());
}

/*****************************************************************************
 * SEARCH
 *****************************************************************************/
template <typename KeyType, typename ValueType, typename KeyComparator>
bool HASH_TABLE_TYPE::GetValue(Transaction *transaction, const KeyType &key, std::vector<ValueType> *result) {
  table_latch_.RLock();

  auto page_id = KeyToPageId(key, FetchDirectoryPage());
  auto *bucket_page = FetchBucketPage(page_id);
  bool result_bool = bucket_page->GetValue(key, comparator_, result);

  buffer_pool_manager_->UnpinPage(directory_page_id_, false);
  buffer_pool_manager_->UnpinPage(page_id, false);
  table_latch_.RUnlock();
  return result_bool;
}

/*****************************************************************************
 * INSERTION
 *****************************************************************************/
template <typename KeyType, typename ValueType, typename KeyComparator>
bool HASH_TABLE_TYPE::Insert(Transaction *transaction, const KeyType &key, const ValueType &value) {
  table_latch_.WLock();
  auto bucket_page_id = KeyToPageId(key, FetchDirectoryPage());
  auto *bucket_page = FetchBucketPage(bucket_page_id);
  bool result;

  if (bucket_page->IsFull()) {
    table_latch_.WUnlock();
    result = SplitInsert(transaction, key, value);
    table_latch_.WLock();
  } else {
    result = bucket_page->Insert(key, value, comparator_);
  }

  buffer_pool_manager_->UnpinPage(directory_page_id_, false);
  buffer_pool_manager_->UnpinPage(bucket_page_id, true);
  table_latch_.WUnlock();
  return result;
}

template <typename KeyType, typename ValueType, typename KeyComparator>
bool HASH_TABLE_TYPE::SplitInsert(Transaction *transaction, const KeyType &key, const ValueType &value) {
  table_latch_.WLock();

  bool result_bool = false;

  auto *dir_pag = FetchDirectoryPage();
  auto bucket_page_id = KeyToPageId(key, dir_pag);
  auto *bucket_page = FetchBucketPage(bucket_page_id);

  if (bucket_page->IsFull()) {
    auto index = KeyToDirectoryIndex(key, dir_pag);

    dir_pag->IncrLocalDepth(index);
    auto newindex = dir_pag->GetSplitImageIndex(index);

    if (dir_pag->GetLocalDepth(index) > dir_pag->GetGlobalDepth()) {
      dir_pag->IncrGlobalDepth();
    } else {
      dir_pag->IncrLocalDepth(newindex);
    }

    page_id_t bucket_pageid = 0;
    buffer_pool_manager_->NewPage(&bucket_pageid);
    dir_pag->SetBucketPageId(newindex, bucket_pageid);

    std::vector<MappingType> bucket_values;
    bucket_page->GetAllValue(&bucket_values);
    bucket_page->Clear();
    for (auto &&v : bucket_values) {
      table_latch_.WUnlock();
      Insert(transaction, v.first, v.second);
      table_latch_.WLock();
    }

    table_latch_.WUnlock();
    result_bool = Insert(transaction, key, value);
    table_latch_.WLock();

    buffer_pool_manager_->UnpinPage(directory_page_id_, true);
    buffer_pool_manager_->UnpinPage(bucket_pageid, true);
  } else {
    result_bool = bucket_page->Insert(key, value, comparator_);
    buffer_pool_manager_->UnpinPage(directory_page_id_, false);
  }
  buffer_pool_manager_->UnpinPage(bucket_page_id, true);
  table_latch_.WUnlock();
  return result_bool;
}

/*****************************************************************************
 * REMOVE
 *****************************************************************************/
template <typename KeyType, typename ValueType, typename KeyComparator>
bool HASH_TABLE_TYPE::Remove(Transaction *transaction, const KeyType &key, const ValueType &value) {
  table_latch_.WLock();

  auto bucket_page_id = KeyToPageId(key, FetchDirectoryPage());
  auto *bucket_page = FetchBucketPage(bucket_page_id);

  if (!bucket_page->Remove(key, value, comparator_)) {
    buffer_pool_manager_->UnpinPage(bucket_page_id, false);
    buffer_pool_manager_->UnpinPage(directory_page_id_, false);
    table_latch_.WUnlock();

    return false;
  }
  buffer_pool_manager_->UnpinPage(bucket_page_id, true);
  buffer_pool_manager_->UnpinPage(directory_page_id_, false);
  table_latch_.WUnlock();
  if (bucket_page->IsEmpty()) {
    Merge(transaction, key, value);
  }
  return true;
}

template <typename KeyType, typename ValueType, typename KeyComparator>
void HASH_TABLE_TYPE::ChangeBucketDepth(uint32_t bucket_index, page_id_t bucket_page_id,
                                        HashTableDirectoryPage *dir_page) {
  uint32_t bucket_interval = 1 << (dir_page->GetGlobalDepth() - 1);

  if (bucket_index + bucket_interval < dir_page->Size() &&
      dir_page->GetBucketPageId(bucket_index) == dir_page->GetBucketPageId(bucket_index + bucket_interval)) {
    dir_page->DecrLocalDepth(bucket_index + bucket_interval);
    dir_page->SetBucketPageId(bucket_index + bucket_interval, bucket_page_id);
  } else if (bucket_index - bucket_interval < dir_page->Size() &&
             dir_page->GetBucketPageId(bucket_index) == dir_page->GetBucketPageId(bucket_index - bucket_interval)) {
    dir_page->DecrLocalDepth(bucket_index - bucket_interval);
    dir_page->SetBucketPageId(bucket_index - bucket_interval, bucket_page_id);
  }
}

/*****************************************************************************
 * MERGE
 *****************************************************************************/
template <typename KeyType, typename ValueType, typename KeyComparator>
void HASH_TABLE_TYPE::Merge(Transaction *transaction, const KeyType &key, const ValueType &value) {
  table_latch_.WLock();

  auto *dir_pag = FetchDirectoryPage();
  auto bucket_page_id = KeyToPageId(key, dir_pag);
  auto *bucket_page = FetchBucketPage(bucket_page_id);
  bool is_dirty = false;
  if (bucket_page->IsEmpty()) {
    auto index = KeyToDirectoryIndex(key, dir_pag);
    auto newindex = dir_pag->GetSplitImageIndex(index);
    if (dir_pag->GetLocalDepth(index) == dir_pag->GetLocalDepth(newindex)) {
      is_dirty = true;
      ChangeBucketDepth(index, dir_pag->GetBucketPageId(newindex), dir_pag);
      ChangeBucketDepth(newindex, dir_pag->GetBucketPageId(newindex), dir_pag);

      dir_pag->DecrLocalDepth(index);
      dir_pag->DecrLocalDepth(newindex);
      dir_pag->SetBucketPageId(index, dir_pag->GetBucketPageId(newindex));
      buffer_pool_manager_->UnpinPage(bucket_page_id, is_dirty);
      buffer_pool_manager_->DeletePage(bucket_page_id);
      if (dir_pag->CanShrink()) {
        dir_pag->DecrGlobalDepth();
      }
    }
  }
  if (!is_dirty) {
    buffer_pool_manager_->UnpinPage(bucket_page_id, is_dirty);
  }
  buffer_pool_manager_->UnpinPage(directory_page_id_, is_dirty);
  table_latch_.WUnlock();
}

/*****************************************************************************
 * GETGLOBALDEPTH - DO NOT TOUCH
 *****************************************************************************/
template <typename KeyType, typename ValueType, typename KeyComparator>
uint32_t HASH_TABLE_TYPE::GetGlobalDepth() {
  table_latch_.RLock();
  HashTableDirectoryPage *dir_page = FetchDirectoryPage();
  uint32_t global_depth = dir_page->GetGlobalDepth();
  assert(buffer_pool_manager_->UnpinPage(directory_page_id_, false, nullptr));
  table_latch_.RUnlock();
  return global_depth;
}

/*****************************************************************************
 * VERIFY INTEGRITY - DO NOT TOUCH
 *****************************************************************************/
template <typename KeyType, typename ValueType, typename KeyComparator>
void HASH_TABLE_TYPE::VerifyIntegrity() {
  table_latch_.RLock();
  HashTableDirectoryPage *dir_page = FetchDirectoryPage();
  dir_page->VerifyIntegrity();
  assert(buffer_pool_manager_->UnpinPage(directory_page_id_, false, nullptr));
  table_latch_.RUnlock();
}

/*****************************************************************************
 * TEMPLATE DEFINITIONS - DO NOT TOUCH
 *****************************************************************************/
template class ExtendibleHashTable<int, int, IntComparator>;

template class ExtendibleHashTable<GenericKey<4>, RID, GenericComparator<4>>;
template class ExtendibleHashTable<GenericKey<8>, RID, GenericComparator<8>>;
template class ExtendibleHashTable<GenericKey<16>, RID, GenericComparator<16>>;
template class ExtendibleHashTable<GenericKey<32>, RID, GenericComparator<32>>;
template class ExtendibleHashTable<GenericKey<64>, RID, GenericComparator<64>>;

}  // namespace bustub
