//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// lru_replacer.cpp
//
// Identification: src/buffer/lru_replacer.cpp
//
// Copyright (c) 2015-2019, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "buffer/lru_replacer.h"

namespace bustub {

LRUReplacer::LRUReplacer(size_t num_pages) { capacity_ = num_pages; }

LRUReplacer::~LRUReplacer() = default;

bool LRUReplacer::Victim(frame_id_t *frame_id) {
  latch_.lock();

  if (lru_map_.empty()) {
    latch_.unlock();
    return false;
  }

  auto frame = lru_list_.back();

  lru_map_.erase(frame);
  lru_list_.pop_back();
  *frame_id = frame;

  latch_.unlock();
  return true;
}

void LRUReplacer::Pin(frame_id_t frame_id) {
  latch_.lock();

  if (lru_map_.find(frame_id) != lru_map_.end()) {
    lru_list_.erase(lru_map_[frame_id]);
    lru_map_.erase(frame_id);
  }

  latch_.unlock();
}

void LRUReplacer::Unpin(frame_id_t frame_id) {
  latch_.lock();

  if (lru_map_.find(frame_id) == lru_map_.end()) {
    while (lru_map_.size() >= capacity_) {
      auto frame = lru_list_.back();
      lru_map_.erase(frame);
      lru_list_.pop_back();
    }

    lru_list_.push_front(frame_id);
    lru_map_[frame_id] = lru_list_.begin();
  }

  latch_.unlock();
}

size_t LRUReplacer::Size() { return lru_map_.size(); }

}  // namespace bustub
