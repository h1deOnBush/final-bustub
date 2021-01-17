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
#include <algorithm>

namespace bustub {

LRUReplacer::LRUReplacer(size_t num_pages) { num_pages_ = num_pages; }

LRUReplacer::~LRUReplacer() = default;

bool LRUReplacer::Victim(frame_id_t *frame_id) {
  std::lock_guard<std::mutex> lk(latch);

  if (hash_.empty()) {
    *frame_id = INVALID_PAGE_ID;
    return false;
  }
  *frame_id = unpinned_list.back();
  hash_.erase(*frame_id);
  unpinned_list.pop_back();
  return true;
}

void LRUReplacer::Pin(frame_id_t frame_id) {
  std::lock_guard<std::mutex> lk(latch);
  auto it = hash_.find(frame_id);
  if (hash_.find(frame_id) != hash_.end()) {
    unpinned_list.erase(it->second);
    hash_.erase(it);
  }
}

// 需要外部确保unpin list 的size不会大于num_pages
void LRUReplacer::Unpin(frame_id_t frame_id) {
  std::lock_guard<std::mutex> lk(latch);
  if (hash_.find(frame_id) == hash_.end()) {
    unpinned_list.push_front(frame_id);
    hash_[frame_id] = unpinned_list.begin();
    while (unpinned_list.size() > num_pages_) {
      unpinned_list.pop_back();
    }
  }
}

size_t LRUReplacer::Size() {
  std::lock_guard<std::mutex> lk(latch);
  return unpinned_list.size();
}

}  // namespace bustub
