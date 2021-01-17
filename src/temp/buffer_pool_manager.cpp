//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// buffer_pool_manager.cpp
//
// Identification: src/buffer/buffer_pool_manager.cpp
//
// Copyright (c) 2015-2019, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "buffer/buffer_pool_manager.h"
// added for debug
#include <list>

namespace bustub {

BufferPoolManager::BufferPoolManager(size_t pool_size, DiskManager *disk_manager, LogManager *log_manager)
    : pool_size_(pool_size), disk_manager_(disk_manager), log_manager_(log_manager) {
  // We allocate a consecutive memory space for the buffer pool.
  pages_ = new Page[pool_size_];
  replacer_ = new LRUReplacer(pool_size);

  // Initially, every page is in the free list.
  for (size_t i = 0; i < pool_size_; ++i) {
    free_list_.emplace_back(static_cast<int>(i));  // i is the frame_id
  }
}

BufferPoolManager::~BufferPoolManager() {
  delete[] pages_;
  delete replacer_;
}

// 外部调用他需要加锁
frame_id_t BufferPoolManager::findVictimPage() {
  frame_id_t frameId = INVALID_PAGE_ID;
  if (free_list_.empty()) {
    bool res = replacer_->Victim(&frameId);
    if (!res) {
      return INVALID_PAGE_ID;
    }
    return frameId;
  }
  frameId = free_list_.back();
  free_list_.pop_back();
  return frameId;
}

Page *BufferPoolManager::FetchPageImpl(page_id_t page_id) {
  // 1.     Search the page table for the requested page (P).
  // 1.1    If P exists, pin it and return it immediately.
  // 1.2    If P does not exist, find a replacement page (R) from either the free list or the replacer.
  //        Note that pages are always found from the free list first.
  // 2.     If R is dirty, write it back to the disk.
  // 3.     Delete R from the page table and insert P.
  // 4.     Update P's metadata, read in the page content from disk, and then return a pointer to P.
  // if (page_id == INVALID_PAGE_ID) return nullptr;
  std::lock_guard<std::mutex> lk(latch_);
  frame_id_t frameId = INVALID_PAGE_ID;

  auto it = page_table_.find(page_id);
  if (it != page_table_.end()) {
    frameId = it->second;
    auto &page = pages_[frameId];
    if (page.GetPinCount() == 0) {
      replacer_->Pin(frameId);
    }
    page.pin_count_ += 1;
    return &page;
  }
  frameId = findVictimPage();

  if (frameId == INVALID_PAGE_ID) {
    // std::cout << "Fetch page id" << page_id << " failure " << std::endl;
    // std::cout << free_list_.size() << " / " << replacer_->Size() << " / " << std::endl;
    // throw Exception("findVictim Page 有问题在FetchPageImpl, 有过多page没有unpin");
    return nullptr;
  }
  auto &page = pages_[frameId];
  page_id_t old_page_id = page.page_id_;
  if (page.IsDirty()) {
    disk_manager_->WritePage(old_page_id, page.GetData());
  }
  page_table_.erase(old_page_id);
  replacer_->Pin(frameId);
  page_table_.insert({page_id, frameId});
  // page->ResetMemory();
  page.is_dirty_ = false;
  page.page_id_ = page_id;
  page.pin_count_ = 1;
  disk_manager_->ReadPage(page.page_id_, page.GetData());

  return &page;
}

bool BufferPoolManager::UnpinPageImpl(page_id_t page_id, bool is_dirty) {
  std::lock_guard<std::mutex> lk(latch_);
  auto it = page_table_.find(page_id);
  if (it == page_table_.end()) {
    return false;
  }
  frame_id_t frameId = it->second;
  auto &page = pages_[frameId];
  // BUSTUB_ASSERT(page.pin_count_ > 0, "the page being unpinning should be pinned before");
  if (page.pin_count_ <= 0) {
    return false;
  }
  page.is_dirty_ = page.is_dirty_ || is_dirty;

  page.pin_count_ -= 1;

  if (page.pin_count_ == 0) {
    // std::cout << "Unpin page " << page_id << std::endl;
    replacer_->Unpin(frameId);
  }
  return true;
}

// 外部使用这个函数需要加锁
bool BufferPoolManager::FlushPageImpl(page_id_t page_id) {
  std::lock_guard<std::mutex> lk(latch_);
  // Make sure you call DiskManager::WritePage!

  auto it = page_table_.find(page_id);
  if (it == page_table_.end()) {
    return false;
  }
  auto frameId = it->second;
  // WritePage需不需要检查page的数据是否超过4096
  auto &page = pages_[frameId];

  disk_manager_->WritePage(page.GetPageId(), page.GetData());

  page.is_dirty_ = false;

  return true;
}

Page *BufferPoolManager::NewPageImpl(page_id_t *page_id) {
  // 0.   Make sure you call DiskManager::AllocatePage!
  // 1.   If all the pages in the buffer pool are pinned, return nullptr.
  // 2.   Pick a victim page P from either the free list or the replacer. Always pick from the free list first.
  // 3.   Update P's metadata, zero out memory and add P to the page table.
  // 4.   Set the page ID output parameter. Return a pointer to P.
  std::lock_guard<std::mutex> lk(latch_);
  bool notunpinned = true;
  for (size_t index = 0; index < pool_size_; ++index) {
    if (pages_[index].GetPinCount() == 0) {
      notunpinned = false;
      break;
    }
  }
  if (notunpinned) {
    return nullptr;
  }
  page_id_t pageId = disk_manager_->AllocatePage();
  frame_id_t victimId = findVictimPage();
  if (victimId == INVALID_PAGE_ID) {
    // throw Exception("findVictimPage有问题在NewPageImpl, 有过多页面用完没unpin");
    return nullptr;
  }
  auto &page = pages_[victimId];
  if (page.IsDirty()) {
    disk_manager_->WritePage(page.page_id_, page.GetData());
  }
  replacer_->Pin(victimId);
  page_table_.erase(page.page_id_);
  page.page_id_ = pageId;

  // new一个新页面，该页面一开始不是dirty。但一开始需要pin
  page.pin_count_ = 1;
  page.ResetMemory();
  page.is_dirty_ = false;
  page_table_[pageId] = victimId;
  *page_id = pageId;
  return &page;
}

bool BufferPoolManager::DeletePageImpl(page_id_t page_id) {
  // 0.   Make sure you call DiskManager::DeallocatePage!
  // 1.   Search the page table for the requested page (P).
  // 1.   If P does not exist, return true.
  // 2.   If P exists, but has a non-zero pin-count, return false. Someone is using the page.
  // 3.   Otherwise, P can be deleted. Remove P from the page table, reset its metadata and return it to the free list.
  std::lock_guard<std::mutex> lk(latch_);
  if (page_id == INVALID_PAGE_ID) {
    return false;
  }

  disk_manager_->DeallocatePage(page_id);
  auto it = page_table_.find(page_id);
  if (it == page_table_.end()) {
    return true;
  }
  auto frameId = it->second;

  auto &page = pages_[frameId];
  if (page.pin_count_ != 0) {
    return false;
  }
  replacer_->Pin(frameId);
  // page.pin_count_ = 0;
  free_list_.push_back(frameId);
  page_table_.erase(page_id);
  // page.page_id_ = INVALID_PAGE_ID;

  page.is_dirty_ = false;
  page.ResetMemory();

  return true;
}

void BufferPoolManager::FlushAllPagesImpl() {
  // You can do it!
  std::lock_guard<std::mutex> lk(latch_);
  for (auto &it : page_table_) {
    auto &page = pages_[it.second];
    disk_manager_->WritePage(page.GetPageId(), page.GetData());
  }
}

}  // namespace bustub
