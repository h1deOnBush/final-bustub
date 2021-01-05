//===----------------------------------------------------------------------===//
//
//                         CMU-DB Project (15-445/645)
//                         ***DO NO SHARE PUBLICLY***
//
// Identification: src/include/index/index_iterator.h
//
// Copyright (c) 2018, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//
/**
 * index_iterator.h
 * For range scan of b+ tree
 */
#pragma once
#include "storage/page/b_plus_tree_leaf_page.h"

namespace bustub {

#define INDEXITERATOR_TYPE IndexIterator<KeyType, ValueType, KeyComparator>

INDEX_TEMPLATE_ARGUMENTS
class IndexIterator {
 public:
  // you may define your own constructor based on your member variable
  IndexIterator(page_id_t pageId, int index, BufferPoolManager *buffer_pool_manager);
  explicit IndexIterator(bool is_end);
  ~IndexIterator();

  bool isEnd() const;

  const MappingType &operator*();

  IndexIterator &operator++();

  bool operator==(const IndexIterator &itr) const {
    return (isEnd() && itr.isEnd()) || (pageId_ == itr.GetPageId() && index_ == itr.index_);
  }

  bool operator!=(const IndexIterator &itr) const { return !(*this == itr); }

  page_id_t GetPageId() const {
    if (is_end_) {
      return INVALID_PAGE_ID;
    }
    return pageId_;
  }

  int GetIndex() const { return index_; }

 private:
  // add your own private member variables here
  int size;
  page_id_t pageId_;
  int index_;
  BufferPoolManager *buffer_pool_manager_;
  bool is_end_{false};
};

}  // namespace bustub
