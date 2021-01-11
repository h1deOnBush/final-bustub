/**
 * index_iterator.cpp
 */
#include <cassert>

#include "storage/index/index_iterator.h"

namespace bustub {

/*
 * NOTE: you can change the destructor/constructor method here
 * set your own input parameters
 */
INDEX_TEMPLATE_ARGUMENTS
INDEXITERATOR_TYPE::IndexIterator(page_id_t pageId, int index, int size, BufferPoolManager *buffer_pool_manager)
    : pageId_(pageId), index_(index), size_(size), buffer_pool_manager_(buffer_pool_manager) {}

INDEX_TEMPLATE_ARGUMENTS
INDEXITERATOR_TYPE::IndexIterator(bool is_end) : is_end_(is_end) {}
INDEX_TEMPLATE_ARGUMENTS
INDEXITERATOR_TYPE::~IndexIterator() = default;

INDEX_TEMPLATE_ARGUMENTS
bool INDEXITERATOR_TYPE::isEnd() const { return is_end_; }

INDEX_TEMPLATE_ARGUMENTS
const MappingType &INDEXITERATOR_TYPE::operator*() {
  if (is_end_) {
    throw Exception(ExceptionType::OUT_OF_RANGE,
                    "reference iterator "
                    "object that is out of the end in operator* function");
  }
  auto *page = buffer_pool_manager_->FetchPage(pageId_);
  auto *leafnode = reinterpret_cast<B_PLUS_TREE_LEAF_PAGE_TYPE *>(page->GetData());
  const MappingType &item = leafnode->GetItem(index_);
  // std::cout << "index is " << index_ << " page id is " << pageId_ <<  " iterating get the key " << item.first << std::endl;
  buffer_pool_manager_->UnpinPage(page->GetPageId(), false);
  return item;
}

INDEX_TEMPLATE_ARGUMENTS
INDEXITERATOR_TYPE &INDEXITERATOR_TYPE::operator++() {
  if (is_end_) {
    throw Exception(ExceptionType::OUT_OF_RANGE,
                    "++ object that is out of "
                    "the end in operator++ function");
  }
  index_ += 1;
  if (index_ >= size_) {
    auto *page = buffer_pool_manager_->FetchPage(pageId_);
    auto *leafnode = reinterpret_cast<B_PLUS_TREE_LEAF_PAGE_TYPE *>(page->GetData());
    auto next_pageId = leafnode->GetNextPageId();
    page->RUnlatch();
    buffer_pool_manager_->UnpinPage(pageId_, false);
    // buffer_pool_manager_->UnpinPage(pageId_, false);
    pageId_ = next_pageId;
    if (pageId_ == INVALID_PAGE_ID) {
      is_end_ = true;
    } else {
      auto *newpage = buffer_pool_manager_->FetchPage(pageId_);
      newpage->RLatch();
      auto new_leafnode = reinterpret_cast<B_PLUS_TREE_LEAF_PAGE_TYPE *>(newpage->GetData());
      size_ = new_leafnode->GetSize();
      buffer_pool_manager_->UnpinPage(newpage->GetPageId(), false);
      index_ = 0;
    }
  }
  return *this;
}

template class IndexIterator<GenericKey<4>, RID, GenericComparator<4>>;

template class IndexIterator<GenericKey<8>, RID, GenericComparator<8>>;

template class IndexIterator<GenericKey<16>, RID, GenericComparator<16>>;

template class IndexIterator<GenericKey<32>, RID, GenericComparator<32>>;

template class IndexIterator<GenericKey<64>, RID, GenericComparator<64>>;

}  // namespace bustub
