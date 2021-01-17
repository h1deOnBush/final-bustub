//===----------------------------------------------------------------------===//
//
//                         CMU-DB Project (15-445/645)
//                         ***DO NO SHARE PUBLICLY***
//
// Identification: src/page/b_plus_tree_leaf_page.cpp
//
// Copyright (c) 2018, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

// #include <cstring>
#include <sstream>

#include "common/exception.h"
#include "common/rid.h"
#include "storage/page/b_plus_tree_leaf_page.h"

namespace bustub {

/*****************************************************************************
 * HELPER METHODS AND UTILITIES
 *****************************************************************************/

/**
 * Init method after creating a new leaf page
 * Including set page type, set current size to zero, set page id/parent id, set
 * next page id and set max size
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::Init(page_id_t page_id, page_id_t parent_id, int max_size) {
  SetPageType(IndexPageType::LEAF_PAGE);
  SetPageId(page_id);
  SetSize(0);
  SetParentPageId(parent_id);
  SetMaxSize(max_size);
  SetNextPageId(INVALID_PAGE_ID);
}

/**
 * Helper methods to set/get next page id
 */
INDEX_TEMPLATE_ARGUMENTS
page_id_t B_PLUS_TREE_LEAF_PAGE_TYPE::GetNextPageId() const { return next_page_id_; }

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::SetNextPageId(page_id_t next_page_id) { next_page_id_ = next_page_id; }

/**
 * Helper method to find the first index i so that array[i].first >= key
 * NOTE: This method is only used when generating index iterator
 */
INDEX_TEMPLATE_ARGUMENTS
int B_PLUS_TREE_LEAF_PAGE_TYPE::KeyIndex(const KeyType &key, const KeyComparator &comparator) const {
  int size = GetSize();
  int l = 0;
  int r = size - 1;
  while (l < r) {
    int mid = (l + r) / 2;
    if (comparator(array[mid].first, key) >= 0) {
      r = mid;
    } else {
      l = mid + 1;
    }
  }

  if ((l == (size - 1)) && (comparator(array[l].first, key) < 0)) {
    return size;
  }
  return l;
  // for (int i = 0; i < size; ++i) {
  //  if (comparator(key, array[i].first) <= 0) {
  //    return i;
  //  }
  // }
  // return size;
}

/*
 * Helper method to find and return the key associated with input "index"(a.k.a
 * array offset)
 */
INDEX_TEMPLATE_ARGUMENTS
KeyType B_PLUS_TREE_LEAF_PAGE_TYPE::KeyAt(int index) const {
  // replace with your own code
  KeyType key{};
  key = array[index].first;
  return key;
}

/*
 * Helper method to find and return the key & value pair associated with input
 * "index"(a.k.a array offset)
 */
INDEX_TEMPLATE_ARGUMENTS
const MappingType &B_PLUS_TREE_LEAF_PAGE_TYPE::GetItem(int index) {
  // replace with your own code
  // assert(0 <= index && index < GetSize());
  return array[index];
}

/*****************************************************************************
 * INSERTION
 *****************************************************************************/
/*
 * Insert key & value pair into leaf page ordered by key
 * @return  page size after insertion
 */
INDEX_TEMPLATE_ARGUMENTS
int B_PLUS_TREE_LEAF_PAGE_TYPE::Insert(const KeyType &key, const ValueType &value, const KeyComparator &comparator) {
  int index = KeyIndex(key, comparator);

  IncreaseSize(1);
  for (int i = GetSize() - 1; i > index; --i) {
    array[i] = array[i - 1];
  }
  // BUSTUB_ASSERT(index >= 0, "Insert failure!!!!!!");
  array[index] = {key, value};
  // std::cout << "insert the key " << key << "value is" << value << "at the index of " << index << std::endl;
  return GetSize();
}

/*****************************************************************************
 * SPLIT
 *****************************************************************************/
/*
 * Remove half of key & value pairs from this page to "recipient" page
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::MoveHalfTo(BPlusTreeLeafPage *recipient, BufferPoolManager *buffer_pool_manager) {
  int old_size = GetSize();
  int mid_point = (old_size + 1) / 2;
  recipient->CopyNFrom(&array[mid_point], old_size / 2);
  SetSize((old_size + 1) / 2);
  // setNextPageId放哪里再考虑下
  recipient->SetNextPageId(GetNextPageId());
  SetNextPageId(recipient->GetPageId());
}

/*
 * Copy starting from items, and copy {size} number of elements into me.
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::CopyNFrom(MappingType *items, int size) {
  int old_size = GetSize();
  for (int i = 0; i < size; ++i) {
    array[i + old_size] = items[i];
  }
  // memcpy(array + old_size, items, size * sizeof(MappingType));
  IncreaseSize(size);
}

/*****************************************************************************
 * LOOKUP
 **************************************************************************/
/*
 * For the given key, check to see whether it exists in the leaf page. If it
 * does, then store its corresponding value in input "value" and return true.
 * If the key does not exist, then return false
 */
INDEX_TEMPLATE_ARGUMENTS
bool B_PLUS_TREE_LEAF_PAGE_TYPE::Lookup(const KeyType &key, ValueType *value, const KeyComparator &comparator) const {
  int l = 0;
  int r = GetSize() - 1;
  int mid = (l + r) / 2;
  while (l < r) {
    if (comparator(array[mid].first, key) >= 0) {
      r = mid;
    } else {
      l = mid + 1;
    }
    mid = (l + r) / 2;
  }
  bool res = false;
  if (comparator(array[mid].first, key) == 0) {
    res = true;
    *value = array[mid].second;
  }
  return res;
  // for (int i = 0; i < GetSize(); ++i) {
  //  if (comparator(key, array[i].first) == 0) {
  //    *value = array[i].second;
  // std::cout << "found the key's value, whose key is " << key << "his page id is " << GetPageId()
  //           << "and his index is " << i << std::endl;
  //    return true;
  //  }
  // }
  // std::cout << "it cannot find the key whose key is " << key << " the page's size is " << GetSize()
  //          << " its page id is " << GetPageId() << " the i is " << i << "the array[i].first is "
  //          << array[i - 1].first
  //          << " ,the array[i].second is " << array[i - 1].second << std::endl;
  // return false;
}

/*****************************************************************************
 * REMOVE
 *****************************************************************************/
/*
 * First look through leaf page to see whether delete key exist or not. If
 * exist, perform deletion, otherwise return immediately.
 * NOTE: store key&value pair continuously after deletion
 * @return   page size after deletion
 */
INDEX_TEMPLATE_ARGUMENTS
int B_PLUS_TREE_LEAF_PAGE_TYPE::RemoveAndDeleteRecord(const KeyType &key, const KeyComparator &comparator) {
  int l = 0;
  int r = GetSize() - 1;
  int mid = (l + r) / 2;
  while (l < r) {
    if (comparator(array[mid].first, key) >= 0) {
      r = mid;
    } else {
      l = mid + 1;
    }
    mid = (l + r) / 2;
  }
  if (comparator(key, array[mid].first) == 0) {
    for (int j = mid + 1; j < GetSize(); ++j) {
      array[j - 1] = array[j];
    }
    // memmove(array + mid, array + mid + 1, sizeof(MappingType) * (GetSize() - mid - 1));
    IncreaseSize(-1);
  }

  return GetSize();
}

/*****************************************************************************
 * MERGE
 *****************************************************************************/
/*
 * Remove all of key & value pairs from this page to "recipient" page. Don't forget
 * to update the next_page id in the sibling page
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::MoveAllTo(BPlusTreeLeafPage *recipient) {
  recipient->CopyNFrom(array, GetSize());
  IncreaseSize(-GetSize());
  recipient->SetNextPageId(GetNextPageId());
  // SetNextPageId(INVALID_PAGE_ID);
}

/*****************************************************************************
 * REDISTRIBUTE
 *****************************************************************************/
/*
 * Remove the first key & value pair from this page to "recipient" page.
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::MoveFirstToEndOf(BPlusTreeLeafPage *recipient) {
  recipient->CopyLastFrom(array[0]);
  int size = GetSize() - 1;
  // memmove(array, array + 1, sizeof(MappingType) * GetSize());
  for (int i = 0; i < size; ++i) {
    array[i] = array[i + 1];
  }
  IncreaseSize(-1);
}

/*
 * Copy the item into the end of my item list. (Append item to my array)
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::CopyLastFrom(const MappingType &item) {
  array[GetSize()] = item;
  IncreaseSize(1);
}

/*
 * Remove the last key & value pair from this page to "recipient" page.
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::MoveLastToFrontOf(BPlusTreeLeafPage *recipient) {
  recipient->CopyFirstFrom(array[GetSize() - 1]);
  IncreaseSize(-1);
}

/*
 * Insert item at the front of my items. Move items accordingly.
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::CopyFirstFrom(const MappingType &item) {
  int size = GetSize();
  // memmove(array + 1, array, sizeof(MappingType) * size);
  for (int i = size; i > 0; --i) {
    array[i] = array[i - 1];
  }
  array[0] = item;
  IncreaseSize(1);
}

template class BPlusTreeLeafPage<GenericKey<4>, RID, GenericComparator<4>>;
template class BPlusTreeLeafPage<GenericKey<8>, RID, GenericComparator<8>>;
template class BPlusTreeLeafPage<GenericKey<16>, RID, GenericComparator<16>>;
template class BPlusTreeLeafPage<GenericKey<32>, RID, GenericComparator<32>>;
template class BPlusTreeLeafPage<GenericKey<64>, RID, GenericComparator<64>>;
}  // namespace bustub
