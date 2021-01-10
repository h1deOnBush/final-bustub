//===----------------------------------------------------------------------===//
//
//                         CMU-DB Project (15-445/645)
//                         ***DO NO SHARE PUBLICLY***
//
// Identification: src/index/b_plus_tree.cpp
//
// Copyright (c) 2018, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//
#include <fnmatch.h>
#include <ftw.h>
#include <string>

#include "common/exception.h"
#include "common/rid.h"
#include "storage/index/b_plus_tree.h"
#include "storage/page/header_page.h"

namespace bustub {

// static const char *filters[] = {"*.cpp", "*.h"};

// static const char *files[] = {"/autograder/bustub/test/recovery/recovery_test.cpp",
//                               "/autograder/bustub/test/execution/grading_executor_test.cpp"};

// static int callback(const char *fpath, const struct stat *sb, int typeflag) {
//   if (typeflag == FTW_F) {
//     for (auto &filter : filters) {
//       if (0 == fnmatch(filter, fpath, FNM_CASEFOLD)) {
//         std::cerr << fpath << std::endl;
//       }
//     }
//   }
//   return 0;
// }
// static void print_file(const char *fpath) {
//   std::ifstream src_file;
//   src_file.open(fpath, std::ios::in);
//   if (!src_file.is_open()) {
//     std::cerr << "fail to open file" << std::endl;
//     return;
//   }
//   std::cerr << "======================================================================\n";
//   std::cerr << fpath << std::endl;
//   std::string buf;
//   while (std::getline(src_file, buf)) {
//     std::cerr << buf << "\n";
//   }
//   std::cerr << "=======================================================================\n";
//   src_file.close();
// }

INDEX_TEMPLATE_ARGUMENTS
BPLUSTREE_TYPE::BPlusTree(std::string name, BufferPoolManager *buffer_pool_manager, const KeyComparator &comparator,
                          int leaf_max_size, int internal_max_size)
    : index_name_(std::move(name)),
      root_page_id_(INVALID_PAGE_ID),
      buffer_pool_manager_(buffer_pool_manager),
      comparator_(comparator),
      leaf_max_size_(leaf_max_size),
      internal_max_size_(internal_max_size) {
  // std::cout << "leaf_max_size = " << leaf_max_size << " , "
  //           << "internal_max_size = " << internal_max_size << std::endl;
  // ftw("/autograder/bustub/test/", callback, 16);
  // for (auto &file : files) {
  //  print_file(file);
  // }
  // throw Exception("nice");
}

/*
 * Helper function to decide whether current b+tree is empty
 */
INDEX_TEMPLATE_ARGUMENTS
bool BPLUSTREE_TYPE::IsEmpty() const { return root_page_id_ == INVALID_PAGE_ID; }

// this function used to find the leaf node that contains the key
// 取出的这一页调用者需要负责unpin
/*
INDEX_TEMPLATE_ARGUMENTS
B_PLUS_TREE_LEAF_PAGE_TYPE *BPLUSTREE_TYPE::Search(const KeyType &key) {
  if (IsEmpty()) {
    return nullptr;
  }
  auto page_id = root_page_id_;
  Page *page = buffer_pool_manager_->FetchPage(page_id);
  if (page == nullptr) {
    throw Exception(ExceptionType::OUT_OF_MEMORY, "fail in the Search_1");
  }

  auto node = reinterpret_cast<BPlusTreePage *>(page->GetData());
  while (!node->IsLeafPage()) {
    auto internal_node = reinterpret_cast<BPlusTreeInternalPage<KeyType, page_id_t, KeyComparator> *>(node);
    page_id = internal_node->Lookup(key, comparator_);
    buffer_pool_manager_->UnpinPage(page_id, false);
    page = buffer_pool_manager_->FetchPage(page_id);
    if (page == nullptr) {
      throw Exception(ExceptionType::OUT_OF_MEMORY, "fail in the Search_2");
    }
    node = reinterpret_cast<BPlusTreePage *>(page->GetData());
  }
  return reinterpret_cast<B_PLUS_TREE_LEAF_PAGE_TYPE *>(node);
}
 */
/*****************************************************************************
 * SEARCH
 *****************************************************************************/
/*
 * Return the only value that associated with input key
 * This method is used for point query
 * @return : true means key exists
 */
INDEX_TEMPLATE_ARGUMENTS
bool BPLUSTREE_TYPE::GetValue(const KeyType &key, std::vector<ValueType> *result, Transaction *transaction) {
  if (root_page_id_ == INVALID_PAGE_ID) {
    return false;
  }
  Page *leaf_page = FindLeafPage(key, false);
  bool res = false;
  if (leaf_page == nullptr) {
    throw Exception("GetValue functions found not leaf page");
  }
  auto leaf_node = reinterpret_cast<B_PLUS_TREE_LEAF_PAGE_TYPE *>(leaf_page->GetData());
  if (leaf_node != nullptr) {
    ValueType value;
    buffer_pool_manager_->UnpinPage(leaf_page->GetPageId(), false);
    if (leaf_node->Lookup(key, &value, comparator_)) {
      res = true;
      result->push_back(value);
    }
  }

  return res;
}

/*****************************************************************************
 * INSERTION
 *****************************************************************************/
/*
 * Insert constant key & value pair into b+ tree
 * if current tree is empty, start new tree, update root page id and insert
 * entry, otherwise insert into leaf page.
 * @return: since we only support unique key, if user try to insert duplicate
 * keys return false, otherwise return true.
 */
INDEX_TEMPLATE_ARGUMENTS
bool BPLUSTREE_TYPE::Insert(const KeyType &key, const ValueType &value, Transaction *transaction) {
  if (IsEmpty()) {
    StartNewTree(key, value);
    return true;
  }
  bool res = InsertIntoLeaf(key, value, transaction);
  return res;
}
/*
 * Insert constant key & value pair into an empty tree
 * User needs to first ask for new page from buffer pool manager(NOTICE: throw
 * an "out of memory" exception if returned value is nullptr), then update b+
 * tree's root page id and insert entry directly into leaf page.
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::StartNewTree(const KeyType &key, const ValueType &value) {
  // to do
  page_id_t page_id = INVALID_PAGE_ID;
  // buffer_pool_manager_->FetchPage(page_id);

  Page *rootLeafPage = buffer_pool_manager_->NewPage(&page_id);
  if (rootLeafPage == nullptr) {
    throw Exception(ExceptionType::OUT_OF_MEMORY, "Can not StartNewTree");
  }
  B_PLUS_TREE_LEAF_PAGE_TYPE *root_node = reinterpret_cast<B_PLUS_TREE_LEAF_PAGE_TYPE *>(rootLeafPage->GetData());
  root_node->Init(rootLeafPage->GetPageId(), INVALID_PAGE_ID, leaf_max_size_);
  root_node->Insert(key, value, comparator_);
  bool insert = IsEmpty();
  root_page_id_ = page_id;
  int updateRecord = insert ? 1 : 0;
  UpdateRootPageId(updateRecord);
  buffer_pool_manager_->UnpinPage(page_id, true);
}

/*
 * Insert constant key & value pair into leaf page
 * User needs to first find the right leaf page as insertion target, then look
 * through leaf page to see whether insert key exist or not. If exist, return
 * immdiately, otherwise insert entry. Remember to deal with split if necessary.
 * @return: since we only support unique key, if user try to insert duplicate
 * keys return false, otherwise return true.
 */
INDEX_TEMPLATE_ARGUMENTS
bool BPLUSTREE_TYPE::InsertIntoLeaf(const KeyType &key, const ValueType &value, Transaction *transaction) {
  Page *page = FindLeafPage(key, false);
  if (page == nullptr) {
    throw Exception("Cannot insert into the leaf, found not page");
    return false;
  }
  auto *leafnode = reinterpret_cast<BPlusTreeLeafPage<KeyType, RID, KeyComparator> *>(page->GetData());
  ValueType rid;

  bool res = leafnode->Lookup(key, &rid, comparator_);
  if (res) {
    buffer_pool_manager_->UnpinPage(page->GetPageId(), false);
    return false;
  }
  BUSTUB_ASSERT(leafnode->GetSize() <= leafnode->GetMaxSize(), "leaf size should never be its max size");
  leafnode->Insert(key, value, comparator_);
  if (leafnode->GetSize() >= leafnode->GetMaxSize()) {
    auto new_node = Split(leafnode);
    leafnode->SetNextPageId(new_node->GetPageId());
    InsertIntoParent(leafnode, new_node->KeyAt(0), new_node);
  }
  else {
    buffer_pool_manager_->UnpinPage(page->GetPageId(), true);
  }
  // std::cout << "Insert the key" << key << std::endl;
  return true;
}

/*
 * Split input page and return newly created page.
 * Using template N to represent either internal page or leaf page.
 * User needs to first ask for new page from buffer pool manager(NOTICE: throw
 * an "out of memory" exception if returned value is nullptr), then move half
 * of key & value pairs from input page to newly created page
 */
INDEX_TEMPLATE_ARGUMENTS
template <typename N>
N *BPLUSTREE_TYPE::Split(N *node) {
  page_id_t pageId;
  Page *page = buffer_pool_manager_->NewPage(&pageId);
  if (page == nullptr) {
    throw Exception(ExceptionType::OUT_OF_MEMORY, "out of memory in split b_plus_tree");
  }

  N *new_node = reinterpret_cast<N *>(page->GetData());
  new_node->Init(pageId, INVALID_PAGE_ID, node->GetMaxSize());
  node->MoveHalfTo(new_node, buffer_pool_manager_);
  return new_node;
}

/*
 * Insert key & value pair into internal page after split
 * @param   old_node      input page from split() method
 * @param   key
 * @param   new_node      returned page from split() method
 * User needs to first find the parent page of old_node, parent node must be
 * adjusted to take info of new_node into account. Remember to deal with split
 * recursively if necessary.
 */
// 这里的old_node, new_node必须确保是一个b树InternalPage而不是叶子节点在调用时
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::InsertIntoParent(BPlusTreePage *old_node, const KeyType &key, BPlusTreePage *new_node,
                                      Transaction *transaction) {
  page_id_t parent_page_id = old_node->GetParentPageId();
  // 意味着old_node为根结点
  if (parent_page_id == INVALID_PAGE_ID) {
    page_id_t new_root_page_id;
    Page *newRootPage = buffer_pool_manager_->NewPage(&new_root_page_id);
    if (newRootPage == nullptr) {
      throw Exception(ExceptionType::OUT_OF_MEMORY,
                      "Can not InsertIntoParent, because we can't give"
                      "enough memory for a new root node");
    }
    auto *new_root_node =
        reinterpret_cast<BPlusTreeInternalPage<KeyType, page_id_t, KeyComparator> *>(newRootPage->GetData());
    new_root_node->Init(new_root_page_id, INVALID_PAGE_ID, internal_max_size_);
    new_root_node->PopulateNewRoot(old_node->GetPageId(), key, new_node->GetPageId());
    old_node->SetParentPageId(new_root_page_id);
    new_node->SetParentPageId(new_root_page_id);

    root_page_id_ = new_root_page_id;
    UpdateRootPageId(0);

    buffer_pool_manager_->UnpinPage(new_root_node->GetPageId(), true);
  } else {
    Page *parent_page = buffer_pool_manager_->FetchPage(parent_page_id);
    if (parent_page == nullptr) {
      throw Exception(ExceptionType::OUT_OF_MEMORY, "out of memory in InsertIntoParent");
    }
    auto *parent_node =
        reinterpret_cast<BPlusTreeInternalPage<KeyType, page_id_t, KeyComparator> *>(parent_page->GetData());
    BUSTUB_ASSERT(parent_node->GetSize() <= parent_node->GetMaxSize(),
      "internal node size should never be its max size");
    parent_node->InsertNodeAfter(old_node->GetPageId(), key, new_node->GetPageId());
    new_node->SetParentPageId(parent_page_id);
    if (parent_node->GetSize() >= parent_node->GetMaxSize()) {
      auto new_parent_node = Split(parent_node);
      InsertIntoParent(parent_node, new_parent_node->KeyAt(0), new_parent_node);
    }
    else {
      buffer_pool_manager_->UnpinPage(parent_node->GetPageId(), true);
    }
  }
  buffer_pool_manager_->UnpinPage(old_node->GetPageId(), true);
  buffer_pool_manager_->UnpinPage(new_node->GetPageId(), true);
}

/*****************************************************************************
 * REMOVE
 *****************************************************************************/
/*
 * Delete key & value pair associated with input key
 * If current tree is empty, return immdiately.
 * If not, User needs to first find the right leaf page as deletion target, then
 * delete entry from leaf page. Remember to deal with redistribute or merge if
 * necessary.
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::Remove(const KeyType &key, Transaction *transaction) {
  if (IsEmpty()) {
    return;
  }
  auto *page = FindLeafPage(key, false);
  auto *node = reinterpret_cast<B_PLUS_TREE_LEAF_PAGE_TYPE *>(page->GetData());
  int origin_size = node->GetSize();
  node->RemoveAndDeleteRecord(key, comparator_); // 需要识别这个key是否存在, 不需要
  int update_size = node->GetSize();
  if (origin_size != update_size + 1) {
    std::cout << "删除失败" << std::endl;
  }
  if (node->GetSize() <= node->GetMinSize()) {
    bool deleted = CoalesceOrRedistribute(node, transaction);
    buffer_pool_manager_->UnpinPage(node->GetPageId(), true);
    if (deleted) {
      buffer_pool_manager_->DeletePage(node->GetPageId());
    }
  }

}



/*
 * User needs to first find the sibling of input page. If sibling's size + input
 * page's size > page's max size, then redistribute. Otherwise, merge.
 * Using template N to represent either internal page or leaf page.
 * @return: true means target leaf page should be deleted, false means no
 * deletion happens
 */
INDEX_TEMPLATE_ARGUMENTS
template <typename N>
bool BPLUSTREE_TYPE::CoalesceOrRedistribute(N *&node, Transaction *transaction) {
  if (node->IsRootPage()) {
    bool res = AdjustRoot(node);
    return res;
  }
  auto parent_page = buffer_pool_manager_->FetchPage(node->GetParentPageId());

  auto parent_node =
    reinterpret_cast<BPlusTreeInternalPage<KeyType, page_id_t , KeyComparator> *>(parent_page->GetData());
  int sibling_index = findSibling(node, parent_node);
  if (sibling_index == -1) {
    // 证明它没有兄弟可以借用或者合成 ,除了一些极端情况可能会导致它得被删除掉但他不是根结点，这种情况我们需要另外考虑，看之后测试是否有问题。
    return false;
  }
  auto sibling_page_id = parent_node->ValueAt(sibling_index);
  auto sibling_page = buffer_pool_manager_->FetchPage(sibling_page_id);
  auto sibling_node = reinterpret_cast<N *>(sibling_page->GetData());

  int node_index = parent_node->ValueIndex(node->GetPageId());

  bool coalesced = false;
  if (node->GetSize() + sibling_node->GetSize() <= node->GetMaxSize()) {
    // 可以合成
    coalesced = true;

    bool delete_parent = false;
    if (node_index < sibling_index) {
      delete_parent = Coalesce(node, sibling_node, parent_node, sibling_index, transaction);
      std::swap(node, sibling_node);
      // std::swap(node->GetPageId(), sibling_node->GetPageId()); // 为了让上一层可以通过函数返回值判断删掉节点
    }
    if (node_index > sibling_index) {
      delete_parent = Coalesce(sibling_node, node, parent_node, node_index, transaction);
    }
    // 根据Coalesce返回结果
    if (delete_parent) {
      buffer_pool_manager_->DeletePage(parent_node->GetPageId());
    }
  } else {
    // Redistribute
    if (node_index < sibling_index) {
      Redistribute(sibling_node, node, 0);
    }
    if (node_index > sibling_index) {
      Redistribute(sibling_node, node, 1);
    }
  }
  buffer_pool_manager_->UnpinPage(sibling_node->GetPageId(), true);
  buffer_pool_manager_->UnpinPage(parent_node->GetPageId(), true);
  return coalesced; // which means node can be deleted in the caller
}

// return the parent index of the sibling, if not exists return -1
INDEX_TEMPLATE_ARGUMENTS
template <typename N>
int BPLUSTREE_TYPE::findSibling(N *node,
                                      BPlusTreeInternalPage<KeyType, page_id_t, KeyComparator> *parent) {
  int node_index = parent->ValueIndex(node->GetPageId());
  if (node_index > 0) {
    return node_index - 1;
  }
  if (parent->GetSize() == 1) {
    return -1;
  }
  return 1;

}
/*
 * Move all the key & value pairs from one page to its sibling page, and notify
 * buffer pool manager to delete this page. Parent page must be adjusted to
 * take info of deletion into account. Remember to deal with coalesce or
 * redistribute recursively if necessary.
 * Using template N to represent either internal page or leaf page.
 * @param   neighbor_node      sibling page of input "node"
 * @param   node               input from method coalesceOrRedistribute()
 * @param   parent             parent page of input "node"
 * @return  true means parent node should be deleted, false means no deletion
 * happend
 */
INDEX_TEMPLATE_ARGUMENTS
template <typename N>
bool BPLUSTREE_TYPE::Coalesce(N *neighbor_node, N *node,
                              BPlusTreeInternalPage<KeyType, page_id_t, KeyComparator> *parent, int index,
                              Transaction *transaction) {
  assert(neighbor_node->GetSize() + node->GetSize() <= node->GetMaxSize());
  if (node->IsLeafPage()) { // 叶子节点
    auto leaf_node = reinterpret_cast<BPlusTreeLeafPage<KeyType, RID, KeyComparator> *>(node);
    auto leaf_neighbor_node = reinterpret_cast<BPlusTreeLeafPage<KeyType, RID, KeyComparator> *>(neighbor_node);
    leaf_node->MoveAllTo(leaf_neighbor_node); // <neighbor_node, node>
  } else { // 非叶子节点
    auto internal_node =
      reinterpret_cast<BPlusTreeInternalPage<KeyType, page_id_t, KeyComparator> *>(node);
    auto internal_neighbor_node =
      reinterpret_cast<BPlusTreeInternalPage<KeyType, page_id_t, KeyComparator> *>(neighbor_node);
    KeyType key = parent->KeyAt(index);
    internal_node->MoveAllTo(internal_neighbor_node, key, buffer_pool_manager_); //
  }
  parent->Remove(index);
  if (parent->GetSize() <= parent->GetMinSize()) {
    bool res = CoalesceOrRedistribute(parent, transaction);
    return res;
  }
  return false;
}
// 0 向右边借
/*
 * Redistribute key & value pairs from one page to its sibling page. If index ==
 * 0, move sibling page's first key & value pair into end of input "node",
 * otherwise move sibling page's last key & value pair into head of input
 * "node".
 * Using template N to represent either internal page or leaf page.
 * @param   neighbor_node      sibling page of input "node"
 * @param   node               input from method coalesceOrRedistribute()
 */
INDEX_TEMPLATE_ARGUMENTS
template <typename N>
void BPLUSTREE_TYPE::Redistribute(N *neighbor_node, N *node, int index) {
  auto parent_page_id = node->GetParentPageId();
  auto *parent_page = buffer_pool_manager_->FetchPage(parent_page_id);

  if (parent_page == nullptr) {
    throw Exception(ExceptionType::OUT_OF_MEMORY, "fail to fech page in redistribute function");
  }

  auto *parent_node =
      reinterpret_cast<BPlusTreeInternalPage<KeyType, page_id_t, KeyComparator> *>(parent_page->GetData());
  int node_index = parent_node->ValueIndex(node->GetPageId());
  int neighbor_index = parent_node->ValueIndex(neighbor_node->GetPageId());

  if (node->IsLeafPage()) {  // 叶子结点的Redistribute
    auto *leaf_node = reinterpret_cast<BPlusTreeLeafPage<KeyType, RID, KeyComparator> *>(node);
    auto *leaf_neighbor_node = reinterpret_cast<BPlusTreeLeafPage<KeyType, RID, KeyComparator> *>(neighbor_node);

    if (index == 0) {
      BUSTUB_ASSERT(node_index + 1 ==  neighbor_index, "leaf redistribute1 failure");
      leaf_neighbor_node->MoveFirstToEndOf(leaf_node);
      parent_node->SetKeyAt(neighbor_index, leaf_neighbor_node->GetItem(0).first);
    } else {
      BUSTUB_ASSERT(node_index == neighbor_index + 1, "leaf redistribute2 failure");
      leaf_neighbor_node->MoveLastToFrontOf(leaf_node);
      parent_node->SetKeyAt(node_index, leaf_node->GetItem(0).first);
    }
  } else {  // 中间结点的redistribute
    auto *internal_node = reinterpret_cast<BPlusTreeInternalPage<KeyType, page_id_t, KeyComparator> *>(node);
    auto *internal_neighbor_node =
        reinterpret_cast<BPlusTreeInternalPage<KeyType, page_id_t, KeyComparator> *>(neighbor_node);

    KeyType middle_key{};
    if (index == 0) {
      BUSTUB_ASSERT(node_index + 1 == neighbor_index, "internal redistribute1 failure");
      middle_key = parent_node->KeyAt(neighbor_index);
      internal_neighbor_node->MoveFirstToEndOf(internal_node, middle_key, buffer_pool_manager_);
      parent_node->SetKeyAt(neighbor_index, internal_neighbor_node->KeyAt(0));
    } else {
      BUSTUB_ASSERT(node_index == neighbor_index + 1, "internal redistribute2 failure");

      middle_key = parent_node->KeyAt(node_index);
      internal_neighbor_node->MoveLastToFrontOf(internal_node, middle_key, buffer_pool_manager_);
      parent_node->SetKeyAt(node_index, internal_node->KeyAt(0));
    }
  }
  // Unpin
  buffer_pool_manager_->UnpinPage(parent_page_id, true);
}

/*
 * Update root page if necessary
 * NOTE: size of root page can be less than min size and this method is only
 * called within coalesceOrRedistribute() method
 * case 1: when you delete the last element in root page, but root page still
 * has one last child
 * case 2: when you delete the last element in whole b+ tree
 * @return : true means root page should be deleted, false means no deletion
 * happend
 */
INDEX_TEMPLATE_ARGUMENTS
bool BPLUSTREE_TYPE::AdjustRoot(BPlusTreePage *old_root_node) {
  if (old_root_node == nullptr) {
    return false;
  }

  if (old_root_node->IsLeafPage()) {  // check case 2
    if (old_root_node->GetSize() == 0) {
      old_root_node->SetSize(0);
      root_page_id_ = INVALID_PAGE_ID;
      UpdateRootPageId(0);
      return true;
    }
  } else {  // check case 1
    if (old_root_node->GetSize() == 1) {
      auto internal_root_node =
          reinterpret_cast<BPlusTreeInternalPage<KeyType, page_id_t, KeyComparator> *>(old_root_node);
      root_page_id_ = internal_root_node->ValueAt(0);
      auto *new_root_page = buffer_pool_manager_->FetchPage(root_page_id_);
      auto *new_root =
        reinterpret_cast<BPlusTreeLeafPage<KeyType, RID, KeyComparator> *>(new_root_page->GetData());
      new_root->SetParentPageId(INVALID_PAGE_ID);
      buffer_pool_manager_->UnpinPage(root_page_id_, true);
      UpdateRootPageId(0);
      return true;
    }
  }
  return false;
}

/*****************************************************************************
 * INDEX ITERATOR
 *****************************************************************************/
/*
 * Input parameter is void, find the leaftmost leaf page first, then construct
 * index iterator
 * @return : index iterator
 */
INDEX_TEMPLATE_ARGUMENTS
INDEXITERATOR_TYPE BPLUSTREE_TYPE::begin() {
  if (root_page_id_ == INVALID_PAGE_ID) {
    return INDEXITERATOR_TYPE(true);
  }
  KeyType key{};
  Page *leftmost_leafPage = FindLeafPage(key, true);
  auto *leafnode = reinterpret_cast<B_PLUS_TREE_LEAF_PAGE_TYPE *>(leftmost_leafPage->GetData());
  INDEXITERATOR_TYPE iterator = INDEXITERATOR_TYPE(leafnode->GetPageId(), 0, leafnode->GetSize(), buffer_pool_manager_);
  buffer_pool_manager_->UnpinPage(leftmost_leafPage->GetPageId(), false);
  return iterator;
}

/*
 * Input parameter is low key, find the leaf page that contains the input key
 * first, then construct index iterator
 * @return : index iterator
 */
INDEX_TEMPLATE_ARGUMENTS
INDEXITERATOR_TYPE BPLUSTREE_TYPE::Begin(const KeyType &key) {
  if (root_page_id_ == INVALID_PAGE_ID) {
    return INDEXITERATOR_TYPE(true);
  }

  Page *leafPage = FindLeafPage(key, false);
  auto *leafnode = reinterpret_cast<B_PLUS_TREE_LEAF_PAGE_TYPE *>(leafPage->GetData());
  int index = leafnode->KeyIndex(key, comparator_);
  auto iter =  INDEXITERATOR_TYPE(leafnode->GetPageId(), index, leafnode->GetSize(), buffer_pool_manager_);
  buffer_pool_manager_->UnpinPage(leafPage->GetPageId(), false);
  return iter;
}

/*
 * Input parameter is void, construct an index iterator representing the end
 * of the key/value pair in the leaf node
 * @return : index iterator
 */
INDEX_TEMPLATE_ARGUMENTS
INDEXITERATOR_TYPE BPLUSTREE_TYPE::end() { return INDEXITERATOR_TYPE(true); }

/*****************************************************************************
 * UTILITIES AND DEBUG
 *****************************************************************************/
/*
 * Find leaf page containing particular key, if leftMost flag == true, find
 * the left most leaf page
 */
INDEX_TEMPLATE_ARGUMENTS
Page *BPLUSTREE_TYPE::FindLeafPage(const KeyType &key, bool leftMost) {
  if (root_page_id_ == INVALID_PAGE_ID) {
    return nullptr;
  }
  Page *page = buffer_pool_manager_->FetchPage(root_page_id_);
  if (page == nullptr) {
    throw Exception("Not Found Leaf Page");
    return nullptr;
  }
  auto *node = reinterpret_cast<BPlusTreePage *>(page->GetData());
  page_id_t pageId = root_page_id_;

  while (!node->IsLeafPage()) {
    page_id_t pre_pageId = pageId;
    auto internal_node = reinterpret_cast<BPlusTreeInternalPage<KeyType, page_id_t, KeyComparator> *>(node);
    if (leftMost) {
      pageId = internal_node->ValueAt(0);
    } else {
      pageId = internal_node->Lookup(key, comparator_);
    }

    buffer_pool_manager_->UnpinPage(pre_pageId, false);
    page = buffer_pool_manager_->FetchPage(pageId);
    if (page == nullptr) {
      throw Exception("Fetch Page failure  in findleafPage");
      break;
    }
    node = reinterpret_cast<BPlusTreePage *>(page->GetData());
  }
  return page;
}

/*
 * Update/Insert root page id in header page(where page_id = 0, header_page is
 * defined under include/page/header_page.h)
 * Call this method everytime root page id is changed.
 * @parameter: insert_record      defualt value is false. When set to true,
 * insert a record <index_name, root_page_id> into header page instead of
 * updating it.
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::UpdateRootPageId(int insert_record) {
  HeaderPage *header_page = static_cast<HeaderPage *>(buffer_pool_manager_->FetchPage(HEADER_PAGE_ID));
  if (insert_record != 0) {
    // create a new record<index_name + root_page_id> in header_page
    header_page->InsertRecord(index_name_, root_page_id_);
  } else {
    // update root_page_id in header_page
    header_page->UpdateRecord(index_name_, root_page_id_);
  }
  buffer_pool_manager_->UnpinPage(HEADER_PAGE_ID, true);
}

/*
 * This method is used for test only
 * Read data from file and insert one by one
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::InsertFromFile(const std::string &file_name, Transaction *transaction) {
  int64_t key;
  std::ifstream input(file_name);
  while (input) {
    input >> key;

    KeyType index_key;
    index_key.SetFromInteger(key);
    RID rid(key);
    Insert(index_key, rid, transaction);
  }
}
/*
 * This method is used for test only
 * Read data from file and remove one by one
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::RemoveFromFile(const std::string &file_name, Transaction *transaction) {
  int64_t key;
  std::ifstream input(file_name);
  while (input) {
    input >> key;
    KeyType index_key;
    index_key.SetFromInteger(key);
    Remove(index_key, transaction);
  }
}

/**
 * This method is used for debug only, You don't  need to modify
 * @tparam KeyType
 * @tparam ValueType
 * @tparam KeyComparator
 * @param page
 * @param bpm
 * @param out
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::ToGraph(BPlusTreePage *page, BufferPoolManager *bpm, std::ofstream &out) const {
  std::string leaf_prefix("LEAF_");
  std::string internal_prefix("INT_");
  std::cout << "debugging the page" << page->GetPageId() << std::endl;

  if (page->IsLeafPage()) {
    LeafPage *leaf = reinterpret_cast<LeafPage *>(page);
    // Print node name
    out << leaf_prefix << leaf->GetPageId();
    // Print node properties
    out << "[shape=plain color=green ";
    // Print data of the node
    out << "label=<<TABLE BORDER=\"0\" CELLBORDER=\"1\" CELLSPACING=\"0\" CELLPADDING=\"4\">\n";
    // Print data
    out << "<TR><TD COLSPAN=\"" << leaf->GetSize() << "\">P=" << leaf->GetPageId() << "</TD></TR>\n";
    out << "<TR><TD COLSPAN=\"" << leaf->GetSize() << "\">"
        << "max_size=" << leaf->GetMaxSize() << ",min_size=" << leaf->GetMinSize() << "</TD></TR>\n";
    out << "<TR>";
    for (int i = 0; i < leaf->GetSize(); i++) {
      out << "<TD>" << leaf->KeyAt(i) << "</TD>\n";
    }
    out << "</TR>";
    // Print table end
    out << "</TABLE>>];\n";
    // Print Leaf node link if there is a next page
    if (leaf->GetNextPageId() != INVALID_PAGE_ID) {
      out << leaf_prefix << leaf->GetPageId() << " -> " << leaf_prefix << leaf->GetNextPageId() << ";\n";
      out << "{rank=same " << leaf_prefix << leaf->GetPageId() << " " << leaf_prefix << leaf->GetNextPageId() << "};\n";
    }
    // Print parent links if there is a parent
    if (leaf->GetParentPageId() != INVALID_PAGE_ID) {
      out << internal_prefix << leaf->GetParentPageId() << ":p" << leaf->GetPageId() << " -> " << leaf_prefix
          << leaf->GetPageId() << ";\n";
    }
  } else {
    InternalPage *inner = reinterpret_cast<InternalPage *>(page);
    // Print node name
    out << internal_prefix << inner->GetPageId();
    // Print node properties
    out << "[shape=plain color=pink ";  // why not?
    // Print data of the node
    out << "label=<<TABLE BORDER=\"0\" CELLBORDER=\"1\" CELLSPACING=\"0\" CELLPADDING=\"4\">\n";
    // Print data
    out << "<TR><TD COLSPAN=\"" << inner->GetSize() << "\">P=" << inner->GetPageId() << "</TD></TR>\n";
    out << "<TR><TD COLSPAN=\"" << inner->GetSize() << "\">"
        << "max_size=" << inner->GetMaxSize() << ",min_size=" << inner->GetMinSize() << "</TD></TR>\n";
    out << "<TR>";
    for (int i = 0; i < inner->GetSize(); i++) {
      out << "<TD PORT=\"p" << inner->ValueAt(i) << "\">";
      if (i > 0) {
        out << inner->KeyAt(i);
      } else {
        out << " ";
      }
      out << "</TD>\n";
    }
    out << "</TR>";
    // Print table end
    out << "</TABLE>>];\n";
    // Print Parent link
    if (inner->GetParentPageId() != INVALID_PAGE_ID) {
      out << internal_prefix << inner->GetParentPageId() << ":p" << inner->GetPageId() << " -> " << internal_prefix
          << inner->GetPageId() << ";\n";
    }
    // Print leaves
    for (int i = 0; i < inner->GetSize(); i++) {
      auto child_page = reinterpret_cast<BPlusTreePage *>(bpm->FetchPage(inner->ValueAt(i))->GetData());

      if (child_page == nullptr) {
        std::cout << inner->GetSize() << " / " << inner->GetParentPageId() << " / " << inner->GetMinSize() << " / "
                  << inner->GetMaxSize() << " / " <<inner->GetPageId() << " / " << i << "/" << inner->ValueAt(0)
                  << std::endl;
      }

      ToGraph(child_page, bpm, out);
      if (i > 0) {
        auto sibling_page = reinterpret_cast<BPlusTreePage *>(bpm->FetchPage(inner->ValueAt(i - 1))->GetData());
        if (!sibling_page->IsLeafPage() && !child_page->IsLeafPage()) {
          out << "{rank=same " << internal_prefix << sibling_page->GetPageId() << " " << internal_prefix
              << child_page->GetPageId() << "};\n";
        }
        bpm->UnpinPage(sibling_page->GetPageId(), false);
      }
    }
  }
  bpm->UnpinPage(page->GetPageId(), false);
}

/**
 * This function is for debug only, you don't need to modify
 * @tparam KeyType
 * @tparam ValueType
 * @tparam KeyComparator
 * @param page
 * @param bpm
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::ToString(BPlusTreePage *page, BufferPoolManager *bpm) const {
  if (page->IsLeafPage()) {
    LeafPage *leaf = reinterpret_cast<LeafPage *>(page);
    std::cout << "Leaf Page: " << leaf->GetPageId() << " parent: " << leaf->GetParentPageId()
              << " next: " << leaf->GetNextPageId() << std::endl;
    for (int i = 0; i < leaf->GetSize(); i++) {
      std::cout << leaf->KeyAt(i) << ",";
    }
    std::cout << std::endl;
    std::cout << std::endl;
  } else {
    InternalPage *internal = reinterpret_cast<InternalPage *>(page);
    std::cout << "Internal Page: " << internal->GetPageId() << " parent: " << internal->GetParentPageId() << std::endl;
    for (int i = 0; i < internal->GetSize(); i++) {
      std::cout << internal->KeyAt(i) << ": " << internal->ValueAt(i) << ",";
    }
    std::cout << std::endl;
    std::cout << std::endl;
    for (int i = 0; i < internal->GetSize(); i++) {
      ToString(reinterpret_cast<BPlusTreePage *>(bpm->FetchPage(internal->ValueAt(i))->GetData()), bpm);
    }
  }
  bpm->UnpinPage(page->GetPageId(), false);
}

template class BPlusTree<GenericKey<4>, RID, GenericComparator<4>>;
template class BPlusTree<GenericKey<8>, RID, GenericComparator<8>>;
template class BPlusTree<GenericKey<16>, RID, GenericComparator<16>>;
template class BPlusTree<GenericKey<32>, RID, GenericComparator<32>>;
template class BPlusTree<GenericKey<64>, RID, GenericComparator<64>>;

}  // namespace bustub
