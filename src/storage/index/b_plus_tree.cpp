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
#include "storage/index/b_plus_tree.h"
#include <string>
#include "common/exception.h"
#include "common/rid.h"
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

/*****************************************************************************
 * SEARCH
 <*****************************************************************************/
/*
 * Return the only value that associated with input key
 * This method is used for point query
 * @return : true means key exists
 */
INDEX_TEMPLATE_ARGUMENTS
bool BPLUSTREE_TYPE::GetValue(const KeyType &key, std::vector<ValueType> *result, Transaction *transaction) {
  Page *leaf_page = FindLeafPage(key, false);
  bool res = false;
  // if (leaf_page == nullptr) {
  //  UnlockRoot(false, 0);
  //  throw Exception("GetValue functions found not leaf page");
  // }
  auto leaf_node = reinterpret_cast<B_PLUS_TREE_LEAF_PAGE_TYPE *>(leaf_page->GetData());
  // if (leaf_node == nullptr) {
  //  throw Exception("problem found in GetValue function!");
  // }
  if (leaf_node != nullptr) {
    ValueType value;
    if (leaf_node->Lookup(key, &value, comparator_)) {
      // if (transaction != nullptr) {
      // std::cout << "thread id is " << transaction->GetThreadId() << "value is " << value << std::endl;
      // }
      res = true;
      // result->clear();
      // std::cout << "before push back the value to LookupHelper, value is " << value << "result's size is "
      //          << result->size() << std::endl;
      result->push_back(value);
      // std::cout << "After push back the value to LookupHelper, value is " << value << "result's size is  "
      //          << result->size() << std::endl;
    }
    auto page_id = leaf_page->GetPageId();
    UnlockPage(leaf_page, false);
    buffer_pool_manager_->UnpinPage(page_id, false);
    if (page_id == root_page_id_) {
      UnlockRoot(false, 0);
    }
  }
  // if (!res) {
  //   std::cout << "because of res false,"
  //            << "thread id is" << transaction->GetThreadId()
  //            << "GetValue function return a zero size result whose key is " << key << "the page id is "
  //            << leaf_node->GetPageId() << "his page's size is" << leaf_node->GetSize() << std::endl;
  // }
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
  // std::cout << "transaction thread id " << transaction->GetThreadId() << "is inserting!!!" << std::endl;
  LockRoot(true, 1);
  if (IsEmpty()) {
    StartNewTree(key, value);
    // UnlockRoot(true);
    return true;
  }

  bool res = InsertIntoLeaf(key, value, transaction);
  // 这里没必要UnlockRoot,留待FreePageInTransaction去解决。
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
  // page_id_t page_id = INVALID_PAGE_ID;
  // buffer_pool_manager_->FetchPage(page_id);
  Page *rootLeafPage = buffer_pool_manager_->NewPage(&root_page_id_);
  LockPage(rootLeafPage, true, 1);
  // if (rootLeafPage == nullptr) {
  //  throw Exception(ExceptionType::OUT_OF_MEMORY, "Can not StartNewTree");
  // }
  auto *root_node = reinterpret_cast<B_PLUS_TREE_LEAF_PAGE_TYPE *>(rootLeafPage->GetData());
  root_node->Init(rootLeafPage->GetPageId(), INVALID_PAGE_ID, leaf_max_size_);
  root_node->Insert(key, value, comparator_);

  // bool insert = IsEmpty();

  // int updateRecord = insert ? 1 : 0;
  UpdateRootPageId(1);

  UnlockPage(rootLeafPage, true, 1);
  // std::cout << "Unlockroot in startnewtree !!!" << std::endl;
  UnlockRoot(true, 1);
  buffer_pool_manager_->UnpinPage(root_page_id_, true);
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
  // std::cout << "transaction thread id " << transaction->GetThreadId() << " is InsertIntoLeaf" << std::endl;
  Page *page = Search(key, 1, transaction);
  if (page == nullptr) {
    UnlockRoot(true, 1);
    //  throw Exception("Cannot insert into the leaf, found not page");
    return false;
  }
  auto *leafnode = reinterpret_cast<BPlusTreeLeafPage<KeyType, RID, KeyComparator> *>(page->GetData());
  ValueType rid;

  bool res = leafnode->Lookup(key, &rid, comparator_);
  if (res) {
    // std::cout << "Not found the value " << key << std::endl;
    FreeAllPageInTransaction(transaction, 1);
    return false;
  }
  // BUSTUB_ASSERT(leafnode->GetSize() < leafnode->GetMaxSize(), "leaf size should never be its max size");
  // std::cout << "before insert into the leafnode's page id is " << page->GetPageId() << std::endl;
  leafnode->Insert(key, value, comparator_);
  if (leafnode->GetSize() >= leafnode->GetMaxSize()) {
    auto new_node = Split(leafnode, transaction);
    // setnextpageId在b_plus_tree_leaf_page里设置
    InsertIntoParent(leafnode, new_node->KeyAt(0), new_node, transaction);
  }

  FreeAllPageInTransaction(transaction, 1);
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
N *BPLUSTREE_TYPE::Split(N *node, Transaction *transaction) {
  // std::cout << "transaction thread id " << transaction->GetThreadId() << "is splitting the node " << std::endl;
  page_id_t pageId;
  Page *page = buffer_pool_manager_->NewPage(&pageId);

  // if (page == nullptr) {
  //   throw Exception(ExceptionType::OUT_OF_MEMORY, "out of memory in split b_plus_tree");
  // }

  N *new_node = reinterpret_cast<N *>(page->GetData());
  new_node->Init(pageId, INVALID_PAGE_ID, node->GetMaxSize());
  node->MoveHalfTo(new_node, buffer_pool_manager_);
  LockPage(page, true, 1);
  // std::cout << "AddIntoPageSet1 : " << page->GetPageId();
  if (transaction != nullptr) {
    transaction->AddIntoPageSet(page);
  }

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
  // std::cout << "transaction thread id " << transaction->GetThreadId() << " is InsertIntoParent " << std::endl;
  page_id_t parent_page_id = old_node->GetParentPageId();

  // 意味着old_node为根结点
  if (parent_page_id == INVALID_PAGE_ID) {
    // LockRoot(true); 如果我们不安全那么我们不会unlock
    // std::cout << "new root page" << std::endl;
    page_id_t new_root_page_id;
    Page *newRootPage = buffer_pool_manager_->NewPage(&new_root_page_id);

    // if (newRootPage == nullptr) {
    //  throw Exception(ExceptionType::OUT_OF_MEMORY,
    //                  "Can not InsertIntoParent, because we can't give"
    //                  "enough memory for a new root node");
    // }
    auto *new_root_node =
        reinterpret_cast<BPlusTreeInternalPage<KeyType, page_id_t, KeyComparator> *>(newRootPage->GetData());
    new_root_node->Init(new_root_page_id, INVALID_PAGE_ID, internal_max_size_);
    new_root_node->PopulateNewRoot(old_node->GetPageId(), key, new_node->GetPageId());
    root_page_id_ = new_root_page_id;
    LockPage(newRootPage, true, 1);
    if (transaction != nullptr) {
      transaction->AddIntoPageSet(newRootPage);
      // std::cout << "add into page set the newRootPage, his page id is " << newRootPage->GetPageId() << std::endl;
    }

    old_node->SetParentPageId(new_root_page_id);
    new_node->SetParentPageId(new_root_page_id);
    UpdateRootPageId(0);
  } else {  // 非根结点
    Page *parent_page = buffer_pool_manager_->FetchPage(
        parent_page_id);  // 因为之前Search的时候已经锁了，所以这里不用再锁，
                          // 至于正确性，你想想，你都没释放，其他transaction无法干扰到你
                          // 对于每个transaction本身他的操作是sequential的，所以不会有冲突
    // if (parent_page == nullptr) {
    //   throw Exception(ExceptionType::OUT_OF_MEMORY, "out of memory in InsertIntoParent");
    // }
    auto *parent_node =
        reinterpret_cast<BPlusTreeInternalPage<KeyType, page_id_t, KeyComparator> *>(parent_page->GetData());
    // BUSTUB_ASSERT(parent_node->GetSize() <= parent_node->GetMaxSize(),
    //               "internal node size should never be its max size");

    if (parent_node->GetSize() == parent_node->GetMaxSize()) {
      auto new_parent_node = Split(parent_node, transaction);
      auto old_node_index = parent_node->ValueIndex(old_node->GetPageId());
      if (old_node_index == parent_node->GetSize()) {
        new_parent_node->InsertNodeAfter(old_node->GetPageId(), key, new_node->GetPageId());
        new_node->SetParentPageId(new_parent_node->GetPageId());
      } else {
        parent_node->InsertNodeAfter(old_node->GetPageId(), key, new_node->GetPageId());
        new_node->SetParentPageId(parent_page_id);
      }
      InsertIntoParent(parent_node, new_parent_node->KeyAt(0), new_parent_node, transaction);
    } else {
      // safely insert into parent_node not need split
      parent_node->InsertNodeAfter(old_node->GetPageId(), key, new_node->GetPageId());
      new_node->SetParentPageId(parent_page_id);
    }
    buffer_pool_manager_->UnpinPage(parent_node->GetPageId(), true);
  }
}

/*****************************************************************************
 * REMOVE
 *****************************************************************************/
INDEX_TEMPLATE_ARGUMENTS
template <typename N>
bool BPLUSTREE_TYPE::coalesceOrNot(N *node, N *sibling) {
  bool res = false;
  if (node->IsLeafPage()) {
    res = node->GetSize() + sibling->GetSize() < node->GetMaxSize();
  } else {
    res = node->GetSize() + sibling->GetSize() <= node->GetMaxSize();
  }
  return res;
}

/*
 * Delete key & value pair associated with input key
 * If current tree is empty, return immdiately.
 * If not, User needs to first find the right leaf page as deletion target, then
 * delete entry from leaf page. Remember to deal with redistribute or merge if
 * necessary.
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::Remove(const KeyType &key, Transaction *transaction) {
  // std::cout << "transaction thread id " << transaction->GetThreadId() << "is Remove!!!" << std::endl;
  LockRoot(true, 2);
  if (IsEmpty()) {
    UnlockRoot(true, 2);
    return;
  }
  auto *page = Search(key, 2, transaction);
  // if (page == nullptr) {
  //  throw Exception("problem in Remove");
  // return;
  // }
  auto *node = reinterpret_cast<B_PLUS_TREE_LEAF_PAGE_TYPE *>(page->GetData());

  ValueType value;
  if (!node->Lookup(key, &value, comparator_)) {  // concurrent index的还是要识别是否存在处理下
    FreeAllPageInTransaction(transaction, 1);
    return;
  }

  // if (!node->IsRootPage()) {
  //  BUSTUB_ASSERT(node->GetSize() >= node->GetMinSize(), "leaf page's size should >= leaf page's minsize");
  // }

  node->RemoveAndDeleteRecord(key, comparator_);

  if (node->GetSize() < node->GetMinSize()) {
    bool deleted = CoalesceOrRedistribute(&node, transaction);
    if (deleted) {
      // std::cout << "AddIntoPageSet3 : " << page->GetPageId();
      transaction->AddIntoDeletedPageSet(node->GetPageId());
    }
  }
  FreeAllPageInTransaction(transaction, 2);
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
bool BPLUSTREE_TYPE::CoalesceOrRedistribute(N **node, Transaction *transaction) {
  // std::cout << "transaction thread id " << transaction->GetThreadId() << " is CoalesceOrRedistribute!!!" <<
  // std::endl;
  if ((*node)->IsRootPage()) {
    bool res =
        AdjustRoot(*node);  // 这里AdjustRoot没有必要LockRoot, 或LockPage因为如果需要他在前面Search时候就已经锁住了
    return res;
  }
  auto parent_page = buffer_pool_manager_->FetchPage((*node)->GetParentPageId());

  auto parent_node =
      reinterpret_cast<BPlusTreeInternalPage<KeyType, page_id_t, KeyComparator> *>(parent_page->GetData());
  int sibling_index = findSibling(*node, parent_node);
  // if (sibling_index == -1) {
  //  throw Exception("CoalesceOrRedistributed now should not happen this situation");
  // 证明它没有兄弟可以借用或者合成
  // ,除了一些极端情况可能会导致它得被删除掉但他不是根结点，这种情况我们需要另外考虑，看之后测试是否有问题。
  // return false;
  // }
  auto sibling_page_id = parent_node->ValueAt(sibling_index);
  auto sibling_page = buffer_pool_manager_->FetchPage(sibling_page_id);
  LockPage(sibling_page, true, 2);
  // std::cout << "AddIntoPageSet4 : " << sibling_page->GetPageId();
  transaction->AddIntoPageSet(sibling_page);
  auto sibling_node = reinterpret_cast<N *>(sibling_page->GetData());

  int node_index = parent_node->ValueIndex((*node)->GetPageId());

  bool coalesced = false;
  if (coalesceOrNot(*node, sibling_node)) {
    // 可以合成
    coalesced = true;

    bool delete_parent = false;
    if (node_index < sibling_index) {
      delete_parent = Coalesce(*node, sibling_node, parent_node, sibling_index, transaction);
      // auto temp = node;
      // *node = sibling_node;
      // sibling_node = temp;
      std::swap(*node, sibling_node);
      // std::swap(node->GetPageId(), sibling_node->GetPageId()); // 为了让上一层可以通过函数返回值判断删掉节点
    }
    if (node_index > sibling_index) {
      delete_parent = Coalesce(sibling_node, *node, parent_node, node_index, transaction);
    }

    // 根据Coalesce返回结果
    if (delete_parent) {
      transaction->AddIntoDeletedPageSet(parent_node->GetPageId());
    }
  } else {
    // Redistribute
    if (node_index < sibling_index) {
      Redistribute(sibling_node, *node, 0);
    }
    if (node_index > sibling_index) {
      Redistribute(sibling_node, *node, 1);
    }
  }
  return coalesced;  // which means node can be deleted in the caller
}

// return the parent index of the sibling, if not exists return -1
INDEX_TEMPLATE_ARGUMENTS
template <typename N>
int BPLUSTREE_TYPE::findSibling(N *node, BPlusTreeInternalPage<KeyType, page_id_t, KeyComparator> *parent) {
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
  // std::cout << "transaction thread id " << transaction->GetThreadId() << "is Coalesce !!!" << std::endl;
  if (node->IsLeafPage()) {  // 叶子节点
    // BUSTUB_ASSERT(neighbor_node->GetSize() + node->GetSize() <= node->GetMaxSize(), "叶子节点小于maxsize");
    auto leaf_node = reinterpret_cast<BPlusTreeLeafPage<KeyType, RID, KeyComparator> *>(node);
    auto leaf_neighbor_node = reinterpret_cast<BPlusTreeLeafPage<KeyType, RID, KeyComparator> *>(neighbor_node);
    leaf_node->MoveAllTo(leaf_neighbor_node);  // <neighbor_node, node>
  } else {                                     // 非叶子节点
    // BUSTUB_ASSERT(neighbor_node->GetSize() + node->GetSize() <= node->GetMaxSize(), "非叶子节点小于等于maxsize");
    auto internal_node = reinterpret_cast<BPlusTreeInternalPage<KeyType, page_id_t, KeyComparator> *>(node);
    auto internal_neighbor_node =
        reinterpret_cast<BPlusTreeInternalPage<KeyType, page_id_t, KeyComparator> *>(neighbor_node);
    KeyType key = parent->KeyAt(index);
    internal_node->MoveAllTo(internal_neighbor_node, key, buffer_pool_manager_);  //
  }

  parent->Remove(index);
  if (parent->GetSize() <= parent->GetMinSize()) {
    bool res = CoalesceOrRedistribute(&parent, transaction);
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
    throw Exception(ExceptionType::OUT_OF_MEMORY, "fail to fetch page in redistribute function");
  }

  auto *parent_node =
      reinterpret_cast<BPlusTreeInternalPage<KeyType, page_id_t, KeyComparator> *>(parent_page->GetData());
  int node_index = parent_node->ValueIndex(node->GetPageId());
  int neighbor_index = parent_node->ValueIndex(neighbor_node->GetPageId());

  if (node->IsLeafPage()) {  // 叶子结点的Redistribute
    auto *leaf_node = reinterpret_cast<BPlusTreeLeafPage<KeyType, RID, KeyComparator> *>(node);
    auto *leaf_neighbor_node = reinterpret_cast<BPlusTreeLeafPage<KeyType, RID, KeyComparator> *>(neighbor_node);

    if (index == 0) {
      // BUSTUB_ASSERT(node_index + 1 == neighbor_index, "leaf redistribute1 failure");
      leaf_neighbor_node->MoveFirstToEndOf(leaf_node);
      parent_node->SetKeyAt(neighbor_index, leaf_neighbor_node->GetItem(0).first);
    } else {
      // BUSTUB_ASSERT(node_index == neighbor_index + 1, "leaf redistribute2 failure");
      leaf_neighbor_node->MoveLastToFrontOf(leaf_node);
      parent_node->SetKeyAt(node_index, leaf_node->GetItem(0).first);
    }
  } else {  // 中间结点的redistribute
    auto *internal_node = reinterpret_cast<BPlusTreeInternalPage<KeyType, page_id_t, KeyComparator> *>(node);
    auto *internal_neighbor_node =
        reinterpret_cast<BPlusTreeInternalPage<KeyType, page_id_t, KeyComparator> *>(neighbor_node);

    KeyType middle_key{};
    if (index == 0) {
      // BUSTUB_ASSERT(node_index + 1 == neighbor_index, "internal redistribute1 failure");
      middle_key = parent_node->KeyAt(neighbor_index);
      internal_neighbor_node->MoveFirstToEndOf(internal_node, middle_key, buffer_pool_manager_);
      parent_node->SetKeyAt(neighbor_index, internal_neighbor_node->KeyAt(0));
    } else {
      // BUSTUB_ASSERT(node_index == neighbor_index + 1, "internal redistribute2 failure");

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
  if (old_root_node->IsLeafPage()) {  // check case 2
    if (old_root_node->GetSize() == 0) {
      old_root_node->SetSize(0);
      root_page_id_ = INVALID_PAGE_ID;
      UpdateRootPageId(0);
      UnlockRoot(true, 2);
      return true;
    }
  } else {  // check case 1
    if (old_root_node->GetSize() == 1) {
      // std::cout << "AdjustRoot Problem" << std::endl;
      auto internal_root_node =
          reinterpret_cast<BPlusTreeInternalPage<KeyType, page_id_t, KeyComparator> *>(old_root_node);
      root_page_id_ = internal_root_node->RemoveAndReturnOnlyChild();
      auto *new_root_page = buffer_pool_manager_->FetchPage(root_page_id_);
      // std::cout << "AdjustRoot Problem" << std::endl;
      auto *new_root = reinterpret_cast<BPlusTreeLeafPage<KeyType, RID, KeyComparator> *>(new_root_page->GetData());
      new_root->SetParentPageId(INVALID_PAGE_ID);
      UpdateRootPageId(0);
      // UnlockPage(new_root_page, true, 2);

      buffer_pool_manager_->UnpinPage(root_page_id_, true);
      // UnlockRoot(true, 2); // just a try

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
  LockRoot(false);
  if (root_page_id_ == INVALID_PAGE_ID) {
    UnlockRoot(false);
    return INDEXITERATOR_TYPE(true);
  }
  UnlockRoot(false);
  KeyType key{};
  Page *leftmost_leafPage = FindLeafPage(key, true);
  auto *leafnode = reinterpret_cast<B_PLUS_TREE_LEAF_PAGE_TYPE *>(leftmost_leafPage->GetData());
  if (leafnode->IsRootPage()) {
    UnlockRoot(false);
  }
  INDEXITERATOR_TYPE iterator = INDEXITERATOR_TYPE(leafnode->GetPageId(), 0, leafnode->GetSize(), buffer_pool_manager_);
  // UnlockPage(leftmost_leafPage, false);
  // buffer_pool_manager_->UnpinPage(leftmost_leafPage->GetPageId(), false);
  return iterator;
}

/*
 * Input parameter is low key, find the leaf page that contains the input key
 * first, then construct index iterator
 * @return : index iterator
 */
INDEX_TEMPLATE_ARGUMENTS
INDEXITERATOR_TYPE BPLUSTREE_TYPE::Begin(const KeyType &key) {
  LockRoot(false);
  if (root_page_id_ == INVALID_PAGE_ID) {
    UnlockRoot(false);
    return INDEXITERATOR_TYPE(true);
  }
  UnlockRoot(false);
  Page *leafPage = FindLeafPage(key, false);
  // iterator特殊，哪怕遍历过程中这一页的rootpageid改了也没关系，因为这一页已经锁了
  auto *leafnode = reinterpret_cast<B_PLUS_TREE_LEAF_PAGE_TYPE *>(leafPage->GetData());
  if (leafnode->IsRootPage()) {
    UnlockRoot(false);
  }
  int index = leafnode->KeyIndex(key, comparator_);
  auto iter = INDEXITERATOR_TYPE(leafnode->GetPageId(), index, leafnode->GetSize(), buffer_pool_manager_);
  // UnlockPage(leafPage, false);
  // buffer_pool_manager_->UnpinPage(leafPage->GetPageId(), false);
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
 * Lock root pageId, either exclusive or not
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::LockRoot(bool exclusive, int op) {
  if (exclusive) {
    mutex.WLock();
    // std::cout << "1 <  op is " << op << std::endl;
  } else {
    mutex.RLock();
    // std::cout << "2 < " << std::endl;
  }
}

/*
 * Lock root pageId, either exclusive or not
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::UnlockRoot(bool exclusive, int op) {
  if (exclusive) {
    // std::cout << "> 1 op is " << op << std::endl;
    mutex.WUnlock();
  } else {
    // std::cout << "> 2 " << std::endl;
    mutex.RUnlock();
  }
}

/*
 * Lock Page, Fetch page first than Lock it
 * enable == true means Wlatch
 * enable == false means Rlatch
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::LockPage(Page *page, bool enable, int op) {
  if (enable) {
    page->WLatch();
    // auto node = reinterpret_cast<BPlusTreePage *>(page->GetData());
    // std::cout << "WLock the page id " << page->GetPageId() << ", his page size is " << node->GetSize()
    //          << ", his parent is " << node->GetParentPageId() << ", his op is " << op << ", his type is "
    //          << static_cast<int>(node->IsLeafPage()) << ", its max size is" << node->GetMaxSize() <<
    //          std::endl;
  } else {
    page->RLatch();
    // auto node = reinterpret_cast<BPlusTreePage *>(page->GetData());

    // std::cout << "RLock the page id " << page->GetPageId() << ", his page size is " << node->GetSize()
    //           << ", his parent is" << node->GetParentPageId() << ", its max size is" << node->GetMaxSize() <<
    //           std::endl;
  }
}

/*
 * Unlock Page, Unlock page first than unpin it
 * enable == true means WUnlatch
 * enable == false means RUnlatch
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::UnlockPage(bustub::Page *page, bool enable, int op) {
  if (enable) {
    // auto node = reinterpret_cast<BPlusTreePage *>(page->GetData());
    // std::cout << "WUnlock the page id " << page->GetPageId() << ", his page size is " << node->GetSize()
    //          << ", his parent is " << node->GetParentPageId() << ", his op is " << op << ", his type is "
    //          << static_cast<int>(node->IsLeafPage()) << ", its max size is" << node->GetMaxSize() <<
    //          std::endl;

    page->WUnlatch();
    // std::cout << "Unlock successfully!" << std::endl;
  } else {
    // auto node = reinterpret_cast<BPlusTreePage *>(page->GetData());
    // std::cout << "RUnlock the page id " << page->GetPageId() << ", his page size is " << node->GetSize()
    //           << ", his parent is" << node->GetParentPageId() << ", its max size is" << node->GetMaxSize() <<
    //           std::endl;
    page->RUnlatch();
  }
}

/*
 * Safe function,
 * op == 1 means insert, op == 2 means delete
 * return true, when 1.1 during insert, the internal page's size + 1 <= internal's maxsize
 *                   1.2 during insert, the leaf page's size + 1 < leaf's maxsize
 *                   2.1 during delete, the internal page's size - 1 > internal's minsize
 *                   2.2 during delete, the leaf page's size - 1 >= leaf's min size
 *  It means: minsize <= leaf's size     < maxsize
 *            minsize <  internal's size <= maxsize
 */
INDEX_TEMPLATE_ARGUMENTS
template <typename N>
bool BPLUSTREE_TYPE::Safe(N *node, int op) {
  bool res = false;
  if (node->IsLeafPage()) {
    res =
        (op == 1 && node->GetSize() + 1 < node->GetMaxSize()) || (op == 2 && node->GetSize() - 1 >= node->GetMinSize());
  } else {
    res =
        (op == 1 && node->GetSize() + 1 <= node->GetMaxSize()) || (op == 2 && node->GetSize() - 1 > node->GetMinSize());
  }
  return res;
}

/*
 *
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::FreeAllPageInTransaction(bustub::Transaction *transaction, int op) {
  // std::cout << "transaction thread id " << transaction->GetThreadId() << " is FreeAllPageInTransaction!!!" <<
  // std::endl;
  auto page_set = transaction->GetPageSet();
  page_id_t pageId;
  // bool res = false;
  bool check = true;
  if (op == 1) {
    while (!page_set->empty()) {
      auto pn = page_set->front();
      page_set->pop_front();
      pageId = pn->GetPageId();
      UnlockPage(pn, true, op);
      buffer_pool_manager_->UnpinPage(pageId, true);
      if (check && (pageId == root_page_id_)) {  // 这里为什么可以不加锁就用root_page_id,是有一个正确不变量保证的
        // 我们是先LockRoot，再LockRootPage,因此我们得先UnlockRootPage,再UnlockRoot
        // 为了保证正确性，首先我们这里不会对root_page_id进行修改操作，第二这棵b+树如果
        // root_page_id的值在这个过程中改了，因为我们的父亲还在被锁着，所以轮不到子节点等于
        // root_page_id，间接保证了正确性。
        UnlockRoot(true, op);
        check = false;
      }
    }
  } else if (op == 2) {
    auto deleted_page_set = transaction->GetDeletedPageSet();

    while (!page_set->empty()) {
      auto *pn = page_set->back();
      page_set->pop_back();
      pageId = pn->GetPageId();
      UnlockPage(pn, true, op);
      buffer_pool_manager_->UnpinPage(pageId, true);
      if (check && pageId == root_page_id_) {
        UnlockRoot(true, op);
        check = false;
      }

      auto it = deleted_page_set->find(pageId);
      if (it != deleted_page_set->end()) {
        // std::cout << "Delete the page " << pageId << std::endl;
        deleted_page_set->erase(it);
        buffer_pool_manager_->DeletePage(pageId);
      }
    }
    // deleted_page_set->clear();
  }
  // if (res) {
  // std::cout << "Unlock root in free all page in transaction " << std::endl;
  //   UnlockRoot(true, op);
  // }
}
/*
 * Search
 */
INDEX_TEMPLATE_ARGUMENTS
Page *BPLUSTREE_TYPE::Search(const KeyType &key, int op, Transaction *transaction) {
  // std::cout << "transaction thread id " << transaction->GetThreadId() << "is Search !!!" << std::endl;
  // LockRoot(true); 这里不需要Insert和remove的入口已经对它LockRoot了
  if (root_page_id_ == INVALID_PAGE_ID) {
    UnlockRoot(true, op);
    return nullptr;
  }
  Page *page = buffer_pool_manager_->FetchPage(root_page_id_);
  // if (page == nullptr) {
  //  UnlockRoot(true, op);
  //  throw Exception("Not Found leaf page in search function");
  //  return nullptr;
  // }
  LockPage(page, true, op);
  // std::cout << "AddIntoPageSet4 : " << page->GetPageId();
  transaction->AddIntoPageSet(page);
  auto *node = reinterpret_cast<BPlusTreePage *>(page->GetData());
  page_id_t pageId = root_page_id_;

  while (!node->IsLeafPage()) {
    auto *internal_node = reinterpret_cast<BPlusTreeInternalPage<KeyType, page_id_t, KeyComparator> *>(node);
    pageId = internal_node->Lookup(key, comparator_);

    page = buffer_pool_manager_->FetchPage(pageId);
    LockPage(page, true, op);
    node = reinterpret_cast<BPlusTreePage *>(page->GetData());
    // 如果该节点安全，把它祖先的锁都释放了
    if (Safe(node, op)) {
      FreeAllPageInTransaction(transaction, op);  // not use op to avoid search delete page set cost
    }
    transaction->AddIntoPageSet(page);
  }
  return page;
}

/*
 * Find leaf page containing particular key, if leftMost flag == true, find
 * the left most leaf page
 */
INDEX_TEMPLATE_ARGUMENTS
Page *BPLUSTREE_TYPE::FindLeafPage(const KeyType &key, bool leftMost) {
  LockRoot(false);
  if (root_page_id_ == INVALID_PAGE_ID) {
    UnlockRoot(false);
    return nullptr;
  }

  Page *page = buffer_pool_manager_->FetchPage(root_page_id_);
  // if (page == nullptr) {
  //  UnlockRoot(false);
  //   throw Exception("Not Found Leaf Page");
  // return nullptr;
  // }
  LockPage(page, false);  // 先fetch,再获取锁
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
    auto child_page = buffer_pool_manager_->FetchPage(pageId);
    LockPage(child_page, false);
    // if (child_page == nullptr) {
    //   throw Exception("Fetch Page failure  in findleafPage");
    // break;
    // }
    node = reinterpret_cast<BPlusTreePage *>(child_page->GetData());
    UnlockPage(page, false);  // 先解锁,再Unpin
    page = child_page;
    buffer_pool_manager_->UnpinPage(pre_pageId, false);
    if (pre_pageId == root_page_id_) {
      UnlockRoot(false);
    }
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
  LockPage(header_page, true, 3);
  if (insert_record != 0) {
    // create a new record<index_name + root_page_id> in header_page
    header_page->InsertRecord(index_name_, root_page_id_);
  } else {
    // update root_page_id in header_page
    header_page->UpdateRecord(index_name_, root_page_id_);
  }
  UnlockPage(header_page, true, 3);
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
                  << inner->GetMaxSize() << " / " << inner->GetPageId() << " / " << i << "/" << inner->ValueAt(0)
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
