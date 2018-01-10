/**
 * b_plus_tree.cpp
 */
#include <iostream>

#include "common/exception.h"
#include "common/logger.h"
#include "common/rid.h"
#include "index/b_plus_tree.h"
#include "page/header_page.h"

namespace cmudb {

INDEX_TEMPLATE_ARGUMENTS
BPLUSTREE_TYPE::BPlusTree(const std::string &name,
                          BufferPoolManager *buffer_pool_manager,
                          const KeyComparator &comparator,
                          page_id_t root_page_id)
    : index_name_(name), root_page_id_(root_page_id),
      buffer_pool_manager_(buffer_pool_manager), comparator_(comparator) {}

/*
 * Helper function to decide whether current b+tree is empty
 */
INDEX_TEMPLATE_ARGUMENTS
bool BPLUSTREE_TYPE::IsEmpty() const { return root_page_id_ == INVALID_PAGE_ID; }
/*****************************************************************************
 * SEARCH
 *****************************************************************************/
/*
 * Return the only value that associated with input key
 * This method is used for point query
 * @return : true means key exists
 */
INDEX_TEMPLATE_ARGUMENTS
bool BPLUSTREE_TYPE::GetValue(const KeyType &key,
                              std::vector<ValueType> &result,
                              Transaction *transaction) {
  auto leaf = GetLeafPage(key);
  result.resize(1);
  auto ret = leaf->Lookup(key, result[0], comparator_);
  return ret;
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
bool BPLUSTREE_TYPE::Insert(const KeyType &key, const ValueType &value,
                            Transaction *transaction) {
  if (IsEmpty()) {
    StartNewTree(key, value);
    return true;
  }
  return InsertIntoLeaf(key, value, transaction);
}
/*
 * Insert constant key & value pair into an empty tree
 * User needs to first ask for new page from buffer pool manager(NOTICE: throw
 * an "out of memory" exception if returned value is nullptr), then update b+
 * tree's root page id and insert entry directly into leaf page.
 *
 *
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::StartNewTree(const KeyType &key, const ValueType &value) {
  assert(IsEmpty());
  page_id_t page_id;
  Page *page = buffer_pool_manager_->NewPage(page_id);
  if (page == nullptr) {
    throw std::bad_alloc{};
  }
  root_page_id_ = page_id;
  B_PLUS_TREE_LEAF_PAGE_TYPE *lp = reinterpret_cast<B_PLUS_TREE_LEAF_PAGE_TYPE *>(page->GetData());
  lp->Init(page_id, INVALID_PAGE_ID);
  InsertIntoLeaf(key, value);
  UpdateRootPageId(true);
  buffer_pool_manager_->UnpinPage(page_id, true);
}

/*
 * Insert constant key & value pair into leaf page
 * User needs to first find the right leaf page as insertion target, then look
 * through leaf page to see whether insert key exist or not. If exist, return
 * immediately, otherwise insert entry. Remember to deal with split if necessary.
 * @return: since we only support unique key, if user try to insert duplicate
 * keys return false, otherwise return true.
 */
INDEX_TEMPLATE_ARGUMENTS
bool BPLUSTREE_TYPE::InsertIntoLeaf(const KeyType &key, const ValueType &value,
                                    Transaction *transaction) {
  B_PLUS_TREE_LEAF_PAGE_TYPE *lp = GetLeafPage(key);
  if (lp == nullptr) { return false; }

  auto originalSize = lp->GetSize();
  auto newSize = lp->Insert(key, value, comparator_);

  if (newSize > lp->GetMaxSize()) {

    B_PLUS_TREE_LEAF_PAGE_TYPE *oldlp = lp;
    B_PLUS_TREE_LEAF_PAGE_TYPE *newlp = Split(lp);

    InsertIntoParent(oldlp, oldlp->KeyAt(oldlp->GetSize() - 1), newlp);

    buffer_pool_manager_->UnpinPage(newlp->GetPageId(), true);
  }
  buffer_pool_manager_->UnpinPage(lp->GetPageId(), true);

  return originalSize != newSize;
}

/*
 * Split input page and return newly created page.
 * Using template N to represent either internal page or leaf page.
 * User needs to first ask for new page from buffer pool manager(NOTICE: throw
 * an "out of memory" exception if returned value is nullptr), then move half
 * of key & value pairs from input page to newly created page
 */
INDEX_TEMPLATE_ARGUMENTS
template<typename N>
N *BPLUSTREE_TYPE::Split(N *node) {
  page_id_t page_id;
  Page *newPage = buffer_pool_manager_->NewPage(page_id);
  if (newPage == nullptr) {
    throw std::bad_alloc();
  }
  typedef typename std::remove_pointer<N>::type *PagePtr;
  PagePtr ptr = reinterpret_cast<PagePtr>(newPage->GetData());
  ptr->Init(page_id, node->GetParentPageId());

  //this is different between leaf node and internal node.
  node->MoveHalfTo(ptr, buffer_pool_manager_);
  return ptr;
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
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::InsertIntoParent(BPlusTreePage *old_node,
                                      const KeyType &key,
                                      BPlusTreePage *new_node,
                                      Transaction *transaction) {
  page_id_t parentPageId = old_node->GetParentPageId();
  BPInternalPage *ip = nullptr;
  if (parentPageId == INVALID_PAGE_ID) {
    Page *newPage = buffer_pool_manager_->NewPage(parentPageId);
    if (newPage == nullptr) {
      throw std::bad_alloc();
    }
    ip = reinterpret_cast<BPInternalPage *>(newPage->GetData());
    ip->Init(parentPageId, INVALID_PAGE_ID);
    root_page_id_ = parentPageId;
    UpdateRootPageId(false);
    old_node->SetParentPageId(parentPageId);
    new_node->SetParentPageId(parentPageId);
    ip->PopulateNewRoot(old_node->GetPageId(), key, new_node->GetPageId());

    buffer_pool_manager_->UnpinPage(parentPageId, true);
    return;
  }

  ip = GetInternalPage(parentPageId);

  //insert new kv pair points to new_node after that
  ip->InsertNodeAfter(old_node->GetPageId(), key, new_node->GetPageId());

  if (ip->GetSize() > ip->GetMaxSize()) {
    BPInternalPage *oldlp = ip;
    BPInternalPage *newlp = Split(ip);
    InsertIntoParent(oldlp, newlp->KeyAt(0), newlp);

    buffer_pool_manager_->UnpinPage(newlp->GetPageId(), true);
  }
  buffer_pool_manager_->UnpinPage(parentPageId, true);
}

/*****************************************************************************
 * REMOVE
 *****************************************************************************/
/*
 * Delete key & value pair associated with input key
 * If current tree is empty, return immediately.
 * If not, User needs to first find the right leaf page as deletion target, then
 * delete entry from leaf page. Remember to deal with redistribute or merge if
 * necessary.
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::Remove(const KeyType &key, Transaction *transaction) {
  if (IsEmpty()) {
    return;
  }

  B_PLUS_TREE_LEAF_PAGE_TYPE *lp = GetLeafPage(key);
  assert(lp != nullptr);

  auto sizeAfterRemove = lp->RemoveAndDeleteRecord(key, comparator_);
  if (sizeAfterRemove < lp->GetMinSize()) {
    auto shouldRemovePage = CoalesceOrRedistribute(lp, transaction);
    if (shouldRemovePage) {
      buffer_pool_manager_->UnpinPage(lp->GetPageId(), true);
      auto deletePage = buffer_pool_manager_->DeletePage(lp->GetPageId());
      assert(deletePage);
    }
  }
  buffer_pool_manager_->UnpinPage(lp->GetPageId(), true);
}

/*
 * User needs to first find the sibling of input page. If sibling's size + input
 * page's size > page's max size, then redistribute. Otherwise, merge.
 * Using template N to represent either internal page or leaf page.
 * @return: true means target leaf page should be deleted, false means no
 * deletion happens
 */
INDEX_TEMPLATE_ARGUMENTS
template<typename N>
bool BPLUSTREE_TYPE::CoalesceOrRedistribute(N *node, Transaction *transaction) {
  if (node->GetSize() >= node->GetMinSize()) {
    return false;
  }
  //param node could be a leaf page or a internal page
  BPlusTreePage *btp = node;
  BPInternalPage *parent = GetInternalPage(btp->GetParentPageId());
  if (parent == nullptr) {
    assert(node->IsRootPage());
    return AdjustRoot(node);
  }

  auto idx = parent->ValueIndex(btp->GetPageId());

  decltype(node) leftSibling = nullptr;
  decltype(node) rightSibling = nullptr;
  //look ahead
  if (idx - 1 >= 0) {
    //check size of the page
    leftSibling = reinterpret_cast<decltype(node)>(GetPage(parent->ValueAt(idx - 1)));
    if (leftSibling->GetSize() > leftSibling->GetMinSize()) {
      //redistribute with this page
      //move the last element of left sibling to the first place of current node
      //the parent node should be updated as well

      Redistribute(leftSibling, node, 1);
      return false;
    }
  }

  if (idx + 1 < parent->GetSize()) {
    //check size of the page
    rightSibling = reinterpret_cast<decltype(node)>(GetPage(parent->ValueAt(idx + 1)));
    if (rightSibling->GetSize() > rightSibling->GetMinSize()) {
      //redistribute with this page
      Redistribute(rightSibling, node, 0);
      return false;
    }
  }

  assert(leftSibling || rightSibling);
  //return value of Coalesce is not used, as parent node is always checked to see if it should be adjusted
  //i.e. the code after the if-else
  if (leftSibling) {
    //merge with left sibling node
    Coalesce(leftSibling, node, parent, 0, transaction);
  } else {
    //merge with right sibling node
    Coalesce(rightSibling, node, parent, 1, transaction);
  }
  if (CoalesceOrRedistribute(parent, transaction)) {
    buffer_pool_manager_->UnpinPage(parent->GetPageId(), true);
  }
  return true;
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
template<typename N>
bool BPLUSTREE_TYPE::Coalesce(
    N *&neighbor_node, N *&node,
    BPlusTreeInternalPage<KeyType, page_id_t, KeyComparator> *&parent,
    int index, Transaction *transaction) {
  assert(index == 0 || index == 1);
  //index == 0 merge with left sibling, index == 1 with right sibling
  //there is difference in implementing on merge right or left
  if (index == 0) {
    node->MoveAllTo(neighbor_node, parent->ValueIndex(node->GetPageId()), buffer_pool_manager_, comparator_);
    int index = parent->ValueIndex(node->GetPageId());
    parent->Remove(index);
  } else {
    node->MoveAllTo(neighbor_node, parent->ValueIndex(neighbor_node->GetPageId()), buffer_pool_manager_, comparator_);
    //remove kv points to neighbor
    int index = parent->ValueIndex(neighbor_node->GetPageId());
    parent->Remove(index);
    //set kv originally points to node to neighbor
    index = parent->ValueIndex(node->GetPageId());
    parent->SetValueAt(index, neighbor_node->GetPageId());
  }

  auto ret = parent->GetSize() < parent->GetMinSize();
  return ret;
}

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
template<typename N>
void BPLUSTREE_TYPE::Redistribute(N *neighbor_node, N *node, int index) {
  assert(index == 0 || index == 1);//not used as comments said
  BPInternalPage *parent = GetInternalPage(node->GetParentPageId());
  if (index == 0) {
    neighbor_node->MoveFirstToEndOf(node, buffer_pool_manager_);

    int parent_index = parent->ValueIndex(neighbor_node->GetPageId());
    parent->SetKeyAt(parent_index, node->KeyAt(node->GetSize() - 1));
  } else {
    neighbor_node->MoveLastToFrontOf(node, index, buffer_pool_manager_);

    int parent_index = parent->ValueIndex(node->GetPageId());
    parent->SetKeyAt(parent_index, neighbor_node->KeyAt(neighbor_node->GetSize() - 1));
  }
  buffer_pool_manager_->UnpinPage(parent->GetPageId(), true);
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
  assert(old_root_node->IsRootPage());
  if (old_root_node->IsLeafPage() && old_root_node->GetSize() < old_root_node->GetMinSize()) {
    //case 2
    root_page_id_ = INVALID_PAGE_ID;
    UpdateRootPageId(false);
    return true;
  }

  if (!old_root_node->IsLeafPage() && old_root_node->GetSize() == 0) {
    //case 1
    BPInternalPage *ip = reinterpret_cast<BPInternalPage *>(old_root_node);
    BPlusTreePage *newRoot = GetPage(ip->ValueAt(0));
    root_page_id_ = newRoot->GetPageId();
    newRoot->SetParentPageId(INVALID_PAGE_ID);
    UpdateRootPageId(false);
    return true;
  }
  return false;
}

/*****************************************************************************
 * INDEX ITERATOR
 *****************************************************************************/
/*
 * Input parameter is void, find the leftmost leaf page first, then construct
 * index iterator
 * @return : index iterator
 */
INDEX_TEMPLATE_ARGUMENTS
INDEXITERATOR_TYPE BPLUSTREE_TYPE::Begin() {
  page_id_t page_id = root_page_id_;
  BPlusTreePage *page = GetPage(page_id);
  while (!page->IsLeafPage()) {
    BPInternalPage *ip = reinterpret_cast<BPInternalPage *>(page);
    page_id = ip->ValueAt(0);
    page = GetPage(page_id);
  }
  return INDEXITERATOR_TYPE(page->GetPageId(), 0, *buffer_pool_manager_);
}

/*
 * Input parameter is low key, find the leaf page that contains the input key
 * first, then construct index iterator
 * @return : index iterator
 */
INDEX_TEMPLATE_ARGUMENTS
INDEXITERATOR_TYPE BPLUSTREE_TYPE::Begin(const KeyType &key) {
  auto leaf = GetLeafPage(key);
  return INDEXITERATOR_TYPE(leaf->GetPageId(), leaf->KeyIndex(key, comparator_), *buffer_pool_manager_);
}

/*****************************************************************************
 * UTILITIES AND DEBUG
 *****************************************************************************/
/*
 * Find leaf page containing particular key, if leftMost flag == true, find
 * the left most leaf page
 */
INDEX_TEMPLATE_ARGUMENTS
B_PLUS_TREE_LEAF_PAGE_TYPE *BPLUSTREE_TYPE::FindLeafPage(const KeyType &key,
                                                         bool leftMost) {
  return nullptr;
}

/*
 * Update/Insert root page id in header page(where page_id = 0, header_page is
 * defined under include/page/header_page.h)
 * Call this method every time root page id is changed.
 * @parameter: insert_record default value is false. When set to true,
 * insert a record <index_name, root_page_id> into header page instead of
 * updating it.
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::UpdateRootPageId(int insert_record) {
  HeaderPage *header_page = static_cast<HeaderPage *>(
      buffer_pool_manager_->FetchPage(HEADER_PAGE_ID));
  if (insert_record)
    // create a new record<index_name + root_page_id> in header_page
    header_page->InsertRecord(index_name_, root_page_id_);
  else
    // update root_page_id in header_page
    header_page->UpdateRecord(index_name_, root_page_id_);
  buffer_pool_manager_->UnpinPage(HEADER_PAGE_ID, true);
}

/*
 * This method is used for debug only
 * print out whole b+tree structure, rank by rank
 */
INDEX_TEMPLATE_ARGUMENTS
std::string BPLUSTREE_TYPE::ToString(bool verbose) {
  if (IsEmpty()) { return "Empty tree"; }
  BPlusTreePage *r = GetPage(root_page_id_);
  if (r->IsLeafPage()) {
    B_PLUS_TREE_LEAF_PAGE_TYPE *leaf = reinterpret_cast<B_PLUS_TREE_LEAF_PAGE_TYPE *>(r);
    return leaf->ToString(verbose);
  }

  std::string result;
  std::vector<BPlusTreePage *> v{r};
  while (!v.empty()) {
    std::vector<BPlusTreePage *> next;
    for (auto item : v) {
      result += "\n";
      if (item->IsLeafPage()) {
        auto leaf = reinterpret_cast<B_PLUS_TREE_LEAF_PAGE_TYPE *>(item);
        result += leaf->ToString(verbose);
      } else {
        auto inner = reinterpret_cast<BPInternalPage *>(item);
        result += inner->ToString(verbose);
        for (int i = 0; i < inner->GetSize(); i++) {
          page_id_t page = inner->ValueAt(i);
          next.push_back(GetInternalPage(page));
        }
      }
      buffer_pool_manager_->UnpinPage(item->GetPageId(), false);
    }
    swap(v, next);
  }

  return result;
}

/*
 * This method is used for test only
 * Read data from file and insert one by one
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::InsertFromFile(const std::string &file_name,
                                    Transaction *transaction) {
  int64_t key;
  std::ifstream input(file_name);

//  int count = 0;
  while (input) {
    input >> key;

    KeyType index_key;
    index_key.SetFromInteger(key);
    RID rid(key);
    Insert(index_key, rid, transaction);
//    std::cout << "cnt: " << ++count << " " << ToString(true) << std::endl;
  }
}
/*
 * This method is used for test only
 * Read data from file and remove one by one
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::RemoveFromFile(const std::string &file_name,
                                    Transaction *transaction) {
  int64_t key;
  std::ifstream input(file_name);
  while (input) {
    input >> key;
    KeyType index_key;
    index_key.SetFromInteger(key);
    Remove(index_key, transaction);
  }
}

INDEX_TEMPLATE_ARGUMENTS
B_PLUS_TREE_LEAF_PAGE_TYPE *BPLUSTREE_TYPE::GetLeafPage(const KeyType &key) {
  if (IsEmpty()) { return nullptr; }
  page_id_t page_id = root_page_id_;
  BPlusTreePage *btp = GetPage(page_id);

  while (!btp->IsLeafPage()) {
    BPInternalPage *ip = reinterpret_cast<BPInternalPage *>(btp);
    page_id_t next = ip->Lookup(key, comparator_);
    buffer_pool_manager_->UnpinPage(page_id, false);
    page_id = next;
    btp = GetPage(page_id);
  }
  return reinterpret_cast<B_PLUS_TREE_LEAF_PAGE_TYPE *>(btp);
}

template
class BPlusTree<GenericKey<4>, RID, GenericComparator<4>>;
template
class BPlusTree<GenericKey<8>, RID, GenericComparator<8>>;
template
class BPlusTree<GenericKey<16>, RID, GenericComparator<16>>;
template
class BPlusTree<GenericKey<32>, RID, GenericComparator<32>>;
template
class BPlusTree<GenericKey<64>, RID, GenericComparator<64>>;

} // namespace cmudb
