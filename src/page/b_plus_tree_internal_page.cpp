/**
 * b_plus_tree_internal_page.cpp
 */
#include <iostream>
#include <sstream>

#include "common/exception.h"
#include "page/b_plus_tree_internal_page.h"

namespace cmudb {
/*****************************************************************************
 * HELPER METHODS AND UTILITIES
 *****************************************************************************/
/*
 * Init method after creating a new internal page
 * Including set page type, set current size, set page id, set parent id and set
 * max page size
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::Init(page_id_t page_id,
                                          page_id_t parent_id) {

  SetPageType(IndexPageType::INTERNAL_PAGE);
  SetSize(0);
  SetPageId(page_id);
  SetParentPageId(parent_id);
  assert(sizeof(BPlusTreeInternalPage) == 24);
  //this is real keys which equals to branching factor - 1.
  //not counting the fake key related to the left most link.
  //leave a slot for ease of insertion
  //well, size should be all kv pairs include the one index 0, which has no key
  //real key's count are GetSize - 1
  //that is to say, for internal node, this is branching factor
  int size = (PAGE_SIZE - sizeof(BPlusTreeInternalPage)) / sizeof(MappingType) - 1;
  size &= ~(1);
  SetMaxSize(size);
}
/*
 * Helper method to get/set the key associated with input "index"(a.k.a
 * array offset)
 */
INDEX_TEMPLATE_ARGUMENTS
KeyType B_PLUS_TREE_INTERNAL_PAGE_TYPE::KeyAt(int index) const {
  assert(index >= 0 && index < GetSize());
  // replace with your own code
  return array[index].first;
}

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::SetKeyAt(int index, const KeyType &key) {
  assert(index >= 0 && index < GetSize());
  array[index].first = key;
}

/*
 * Helper method to find and return array index(or offset), so that its value
 * equals to input "value"
 */
INDEX_TEMPLATE_ARGUMENTS
int B_PLUS_TREE_INTERNAL_PAGE_TYPE::ValueIndex(const ValueType &value) const {
  //value is not sorted, so liner transverse
  for (int i = 0; i < GetSize(); i++) {
    if (array[i].second == value) {
      return i;
    }
  }
  return -1;
}

/*
 * Helper method to get the value associated with input "index"(a.k.a array
 * offset)
 */
INDEX_TEMPLATE_ARGUMENTS
ValueType B_PLUS_TREE_INTERNAL_PAGE_TYPE::ValueAt(int index) const {
  assert(
      (
          GetSize() <= GetMaxSize() &&
              index >= 0 && index < GetSize()
      ) ||
          (
              GetSize() == GetMaxSize() + 1 &&
                  index == GetSize()
          )

  );
  return array[index].second;
}

/*****************************************************************************
 * LOOKUP
 *****************************************************************************/
/*
 * Find and return the child pointer(page_id) which points to the child page
 * that contains input "key"
 * Start the search from the second key(the first key should always be invalid)
 */
INDEX_TEMPLATE_ARGUMENTS
ValueType
B_PLUS_TREE_INTERNAL_PAGE_TYPE::Lookup(const KeyType &key,
                                       const KeyComparator &comparator) const {
  int b = 1;
  int e = GetSize();
  while (b < e) {
    int mid = b + (e - b) / 2;
    if (comparator(array[mid].first, key) == -1) {
      b = mid + 1;
    } else {
      e = mid;
    }
  }

  return array[b - 1].second;
}

/*****************************************************************************
 * INSERTION
 *****************************************************************************/
/*
 * Populate new root page with old_value + new_key & new_value
 * When the insertion cause overflow from leaf page all the way up to the root
 * page, you should create a new root page and populate its elements.
 * NOTE: This method is only called within InsertIntoParent()(b_plus_tree.cpp)
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::PopulateNewRoot(
    const ValueType &old_value, const KeyType &new_key,
    const ValueType &new_value) {
  array[0].second = old_value;
  array[1].first = new_key;
  array[1].second = new_value;
  IncreaseSize(2);
}
/*
 * Insert new_key & new_value pair right after the pair with its value ==
 * old_value
 * @return:  new size after insertion
 */
INDEX_TEMPLATE_ARGUMENTS
int B_PLUS_TREE_INTERNAL_PAGE_TYPE::InsertNodeAfter(
    const ValueType &old_value, const KeyType &new_key,
    const ValueType &new_value) {
  assert(GetSize() <= GetMaxSize());
  auto ret = ValueIndex(old_value);
  assert(ret != -1);
  for (int i = GetSize(); i > ret + 1; i--) {
    array[i].first = array[i - 1].first;
    array[i].second = array[i - 1].second;
  }
  array[ret + 1].first = new_key;
  array[ret + 1].second = new_value;
  IncreaseSize(1);
  return GetSize();
}

/*****************************************************************************
 * SPLIT
 *****************************************************************************/
/*
 * Remove half of key & value pairs from this page to "recipient" page
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::MoveHalfTo(
    BPlusTreeInternalPage *recipient,
    BufferPoolManager *buffer_pool_manager) {
  //if there are odd keys, move ceiling  size / 2 keys to recipient
  //the key on index zero in the recipient is not use and should be push upward
  //the moved half's parent id should be updated
  assert(GetSize() == GetMaxSize() + 1);
  assert(recipient != nullptr);
  int start = GetMaxSize() / 2;
  int length = GetSize();
  for (int i = start, j = 0; i < length; i++, j++) {
    recipient->array[j].first = array[i].first;
    recipient->array[j].second = array[i].second;
  }
  SetSize(start);
  recipient->IncreaseSize(length - start);

  //update recipient's parent id
  for (int i = 0; i < recipient->GetSize(); i++) {
    page_id_t page_id = recipient->ValueAt(i);
    auto page = buffer_pool_manager->FetchPage(page_id);
    assert(page);
    BPlusTreePage *bp = reinterpret_cast<BPlusTreePage *>(page);
    bp->SetParentPageId(recipient->GetPageId());
    buffer_pool_manager->UnpinPage(page_id, true);
  }
}

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::CopyHalfFrom(
    MappingType *items, int size, BufferPoolManager *buffer_pool_manager) {
  //seems not useful for the moment
}

/*****************************************************************************
 * REMOVE
 *****************************************************************************/
/*
 * Remove the key & value pair in internal page according to input index(a.k.a
 * array offset)
 * NOTE: store key&value pair continuously after deletion
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::Remove(int index) {
  for (int i = index; i + 1 < GetSize(); i++) {
    array[i].first = array[i + 1].first;
    array[i].second = array[i + 1].second;
  }
  IncreaseSize(-1);
}

/*
 * Remove the only key & value pair in internal page and return the value
 * NOTE: only call this method within AdjustRoot()(in b_plus_tree.cpp)
 */
INDEX_TEMPLATE_ARGUMENTS
ValueType B_PLUS_TREE_INTERNAL_PAGE_TYPE::RemoveAndReturnOnlyChild() {
  return INVALID_PAGE_ID;
}
/*****************************************************************************
 * MERGE
 *****************************************************************************/
/*
 * Remove all of key & value pairs from this page to "recipient" page, then
 * update relevant key & value pair in its parent page.
 *
 * altering parent node is not taken care of here but in coalesce of b tree class
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::MoveAllTo(
    BPlusTreeInternalPage *recipient, int index_in_parent,
    BufferPoolManager *buffer_pool_manager, const KeyComparator &comparator) {
  int len = GetSize() + recipient->GetSize();
  BPlusTreeInternalPage
      *parent =
      GetParentPageId() == INVALID_PAGE_ID ? nullptr :
      reinterpret_cast<BPlusTreeInternalPage *>(buffer_pool_manager->FetchPage(GetParentPageId()));
  assert(parent);
  KeyType keyType = parent->KeyAt(index_in_parent);
  if (comparator(firstKey(), recipient->firstKey()) == -1) {
    for (int i = len - 1; i >= GetSize(); i--) {
      recipient->array[i].first = recipient->array[i - GetSize()].first;
      recipient->array[i].second = recipient->array[i - GetSize()].second;
    }
    recipient->array[GetSize()].first = keyType;
    for (int i = 0; i < GetSize(); i++) {
      recipient->array[i].first = array[i].first;
      recipient->array[i].second = array[i].second;
    }
  } else {
    for (int i = 0; i < GetSize(); i++) {
      recipient->array[recipient->GetSize() + i].first = array[i].first;
      recipient->array[recipient->GetSize() + i].second = array[i].second;
    }
    recipient->array[recipient->GetSize()].first = keyType;
  }
  recipient->IncreaseSize(GetSize());
}

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::CopyAllFrom(
    MappingType *items, int size, BufferPoolManager *buffer_pool_manager) {}

/*****************************************************************************
 * REDISTRIBUTE
 *****************************************************************************/
/*
 * Remove the first key & value pair from this page to tail of "recipient"
 * page, then update relevant key & value pair in its parent page.
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::MoveFirstToEndOf(
    BPlusTreeInternalPage *recipient,
    BufferPoolManager *buffer_pool_manager) {
  assert(recipient->GetParentPageId() == GetParentPageId());
  assert(recipient->GetParentPageId() != INVALID_PAGE_ID);
  //copy record
  recipient->array[recipient->GetSize()].first = array[0].first;
  recipient->array[recipient->GetSize()].second = array[0].second;
  recipient->IncreaseSize(1);
  //erase from current node
  for (int i = 0; i < GetSize() - 1; i++) {
    array[i].first = array[i + 1].first;
    array[i].second = array[i + 1].second;
  }
  IncreaseSize(-1);

  //adjust their parent
  Page *page = buffer_pool_manager->FetchPage(GetParentPageId());
  assert(page);

  BPlusTreeInternalPage *parent = reinterpret_cast<BPlusTreeInternalPage *>(page);
  int index = parent->ValueIndex(GetPageId());
  assert(index != -1);
  recipient->SetKeyAt(recipient->GetSize() - 1, parent->KeyAt(index));
  parent->SetKeyAt(index, KeyAt(0));

  buffer_pool_manager->UnpinPage(GetParentPageId(), true);
}

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::CopyLastFrom(
    const MappingType &pair, BufferPoolManager *buffer_pool_manager) {}

/*
 * Remove the last key & value pair from this page to head of "recipient"
 * page, then update relevant key & value pair in its parent page.
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::MoveLastToFrontOf(
    BPlusTreeInternalPage *recipient, int parent_index,
    BufferPoolManager *buffer_pool_manager) {
  assert(recipient->GetParentPageId() == GetParentPageId());
  assert(recipient->GetParentPageId() != INVALID_PAGE_ID);
  for (int i = recipient->GetSize(); i >= 1; i--) {
    recipient->array[i].first = recipient->array[i - 1].first;
    recipient->array[i].second = recipient->array[i - 1].second;
  }

  recipient->array[0].first = array[GetSize() - 1].first;
  recipient->array[0].second = array[GetSize() - 1].second;

  recipient->IncreaseSize(1);
  IncreaseSize(-1);

  //adjust parent
  Page *page = buffer_pool_manager->FetchPage(GetParentPageId());
  assert(page);

  BPlusTreeInternalPage *parent = reinterpret_cast<BPlusTreeInternalPage *>(page);
  int index = parent->ValueIndex(recipient->GetPageId());
  assert(index != -1);
  assert(index == parent_index);
  recipient->SetKeyAt(1, parent->KeyAt(index));
  parent->SetKeyAt(index, recipient->KeyAt(0));

  buffer_pool_manager->UnpinPage(GetParentPageId(), true);
}

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::CopyFirstFrom(
    const MappingType &pair, int parent_index,
    BufferPoolManager *buffer_pool_manager) {}

/*****************************************************************************
 * DEBUG
 *****************************************************************************/
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::QueueUpChildren(
    std::queue<BPlusTreePage *> *queue,
    BufferPoolManager *buffer_pool_manager) {
  for (int i = 0; i < GetSize(); i++) {
    auto *page = buffer_pool_manager->FetchPage(array[i].second);
    if (page == nullptr)
      throw Exception(EXCEPTION_TYPE_INDEX,
                      "all page are pinned while printing");
    BPlusTreePage *node =
        reinterpret_cast<BPlusTreePage *>(page->GetData());
    queue->push(node);
  }
}

INDEX_TEMPLATE_ARGUMENTS
std::string B_PLUS_TREE_INTERNAL_PAGE_TYPE::ToString(bool verbose) const {
  if (GetSize() == 0) {
    return "";
  }
  std::ostringstream os;
  if (verbose) {
    os << "[pageId: " << GetPageId() << " parentId: " << GetParentPageId()
       << "]<keys:" << GetSize() << "> :\n";
  }

  int entry = verbose ? 0 : 1;
  int end = GetSize();
  bool first = true;
  while (entry < end) {
    if (first) {
      first = false;
    } else {
      os << " ";
    }
    os << std::dec << array[entry].first.ToString();
    if (verbose) {
      os << "(" << array[entry].second << ")";
    }
    ++entry;
  }
  return os.str();
}
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::SetKVAt(const KeyType &key, const ValueType &value, int index) {
  assert(this->GetSize() > index);
  array[index].first = key;
  array[index].second = value;
}
// valuetype for internalNode should be page id_t
template
class BPlusTreeInternalPage<GenericKey<4>, page_id_t,
                            GenericComparator<4>>;
template
class BPlusTreeInternalPage<GenericKey<8>, page_id_t,
                            GenericComparator<8>>;
template
class BPlusTreeInternalPage<GenericKey<16>, page_id_t,
                            GenericComparator<16>>;
template
class BPlusTreeInternalPage<GenericKey<32>, page_id_t,
                            GenericComparator<32>>;
template
class BPlusTreeInternalPage<GenericKey<64>, page_id_t,
                            GenericComparator<64>>;
} // namespace cmudb
