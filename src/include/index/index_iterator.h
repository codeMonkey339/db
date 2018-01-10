/**
 * index_iterator.h
 * For range scan of b+ tree
 */
#pragma once
#include "page/b_plus_tree_leaf_page.h"

namespace cmudb {

#define INDEXITERATOR_TYPE                                                     \
  IndexIterator<KeyType, ValueType, KeyComparator>

INDEX_TEMPLATE_ARGUMENTS
class IndexIterator {
 public:
  // you may define your own constructor based on your member variables
  IndexIterator(page_id_t page_id, int idx, BufferPoolManager &buff) :
      index(idx), bufferPoolManager(buff) {
    leafPage = GetLeafPage(page_id);
    assert(leafPage->GetSize() > index && index >= 0);
  }
  ~IndexIterator();

  IndexIterator(const IndexIterator &from) : IndexIterator(from.leafPage->GetPageId(),
                                                           from.index,
                                                           from.bufferPoolManager) {
  }
  IndexIterator &operator=(const IndexIterator &) = delete;

  bool isEnd() {
    return index == leafPage->GetSize() && leafPage->GetNextPageId() == INVALID_PAGE_ID;
  }

  const MappingType &operator*() {
    assert(!isEnd());
    const MappingType &ret = leafPage->GetItem(index);
    return ret;
  }

  IndexIterator &operator++() {
    index++;
    if (index >= leafPage->GetSize()) {
      index = 0;
      page_id_t next = leafPage->GetNextPageId();
      bufferPoolManager.UnpinPage(leafPage->GetPageId(), false);
      leafPage =
          reinterpret_cast<B_PLUS_TREE_LEAF_PAGE_TYPE *> (bufferPoolManager.FetchPage(next)->GetData());
    }
    return *this;
  }

 private:
  // add your own private member variables here
  B_PLUS_TREE_LEAF_PAGE_TYPE *leafPage;
  int index = 0;
  BufferPoolManager &bufferPoolManager;

  B_PLUS_TREE_LEAF_PAGE_TYPE *GetLeafPage(page_id_t page_id) {
    return reinterpret_cast<B_PLUS_TREE_LEAF_PAGE_TYPE *>(bufferPoolManager.FetchPage(page_id)->GetData());
  }
};

} // namespace cmudb
