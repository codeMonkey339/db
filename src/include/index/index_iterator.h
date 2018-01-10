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
    assert(index >= 0);
    noMoreRecords = leafPage->GetSize() <= index;
  }
  ~IndexIterator();

  IndexIterator(const IndexIterator &from) : IndexIterator(from.leafPage->GetPageId(),
                                                           from.index,
                                                           from.bufferPoolManager) {
  }
  IndexIterator &operator=(const IndexIterator &) = delete;

  bool isEnd() {
    return noMoreRecords;
  }

  const MappingType &operator*() {
    assert(!isEnd());
    const MappingType &ret = leafPage->GetItem(index);
    return ret;
  }

  IndexIterator &operator++() {
    index++;
    if (index >= leafPage->GetSize()) {
      page_id_t next = leafPage->GetNextPageId();
      if (next == INVALID_PAGE_ID) {
        noMoreRecords = true;
      } else {
        index = 0;
        bufferPoolManager.UnpinPage(leafPage->GetPageId(), false);
        leafPage =
            reinterpret_cast<B_PLUS_TREE_LEAF_PAGE_TYPE *> (bufferPoolManager.FetchPage(next)->GetData());
      }
    }
    return *this;
  }

 private:
  // add your own private member variables here
  B_PLUS_TREE_LEAF_PAGE_TYPE *leafPage;
  int index;
  BufferPoolManager &bufferPoolManager;
  bool noMoreRecords;

  B_PLUS_TREE_LEAF_PAGE_TYPE *GetLeafPage(page_id_t page_id) {
    if (page_id == INVALID_PAGE_ID) { return nullptr; }
    return reinterpret_cast<B_PLUS_TREE_LEAF_PAGE_TYPE *>(bufferPoolManager.FetchPage(page_id)->GetData());
  }
};

} // namespace cmudb
