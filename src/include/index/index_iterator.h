/**
 * index_iterator.h
 * For range scan of b+ tree
 */
#pragma once

#include "page/b_plus_tree_leaf_page.h"
#include "page/b_plus_tree_internal_page.h"
namespace cmudb {

#define INDEXITERATOR_TYPE                                                     \
  IndexIterator<KeyType, ValueType, KeyComparator>


#define LEAFPAGE_TYPE                                             \
  BPlusTreeLeafPage<KeyType, ValueType, KeyComparator>
#define B_PLUS_TREE_INTERNAL_PAGE_TYPE                                         \
  BPlusTreeInternalPage<KeyType, KeyComparator>

    INDEX_TEMPLATE_ARGUMENTS
    class IndexIterator {
    public:
        // you may define your own constructor based on your member variables
        IndexIterator(
                page_id_t page_id, KeyType key,
                BufferPoolManager *buffer_pool_manager, KeyComparator &cmp);

        IndexIterator();

        ~IndexIterator();

        bool isEnd();

        const MappingType &operator*();

        IndexIterator &operator++();

    private:
        LEAFPAGE_TYPE *leaf;
        BufferPoolManager *buffer_pool_manager;
        size_t idx;
        bool is_end;

    };

} // namespace cmudb
