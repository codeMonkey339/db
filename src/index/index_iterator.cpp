/**
 * index_iterator.cpp
 */
#include <cassert>

#include "index/index_iterator.h"

namespace cmudb {

/*
 * NOTE: you can change the destructor/constructor method here
 * set your own input parameters
 */
    INDEX_TEMPLATE_ARGUMENTS
    INDEXITERATOR_TYPE::IndexIterator(
                page_id_t page_id, KeyType key,
                BufferPoolManager *buffer_pool_manager, KeyComparator *cmp):
    buffer_pool_manager(buffer_pool_manager), is_end(false) {
        Page *page = buffer_pool_manager->FetchPage(page_id);
        leaf = reinterpret_cast<BPlusTreeLeafPage*>(page->GetData());
        idx = leaf->KeyIndex(key, cmp);
    }

    INDEX_TEMPLATE_ARGUMENTS
    INDEXITERATOR_TYPE::~IndexIterator() {
        if (!is_end){
            page_id_t page_id = leaf->GetPageId();
            buffer_pool_manager->UnpinPage(page_id, false);
        }
    }

    INDEX_TEMPLATE_ARGUMENTS
    bool INDEXITERATOR_TYPE::isEnd() {
        return is_end;
    }

    INDEX_TEMPLATE_ARGUMENTS
    const MappingType& INDEXITERATOR_TYPE::operator*() {
        assert(!is_end);
        const MappingType entry = leaf->GetItem(idx);
        return entry;
    }


    INDEX_TEMPLATE_ARGUMENTS
    IndexIterator& INDEXITERATOR_TYPE::operator++() {
        if (idx == (leaf->GetSize() - 1)){
            page_id_t  next_id = leaf->GetNextPageId();
            if (next_id == INVALID_PAGE_ID){
                is_end = true;
            }else{
                buffer_pool_manager->UnpinPage(leaf->GetPageId(), false);
                Page *page = buffer_pool_manager->FetchPage(next_id);
                leaf = reinterpret_cast<BPlusTreeLeafPage*>(page->GetData());
                idx = 0;
                return *this;
            }
        }else{
            idx++;
            return *this;
        }
    }


    template
    class IndexIterator<GenericKey<4>, RID, GenericComparator<4>>;

    template
    class IndexIterator<GenericKey<8>, RID, GenericComparator<8>>;

    template
    class IndexIterator<GenericKey<16>, RID, GenericComparator<16>>;

    template
    class IndexIterator<GenericKey<32>, RID, GenericComparator<32>>;

    template
    class IndexIterator<GenericKey<64>, RID, GenericComparator<64>>;

} // namespace cmudb
