/**
 * b_plus_tree_leaf_page.cpp
 */

#include <sstream>
#include <include/page/b_plus_tree_internal_page.h>

#include "common/exception.h"
#include "common/rid.h"
#include "page/b_plus_tree_leaf_page.h"

namespace cmudb {

/*****************************************************************************
 * HELPER METHODS AND UTILITIES
 *****************************************************************************/

    /**
     * Init method after creating a new leaf page
     * Including set page type, set current size to zero, set page id/parent id, set
     * next page id and set max size
     *
     * @tparam KeyType
     * @tparam ValueType
     * @tparam KeyComparator
     * @param page_id
     * @param parent_id
     */
    INDEX_TEMPLATE_ARGUMENTS
    void
    B_PLUS_TREE_LEAF_PAGE_TYPE::Init(page_id_t page_id, page_id_t parent_id) {
        SetPageType(IndexPageType::LEAF_PAGE);
        SetSize(0);
        SetPageId(page_id);
        SetParentPageId(parent_id);
        size_t size = (PAGE_SIZE - sizeof(B_PLUS_TREE_LEAF_PAGE_TYPE) /
                                           sizeof(MappingType));
        SetMaxSize(size);
        SetNextPageId(INVALID_PAGE_ID);
        //todo: where to set the previous page id?
    }

    /**
     * Helper methods to set/get next page id
     *
     * @tparam KeyType
     * @tparam ValueType
     * @tparam KeyComparator
     * @return
     */
    INDEX_TEMPLATE_ARGUMENTS
    page_id_t B_PLUS_TREE_LEAF_PAGE_TYPE::GetNextPageId() const {
        return next_page_id_;
    }

    INDEX_TEMPLATE_ARGUMENTS
    void B_PLUS_TREE_LEAF_PAGE_TYPE::SetNextPageId(page_id_t next_page_id) {}

    /**
     * Helper method to find the first index i so that array[i].first >= key
     * NOTE: This method is only used when generating index iterator
     *
     * @tparam KeyType
     * @tparam ValueType
     * @tparam KeyComparator
     * @param key
     * @param comparator
     * @return if found, return the key index; otherwise return -1
     */
    INDEX_TEMPLATE_ARGUMENTS
    int B_PLUS_TREE_LEAF_PAGE_TYPE::KeyIndex(
            const KeyType &key, const KeyComparator &comparator) const {
        for (int i = 0; i < GetSize(); i++){
            if (comparator(array[i].first, key) >= 0){
                return i;
            }
        }
        return -1;
    }

    /**
     * Helper method to find and return the key associated with input "index"(a.k.a
     * array offset)
     *
     * @tparam KeyType
     * @tparam ValueType
     * @tparam KeyComparator
     * @param index
     * @return
     */
    INDEX_TEMPLATE_ARGUMENTS
    KeyType B_PLUS_TREE_LEAF_PAGE_TYPE::KeyAt(int index) const {
        KeyType key;
        key = array[index].first;
        return key;
    }

    /**
     * Helper method to find and return the key & value pair associated with
     * input "index"(a.k.a array offset)
     *
     * @tparam KeyType
     * @tparam ValueType
     * @tparam KeyComparator
     * @param index
     * @return
     */
    INDEX_TEMPLATE_ARGUMENTS
    const MappingType &B_PLUS_TREE_LEAF_PAGE_TYPE::GetItem(int index) {
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
    int B_PLUS_TREE_LEAF_PAGE_TYPE::Insert(const KeyType &key,
                                           const ValueType &value,
                                           const KeyComparator &comparator) {
        int index = KeyIndex(key, comparator);
        for (int i = GetSize(); i > index; i--){
            array[i].first = array[i - 1].first;
            array[i].second = array[i - 1].second;
        }
        array[index].first = key;
        array[index].second = value;
        return index;
    }

/*****************************************************************************
 * SPLIT
 *****************************************************************************/
    /**
     * Remove half of key & value pairs from this page to "recipient" page
     *
     * @tparam KeyType
     * @tparam ValueType
     * @tparam KeyComparator
     * @param recipient
     * @param buffer_pool_manager
     */
    INDEX_TEMPLATE_ARGUMENTS
    void B_PLUS_TREE_LEAF_PAGE_TYPE::MoveHalfTo(
            BPlusTreeLeafPage *recipient,
            __attribute__((unused)) BufferPoolManager *buffer_pool_manager) {
        int move_n = GetSize() / 2;
        Page *parent_page = buffer_pool_manager->FetchPage
                (recipient->GetParentPageId());
        B_PLUS_TREE_INTERNAL_PAGE_TYPE *parent =
                reinterpret_cast<B_PLUS_TREE_INTERNAL_PAGE_TYPE*>
                (parent_page->GetData());
        size_t cur_index = parent->ValueIndex(GetPageId());
        size_t recipient_index = parent->ValueIndex
                (recipient->GetParentPageId());
        if (cur_index < recipient_index){
            size_t num = GetSize() - move_n;
            for (int i = move_n, j = 0; i < GetSize();i++,
                    j++){
                recipient->array[j + num].first = recipient->array[j].first;
                recipient->array[j + num].second = recipient->array[j].second;
                recipient->array[j].first = array[i].first;
                recipient->array[j].second = array[i].second;
           }
            IncreaseSize(-num);
            recipient->IncreaseSize(num);
            recipient->SetNextPageId(GetNextPageId());
            SetNextPageId(recipient->GetPageId());
            //todo: need to correct previous page's meta data
        }else{
            for (int i = 0, j = GetSize(); i < move_n;i++, j++){
                recipient->array[j].first = array[i].first;
                recipient->array[j].second = array[i].second;
                array[i].first = array[i + move_n].first;
                array[i].second = array[i + move_n].second;
            }
            IncreaseSize(-move_n);
            recipient->IncreaseSize(move_n);
            recipient->SetNextPageId(GetPageId());
            //todo: need the previous page id to correct its next page id
        }
    }

    INDEX_TEMPLATE_ARGUMENTS
    void
    B_PLUS_TREE_LEAF_PAGE_TYPE::CopyHalfFrom(MappingType *items, int size) {}

/*****************************************************************************
 * LOOKUP
 *****************************************************************************/
    /**
     * For the given key, check to see whether it exists in the leaf page. If it
     * does, then store its corresponding value in input "value" and return
     * true. If the key does not exist, then return false
     *
     * @tparam KeyType
     * @tparam ValueType
     * @tparam KeyComparator
     * @param key
     * @param value
     * @param comparator
     * @return
     */
    INDEX_TEMPLATE_ARGUMENTS
    bool
    B_PLUS_TREE_LEAF_PAGE_TYPE::Lookup(const KeyType &key, ValueType &value,
                                       const KeyComparator &comparator) const {
        for (int i = 0; i < GetSize(); i++){
            if (comparator(array[i].first, key) == 0){
                value = array[i].second;
                return true;
            }
        }
        return false;
    }

/*****************************************************************************
 * REMOVE
 *****************************************************************************/
    /**
     * First look through leaf page to see whether delete key exist or not. If
     * exist, perform deletion, otherwise return immediately.
     * NOTE: store key&value pair continuously after deletion
     *
     * @tparam KeyType
     * @tparam ValueType
     * @tparam KeyComparator
     * @param key
     * @param comparator
     * @return   page size after deletion
     */
    INDEX_TEMPLATE_ARGUMENTS
    int B_PLUS_TREE_LEAF_PAGE_TYPE::RemoveAndDeleteRecord(
            const KeyType &key, const KeyComparator &comparator) {
        for (int i = 0; i < GetSize(); i++){
            if (comparator(KeyAt(i), key) == 0){
                for (int j = i; j < (GetSize() - 1); j++){
                    array[j].first = array[j + 1].first;
                    array[j].second = array[j + 1].second;
                }
                IncreaseSize(-1);
                return GetSize();
            }
        }
        return GetSize();
    }

/*****************************************************************************
 * MERGE
 *****************************************************************************/
    /**
     * Remove all of key & value pairs from this page to "recipient" page, then
     * update next page id
     *
     * @tparam KeyType
     * @tparam ValueType
     * @tparam KeyComparator
     * @param recipient
     */
    INDEX_TEMPLATE_ARGUMENTS
    void B_PLUS_TREE_LEAF_PAGE_TYPE::MoveAllTo(BPlusTreeLeafPage *recipient,
                                               int, BufferPoolManager
                                               *buffer_pool_manager) {
        Page *parent_page = buffer_pool_manager->FetchPage
                (recipient->GetParentPageId());
        B_PLUS_TREE_INTERNAL_PAGE_TYPE *parent =
                reinterpret_cast<B_PLUS_TREE_INTERNAL_PAGE_TYPE*>
                (parent_page->GetData());
        size_t cur_index = parent->ValueIndex(GetPageId());
        size_t recipient_index = parent->ValueIndex
                (recipient->GetParentPageId());
        if (cur_index < recipient_index){
            size_t recipient_size = recipient->GetSize();
            for (int i = 0, j = 0; i < GetSize(); i++,j++){
                recipient->array[j + recipient_size].first =
                        recipient->array[j].first;
                recipient->array[j + recipient_size].second =
                        recipient->array[j].second;
                recipient->array[j].first = array[i].first;
                recipient->array[j].second = array[i].second;
            }
            recipient->IncreaseSize(GetSize());
            IncreaseSize(-GetSize());
            //todo: need to fix the next page id for th previous page
        }else{
            for (int i = 0, j = recipient->GetSize(); i < GetSize(); i++){
                recipient->array[j].first = array[i].first;
                recipient->array[j].second = array[i].second;
            }
            recipient->IncreaseSize(GetSize());
            IncreaseSize(- GetSize());
            recipient->SetNextPageId(GetNextPageId());
            //todo: need to fix the next page id for the previous page
        }
    }

    INDEX_TEMPLATE_ARGUMENTS
    void
    B_PLUS_TREE_LEAF_PAGE_TYPE::CopyAllFrom(MappingType *items, int size) {}

/*****************************************************************************
 * REDISTRIBUTE
 *****************************************************************************/
    /**
     * Remove the first key & value pair from this page to "recipient" page, then
     * update relevant key & value pair in its parent page.
     *
     * @tparam KeyType
     * @tparam ValueType
     * @tparam KeyComparator
     * @param recipient
     * @param buffer_pool_manager
     */
    INDEX_TEMPLATE_ARGUMENTS
    void B_PLUS_TREE_LEAF_PAGE_TYPE::MoveFirstToEndOf(
            BPlusTreeLeafPage *recipient,
            BufferPoolManager *buffer_pool_manager) {
        size_t recipient_size = recipient->GetSize();
        recipient->array[recipient_size].first = array[0].first;
        recipient->array[recipient_size].second = array[0].second;
        Page *parent_page = buffer_pool_manager->FetchPage(GetParentPageId());
        B_PLUS_TREE_INTERNAL_PAGE_TYPE *parent =
                reinterpret_cast<B_PLUS_TREE_INTERNAL_PAGE_TYPE*>
                (parent_page->GetData());
        size_t index_in_parent = parent->ValueIndex(GetParentPageId());
        parent->SetKeyAt(index_in_parent, array[1].first);

        for (int i = 1; i < GetSize(); i++){
            array[i - 1].first = array[i].first;
            array[i - 1].second = array[i].second;
        }
        recipient->IncreaseSize(1);
        IncreaseSize(-1);
    }

    INDEX_TEMPLATE_ARGUMENTS
    void B_PLUS_TREE_LEAF_PAGE_TYPE::CopyLastFrom(const MappingType &item) {}
    /**
     * Remove the last key & value pair from this page to "recipient" page, then
     * update relavent key & value pair in its parent page.
     *
     * @tparam KeyType
     * @tparam ValueType
     * @tparam KeyComparator
     * @param recipient
     * @param parentIndex
     * @param buffer_pool_manager
     */
    INDEX_TEMPLATE_ARGUMENTS
    void B_PLUS_TREE_LEAF_PAGE_TYPE::MoveLastToFrontOf(
            BPlusTreeLeafPage *recipient, int parentIndex,
            BufferPoolManager *buffer_pool_manager) {
        int recipient_size = recipient->GetSize();
        int cur_size = GetSize();
        for (int i = 0; i < recipient_size; i++){
            recipient->array[i + 1].first = recipient->array[i].first;
            recipient->array[i + 1].second = recipient->array[i].second;
        }
        recipient->array[0].first  = array[cur_size - 1].first;
        recipient->array[0].second = array[cur_size - 1].second;
        Page *parent_page = buffer_pool_manager->FetchPage(parentIndex);
        B_PLUS_TREE_INTERNAL_PAGE_TYPE *parent =
                reinterpret_cast<B_PLUS_TREE_INTERNAL_PAGE_TYPE*>
                (parent_page->GetData());
        parent->SetKeyAt(parentIndex, recipient->KeyAt(0));
        recipient->IncreaseSize(1);
        IncreaseSize(-1);
    }

    INDEX_TEMPLATE_ARGUMENTS
    void B_PLUS_TREE_LEAF_PAGE_TYPE::CopyFirstFrom(
            const MappingType &item, int parentIndex,
            BufferPoolManager *buffer_pool_manager) {}

/*****************************************************************************
 * DEBUG
 *****************************************************************************/
    INDEX_TEMPLATE_ARGUMENTS
    std::string B_PLUS_TREE_LEAF_PAGE_TYPE::ToString(bool verbose) const {
        if (GetSize() == 0) {
            return "";
        }
        std::ostringstream stream;
        if (verbose) {
            stream << "[pageId: " << GetPageId() << " parentId: "
                   << GetParentPageId()
                   << "]<" << GetSize() << "> ";
        }
        int entry = 0;
        int end = GetSize();
        bool first = true;

        while (entry < end) {
            if (first) {
                first = false;
            } else {
                stream << " ";
            }
            stream << std::dec << array[entry].first;
            if (verbose) {
                stream << "(" << array[entry].second << ")";
            }
            ++entry;
        }
        return stream.str();
    }


    /****************************** helper methods ***************************/

    /****************************** explicit instantiation *******************/
    template
    class BPlusTreeLeafPage<GenericKey<4>, RID,
            GenericComparator<4>>;

    template
    class BPlusTreeLeafPage<GenericKey<8>, RID,
            GenericComparator<8>>;

    template
    class BPlusTreeLeafPage<GenericKey<16>, RID,
            GenericComparator<16>>;

    template
    class BPlusTreeLeafPage<GenericKey<32>, RID,
            GenericComparator<32>>;

    template
    class BPlusTreeLeafPage<GenericKey<64>, RID,
            GenericComparator<64>>;
} // namespace cmudb
