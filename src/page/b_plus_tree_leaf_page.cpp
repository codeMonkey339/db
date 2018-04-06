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
        size_t size =
                (PAGE_SIZE - sizeof(B_PLUS_TREE_LEAF_PAGE_TYPE) / sizeof(MappingType));
        size &= (~1);
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
    void B_PLUS_TREE_LEAF_PAGE_TYPE::SetNextPageId(page_id_t next_page_id) {
        this->next_page_id_ = next_page_id;
    }

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

    INDEX_TEMPLATE_ARGUMENTS
    int B_PLUS_TREE_LEAF_PAGE_TYPE::findInsertPos(KeyType key,
                                                  KeyComparator comparator) {
        for (int i = 1; i < GetSize(); i++){
            if (comparator(key, array[i].first) < 0){
                return i - 1;
            }
        }
        return (GetSize() - 1);
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
    /**
     * Insert key & value pair into leaf page ordered by key
     *
     * @tparam KeyType
     * @tparam ValueType
     * @tparam KeyComparator
     * @param key
     * @param value
     * @param comparator
     * @return  page size after insertion
     */
    INDEX_TEMPLATE_ARGUMENTS
    int B_PLUS_TREE_LEAF_PAGE_TYPE::Insert(const KeyType &key,
                                           const ValueType &value,
                                           const KeyComparator &comparator) {
        int index = findInsertPos(key, comparator);
        if (comparator(array[index].first, key) == 0){
            return GetSize();
        }
        index = index > 0?index:0;
        for (int i = GetSize(); i > index; i--){
            array[i].first = array[i - 1].first;
            array[i].second = array[i - 1].second;
        }
        array[index].first = key;
        array[index].second = value;
        IncreaseSize(1);
        return GetSize();
    }

/*****************************************************************************
 * SPLIT
 *****************************************************************************/
    /**
     * Remove half of key & value pairs from this page to "recipient" page
     *
     * this method is only used when Splitting a node, the recipient is
     * always an elder sibling to the giving node
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
        assert(recipient->GetSize() == 0 && GetSize() == (GetMaxSize() - 1));
        int move_n = GetSize() / 2;
        size_t num = GetSize() - move_n;
        for (int i = move_n, j = 0; i < GetSize();i++,
                j++){
            recipient->array[j].first = array[i].first;
            recipient->array[j].second = array[i].second;
            page_id_t  page_id = static_cast<RID>(array[i].second).GetPageId();
            Page *page = buffer_pool_manager->FetchPage(page_id);
            BPlusTreePage *tree_page = reinterpret_cast<BPlusTreePage*>
            (page->GetData());
            tree_page->SetParentPageId(recipient->GetPageId());
            buffer_pool_manager->UnpinPage(page_id, true);
        }
        IncreaseSize(-num);
        recipient->IncreaseSize(num);
        recipient->SetNextPageId(GetNextPageId());
        SetNextPageId(recipient->GetPageId());
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
     * the recipient is always a young sibling
     *
     * @tparam KeyType
     * @tparam ValueType
     * @tparam KeyComparator
     * @param recipient
     */
    INDEX_TEMPLATE_ARGUMENTS
    void B_PLUS_TREE_LEAF_PAGE_TYPE::MoveAllTo(BPlusTreeLeafPage *recipient,
                                               int, BufferPoolManager*) {
        for (int i = 0, j = recipient->GetSize(); i < GetSize(); i++){
            recipient->array[j].first = array[i].first;
            recipient->array[j].second = array[i].second;
            /* no need to update parent, since for leaf page entries are no
             * longer pointing to tree pages
             */
        }
        recipient->IncreaseSize(GetSize());
        IncreaseSize(-1 * GetSize());
        recipient->SetNextPageId(GetNextPageId());
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
        //implies that the recipient is young sibling
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
        buffer_pool_manager->UnpinPage(GetParentPageId(), true);
    }

    INDEX_TEMPLATE_ARGUMENTS
    void B_PLUS_TREE_LEAF_PAGE_TYPE::CopyLastFrom(const MappingType &item) {}
    /**
     * Remove the last key & value pair from this page to "recipient" page, then
     * update relevant key & value pair in its parent page.
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
        // implies that the recipient is an elder sibling
        int recipient_size = recipient->GetSize();
        int cur_size = GetSize();
        Page *parent_page = buffer_pool_manager->FetchPage(parentIndex);
        B_PLUS_TREE_INTERNAL_PAGE_TYPE *parent =
                reinterpret_cast<B_PLUS_TREE_INTERNAL_PAGE_TYPE*>
                (parent_page->GetData());
        for (int i = 0; i < recipient_size; i++){
            recipient->array[i + 1].first = recipient->array[i].first;
            recipient->array[i + 1].second = recipient->array[i].second;
        }
        recipient->array[0].first  = array[cur_size - 1].first;
        recipient->array[0].second = array[cur_size - 1].second;
        parent->SetKeyAt(parentIndex, recipient->KeyAt(0));
        recipient->IncreaseSize(1);
        IncreaseSize(-1);
        buffer_pool_manager->UnpinPage(parentIndex, true);
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
