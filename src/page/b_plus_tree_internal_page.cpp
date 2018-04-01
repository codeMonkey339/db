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
    /**
     * Init method after creating a new internal page
     * Including set page type, set current size, set page id, set parent id
     * and set max page size
     *
     * @tparam KeyType
     * @tparam ValueType
     * @tparam KeyComparator
     * @param page_id
     * @param parent_id
     */
    INDEX_TEMPLATE_ARGUMENTS
    void B_PLUS_TREE_INTERNAL_PAGE_TYPE::Init(page_id_t page_id,
                                              page_id_t parent_id) {
        SetPageType(IndexPageType::INTERNAL_PAGE);
        SetSize(0);
        SetPageId(page_id);
        SetParentPageId(parent_id);
        //todo: is this the correct way to calculate max size?
        size_t size = (PAGE_SIZE - sizeof(B_PLUS_TREE_INTERNAL_PAGE_TYPE)) /
                sizeof(MappingType);
        SetMaxSize(size);
        //todo: need to allocated memory to array
    }

    /**
     * Helper method to get/set the key associated with input "index"(a.k.a
     * array offset)
     *
     * @tparam KeyType
     * @tparam ValueType
     * @tparam KeyComparator
     * @param index
     * @return
     */
    INDEX_TEMPLATE_ARGUMENTS
    KeyType B_PLUS_TREE_INTERNAL_PAGE_TYPE::KeyAt(int index) const {
        KeyType key = array[index].first;
        return key;
    }

    INDEX_TEMPLATE_ARGUMENTS
    void
    B_PLUS_TREE_INTERNAL_PAGE_TYPE::SetKeyAt(int index, const KeyType &key) {
        MappingType entry = array[index];
        entry.first = key;
    }

    /**
     * Helper method to find and return array index(or offset), so that its value
     * equals to input "value"
     *
     * @tparam KeyType
     * @tparam ValueType
     * @tparam KeyComparator
     * @param value
     * @return if found, return the index; otherwise return -1
     */
    INDEX_TEMPLATE_ARGUMENTS
    int
    B_PLUS_TREE_INTERNAL_PAGE_TYPE::ValueIndex(const ValueType &value) const {
        for (size_t i = 0; i < static_cast<size_t>(GetSize()); i++){
            ValueType v = array[i].second;
            if (memcmp(&v, &value, sizeof(ValueType)) == 0){
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
    ValueType
    B_PLUS_TREE_INTERNAL_PAGE_TYPE::ValueAt(int index) const {
        assert(index < GetMaxSize() && index >= 0);
        ValueType v = array[index].second;
        return v;
    }

/*****************************************************************************
 * LOOKUP
 *****************************************************************************/
    /**
     * Find and return the child pointer(page_id) which points to the child page
     * that contains input "key" Start the search from the second key(the
     * first key should always be invalid)
     *
     * @tparam KeyType
     * @tparam ValueType
     * @tparam KeyComparator
     * @param key
     * @param comparator
     * @return
     */
    INDEX_TEMPLATE_ARGUMENTS
    ValueType
    B_PLUS_TREE_INTERNAL_PAGE_TYPE::Lookup(const KeyType &key,
                                           const KeyComparator &comparator) const {
        for (size_t i = 1; i < static_cast<size_t>(GetSize()); i++){
            KeyType k = array[i].first;
            if (comparator(key, k) < 0){
                return array[i - 1].second;
            }
        }
        return INVALID_PAGE_ID;
    }

/*****************************************************************************
 * INSERTION
 *****************************************************************************/
    /**
     * Populate new root page with old_value + new_key & new_value
     * When the insertion cause overflow from leaf page all the way upto the
     * root page, you should create a new root page and populate its elements.
     * NOTE: This method is only called within InsertIntoParent()(b_plus_tree
     * .cpp)
     *
     * @tparam KeyType
     * @tparam ValueType
     * @tparam KeyComparator
     * @param old_value
     * @param new_key
     * @param new_value
     */
    INDEX_TEMPLATE_ARGUMENTS
    void B_PLUS_TREE_INTERNAL_PAGE_TYPE::PopulateNewRoot(
            const ValueType &old_value, const KeyType &new_key,
            const ValueType &new_value) {
        //todo:
    }
    /**
     * Insert new_key & new_value pair right after the pair with its value ==
     * old_value. No need to consider overflow, this has been taken care by
     * the B+ tree itself
     *
     * @tparam KeyType
     * @tparam ValueType
     * @tparam KeyComparator
     * @param old_value
     * @param new_key
     * @param new_value
     * @return:  new size after insertion
     */
    INDEX_TEMPLATE_ARGUMENTS
    int B_PLUS_TREE_INTERNAL_PAGE_TYPE::InsertNodeAfter(
            const ValueType &old_value, const KeyType &new_key,
            const ValueType &new_value) {
        assert(GetSize() < GetMaxSize());
        size_t old_index = ValueIndex(old_value);
        assert(old_index != -1);
        for (size_t i = GetSize() - 1; i > old_index; i--){
            MappingType old_pair = array[i];
            MappingType new_pair = array[i + 1];
            new_pair.swap(old_pair);
        }
        array[old_index + 1].first = new_key;
        array[old_index + 1].second = new_value;
        IncreaseSize(1);
        return GetSize();
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
    void B_PLUS_TREE_INTERNAL_PAGE_TYPE::MoveHalfTo(
            BPlusTreeInternalPage *recipient,
            BufferPoolManager *buffer_pool_manager) {
        size_t move_n = GetSize() / 2;
        for (size_t i = move_n, j = 0; i < GetSize(); i++, j++){
            recipient->array[j].first = array[move_n].first;
            recipient->array[j].second = array[move_n].second;
        }

        SetSize(move_n);
        recipient->IncreaseSize(GetSize() - move_n);
        for (size_t i = 0; i < GetSize(); i++){
            page_id_t  page_id = recipient->ValueIndex(i);
            Page *page = buffer_pool_manager->FetchPage(page_id);
            BPlusTreePage *tree_page = reinterpret_cast<BPlusTreePage*>
            (page->GetData());
            tree_page->SetParentPageId(recipient->GetPageId());
            buffer_pool_manager->UnpinPage(page_id, true);
        }
    }

    INDEX_TEMPLATE_ARGUMENTS
    void B_PLUS_TREE_INTERNAL_PAGE_TYPE::CopyHalfFrom(
            MappingType *items, int size,
            BufferPoolManager *buffer_pool_manager) {
        //todo:
    }

/*****************************************************************************
 * REMOVE
 *****************************************************************************/
    /**
     * Remove the key & value pair in internal page according to input index(a.k.a
     * array offset)
     * NOTE: store key&value pair continuously after deletion
     *
     * @tparam KeyType
     * @tparam ValueType
     * @tparam KeyComparator
     * @param index
     */
    INDEX_TEMPLATE_ARGUMENTS
    void B_PLUS_TREE_INTERNAL_PAGE_TYPE::Remove(int index) {
        assert(index >= 0 && index < GetSize());
        for (size_t i = index; i < (GetSize() - 1); i++){
            array[i].first = array[i + 1].first;
            array[i].second = array[i + 1].second;
        }
        IncreaseSize( -1);
    }

    /**
     * Remove the only key & value pair in internal page and return the value
     * NOTE: only call this method within AdjustRoot()(in b_plus_tree.cpp)
     *
     * @tparam KeyType
     * @tparam ValueType
     * @tparam KeyComparator
     * @return
     */
    INDEX_TEMPLATE_ARGUMENTS
    ValueType B_PLUS_TREE_INTERNAL_PAGE_TYPE::RemoveAndReturnOnlyChild() {
        ValueType value = array[0].second;
        Remove(0);
        return value;
    }
/*****************************************************************************
 * MERGE
 *****************************************************************************/
/*
 * Remove all of key & value pairs from this page to "recipient" page, then
 * update relavent key & value pair in its parent page.
 */
    INDEX_TEMPLATE_ARGUMENTS
    void B_PLUS_TREE_INTERNAL_PAGE_TYPE::MoveAllTo(
            BPlusTreeInternalPage *recipient, int index_in_parent,
            BufferPoolManager *buffer_pool_manager) {
        //todo:
    }

    INDEX_TEMPLATE_ARGUMENTS
    void B_PLUS_TREE_INTERNAL_PAGE_TYPE::CopyAllFrom(
            MappingType *items, int size,
            BufferPoolManager *buffer_pool_manager) {}

/*****************************************************************************
 * REDISTRIBUTE
 *****************************************************************************/
/*
 * Remove the first key & value pair from this page to tail of "recipient"
 * page, then update relavent key & value pair in its parent page.
 */
    INDEX_TEMPLATE_ARGUMENTS
    void B_PLUS_TREE_INTERNAL_PAGE_TYPE::MoveFirstToEndOf(
            BPlusTreeInternalPage *recipient,
            BufferPoolManager *buffer_pool_manager) {}

    INDEX_TEMPLATE_ARGUMENTS
    void B_PLUS_TREE_INTERNAL_PAGE_TYPE::CopyLastFrom(
            const MappingType &pair, BufferPoolManager *buffer_pool_manager) {}

/*
 * Remove the last key & value pair from this page to head of "recipient"
 * page, then update relavent key & value pair in its parent page.
 */
    INDEX_TEMPLATE_ARGUMENTS
    void B_PLUS_TREE_INTERNAL_PAGE_TYPE::MoveLastToFrontOf(
            BPlusTreeInternalPage *recipient, int parent_index,
            BufferPoolManager *buffer_pool_manager) {}

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
            os << "[pageId: " << GetPageId() << " parentId: "
               << GetParentPageId()
               << "]<" << GetSize() << "> ";
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
