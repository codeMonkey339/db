/**
 * b_plus_tree.cpp
 */
#include <iostream>
#include <string>

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
              buffer_pool_manager_(buffer_pool_manager),
              comparator_(comparator) {}

/*
 * Helper function to decide whether current b+tree is empty
 */
    INDEX_TEMPLATE_ARGUMENTS
    bool BPLUSTREE_TYPE::IsEmpty() const {
        return root_page_id_ == INVALID_PAGE_ID;
    }
/*****************************************************************************
 * SEARCH
 *****************************************************************************/
    /**
     * Return the only value that associated with input key
     * This method is used for point query
     * @tparam KeyType
     * @tparam ValueType
     * @tparam KeyComparator
     * @param key
     * @param result
     * @param transaction
     * @return true means key exists
     */
    INDEX_TEMPLATE_ARGUMENTS
    bool BPLUSTREE_TYPE::GetValue(const KeyType &key,
                                  std::vector<ValueType> &result,
                                  Transaction *transaction) {
        if (root_page_id_ == INVALID_PAGE_ID){
            return false;
        }
        Page *page = buffer_pool_manager_->FetchPage(root_page_id_);
        return getValue(page, key, result, transaction);
    }

    INDEX_TEMPLATE_ARGUMENTS
    bool BPLUSTREE_TYPE::getValue(Page *page, const KeyType &key,
                                  std::vector<ValueType> &result,
                                  Transaction *trans) {
        LEAFPAGE_TYPE *leaf = getLeafPage(key, page, trans);
        ValueType value;
        bool res = leaf->Lookup(key, value, comparator_);
        if (res){
            result.push_back(value);
            return true;
        }else{
            return false;
        }
    }

/*****************************************************************************
 * INSERTION
 *****************************************************************************/
    /**
     * Insert constant key & value pair into b+ tree
     * if current tree is empty, start new tree, update root page id and insert
     * entry, otherwise insert into leaf page.
     * @tparam KeyType
     * @tparam ValueType
     * @tparam KeyComparator
     * @param key
     * @param value
     * @param transaction
     * @return: since we only support unique key, if user try to insert duplicate
     * keys return false, otherwise return true.
     */
    INDEX_TEMPLATE_ARGUMENTS
    bool BPLUSTREE_TYPE::Insert(const KeyType &key, const ValueType &value,
                                Transaction *transaction) {
        if (root_page_id_ == INVALID_PAGE_ID){
            StartNewTree(key, value);
            return true;
        }else{
            return InsertIntoLeaf(key, value, transaction);
        }
    }
/*
 */
    /**
     * Insert constant key & value pair into an empty tree
     * User needs to first ask for new page from buffer pool manager(NOTICE:
     * throw an "out of memory" exception if returned value is nullptr), then
     * update b+ tree's root page id and insert entry directly into leaf page.
     * @tparam KeyType
     * @tparam ValueType
     * @tparam KeyComparator
     * @param key
     * @param value
     */
    INDEX_TEMPLATE_ARGUMENTS
    void
    BPLUSTREE_TYPE::StartNewTree(const KeyType &key, const ValueType &value) {
        Page *root = buffer_pool_manager_->NewPage(root_page_id_);
        if (root == nullptr){
            throw std::bad_alloc();
        }else{
            B_PLUS_TREE_LEAF_PAGE_TYPE *leaf =
            reinterpret_cast<B_PLUS_TREE_LEAF_PAGE_TYPE*>(root->GetData());
            leaf->Init(root_page_id_, INVALID_PAGE_ID);
            leaf->Insert(key, value, comparator_);
        }
    }

/*
 */
    /**
     * Insert constant key & value pair into leaf page
     * User needs to first find the right leaf page as insertion target, then
     * look through leaf page to see whether insert key exist or not. If
     * exist, return immediately, otherwise insert entry. Remember to deal
     * with split if necessary.
     * @tparam KeyType
     * @tparam ValueType
     * @tparam KeyComparator
     * @param key
     * @param value
     * @param transaction
     * @return: since we only support unique key, if user try to insert
     * duplicate keys return false, otherwise return true.
     */
    INDEX_TEMPLATE_ARGUMENTS
    bool
    BPLUSTREE_TYPE::InsertIntoLeaf(const KeyType &key, const ValueType &value,
                                   Transaction *transaction) {
        /* implementation is a variant of pseudo-code in textbook */
        if (root_page_id_ == INVALID_PAGE_ID){
            return false;
        }
        Page *page = buffer_pool_manager_->FetchPage(root_page_id_);
        B_PLUS_TREE_LEAF_PAGE_TYPE *leaf = getLeafPage(key, page, transaction);
        if (leaf->KeyIndex(key, comparator_) < 0){
            //note: always leave 1 to allow the last insertion
            leaf->Insert(key, value, comparator_);
            if(leaf->GetSize() >= (leaf->GetMaxSize() - 1)){
                B_PLUS_TREE_LEAF_PAGE_TYPE *new_page = Split(leaf);
                /* for leaf pages, key at index 0 carries valid keys */
                InsertIntoParent(leaf, new_page->KeyAt(0), new_page,
                                 transaction);
            }
            return true;
        }else{
            // duplicate key
            return false;
        }
   }

    INDEX_TEMPLATE_ARGUMENTS
    LEAFPAGE_TYPE*
    BPLUSTREE_TYPE::getLeafPage(const KeyType &key, Page *page,
                                Transaction *transaction) {
        BPlusTreePage *treePage = reinterpret_cast<BPlusTreePage*>(page->GetData
                ());
        if (treePage->IsLeafPage()){
            LEAFPAGE_TYPE *leafPage = reinterpret_cast<LEAFPAGE_TYPE*>(treePage);
            return leafPage;
        }else{
            INTERNALPAGE_TYPE *internalPage =
                    reinterpret_cast<INTERNALPAGE_TYPE*>(page->GetData());
            page_id_t value = internalPage->Lookup(key, comparator_);
            page = buffer_pool_manager_->FetchPage(value);
            return getLeafPage(key, page, transaction);
        }

    }

/*
 */
    /**
     * Split input page and return newly created page.
     * Using template N to represent either internal page or leaf page.
     * User needs to first ask for new page from buffer pool manager(NOTICE:
     * throw an "out of memory" exception if returned value is nullptr), then
     * move half of key & value pairs from input page to newly created page
     *
     * @tparam KeyType
     * @tparam ValueType
     * @tparam KeyComparator
     * @tparam N
     * @param node
     * @return
     */
    INDEX_TEMPLATE_ARGUMENTS
    template<typename N>
    N *BPLUSTREE_TYPE::Split(N *node) {
        page_id_t  id;
        Page *page = buffer_pool_manager_->NewPage(id);
        if (page == nullptr){
            throw std::bad_alloc();
        }
        BPlusTreePage *tree_page = reinterpret_cast<BPlusTreePage*>(node);
        if (tree_page->IsLeafPage()){
            B_PLUS_TREE_LEAF_PAGE_TYPE *old_page =
            reinterpret_cast<B_PLUS_TREE_LEAF_PAGE_TYPE*>(node);
            B_PLUS_TREE_LEAF_PAGE_TYPE *new_page =
                    reinterpret_cast<B_PLUS_TREE_LEAF_PAGE_TYPE*>(page->GetData());
            new_page->Init(id, old_page->GetParentPageId());
            old_page->MoveHalfTo(new_page, buffer_pool_manager_);
            return reinterpret_cast<N*>(new_page);
        }else{
            B_PLUS_TREE_INTERNAL_PAGE_TYPE *old_page =
                    reinterpret_cast<B_PLUS_TREE_INTERNAL_PAGE_TYPE*>(node);
            B_PLUS_TREE_INTERNAL_PAGE_TYPE *new_page =
                    reinterpret_cast<B_PLUS_TREE_INTERNAL_PAGE_TYPE*>(page->GetData());
            new_page->Init(id, old_page->GetParentPageId());
            old_page->MoveHalfTo(new_page, buffer_pool_manager_);
            return reinterpret_cast<N*>(new_page);
        }
    }

    /**
     * User needs to first find the parent page of old_node, parent node must be
     * adjusted to take info of new_node into account. Remember to deal with
     * split recursively if necessary.
     *
     * new_node is always an elder sibling to the old_node
     *
     * @tparam KeyType
     * @tparam ValueType
     * @tparam KeyComparator
     * @param   old_node      input page from split() method
     * @param   key
     * @param   new_node      returned page from split() method
     * @param transaction
     */
    INDEX_TEMPLATE_ARGUMENTS
    void BPLUSTREE_TYPE::InsertIntoParent(BPlusTreePage *old_node,
                                          const KeyType &key,
                                          BPlusTreePage *new_node,
                                          Transaction *transaction) {
        if (old_node->IsRootPage()){
            Page *new_page = buffer_pool_manager_->NewPage(root_page_id_);
            old_node->SetParentPageId(root_page_id_);
            new_node->SetParentPageId(root_page_id_);
            if (old_node->IsLeafPage()){
                B_PLUS_TREE_INTERNAL_PAGE_TYPE *new_root_page =
                        reinterpret_cast<B_PLUS_TREE_INTERNAL_PAGE_TYPE*>
                        (new_page->GetData());
                new_root_page->Init(root_page_id_, INVALID_PAGE_ID);
                new_root_page->PopulateNewRoot(old_node->GetPageId(), key,
                                               new_page->GetPageId());
            }else{
                B_PLUS_TREE_INTERNAL_PAGE_TYPE *new_root_page =
                        reinterpret_cast<B_PLUS_TREE_INTERNAL_PAGE_TYPE*>(new_page);
                new_root_page->Init(root_page_id_, INVALID_PAGE_ID);
                new_root_page->PopulateNewRoot(old_node->GetPageId(), key,
                                               new_page->GetPageId());
            }
        }else{
            B_PLUS_TREE_INTERNAL_PAGE_TYPE *parent = getParentPage
                    (old_node->GetParentPageId(), buffer_pool_manager_);
            parent->InsertNodeAfter(old_node->GetPageId(), key, new_node->GetPageId());
            if (parent->GetSize() >= (parent->GetMaxSize() - 1)){
                B_PLUS_TREE_INTERNAL_PAGE_TYPE *next_node = Split(parent);
                InsertIntoParent(parent, next_node->KeyAt(0), next_node);
            }
        }
    }

/*****************************************************************************
 * REMOVE
 *****************************************************************************/
    /**
     * Delete key & value pair associated with input key
     * If current tree is empty, return immediately.
     * If not, User needs to first find the right leaf page as deletion
     * target, then delete entry from leaf page. Remember to deal with
     * redistribute or merge if necessary.
     * @tparam KeyType
     * @tparam ValueType
     * @tparam KeyComparator
     * @param key
     * @param transaction
     */
    INDEX_TEMPLATE_ARGUMENTS
    void BPLUSTREE_TYPE::Remove(const KeyType &key, Transaction *transaction) {
        if (root_page_id_ == INVALID_PAGE_ID){
            return;
        }
        Page *page = buffer_pool_manager_->FetchPage(root_page_id_);
        B_PLUS_TREE_LEAF_PAGE_TYPE *leaf = getLeafPage(key, page, transaction);
        ValueType value = leaf->GetPageId();
        remove_entry(key, value, leaf, transaction);
    }


    /**
     * helper method
     *
     * @tparam KeyType
     * @tparam ValueType
     * @tparam KeyComparator
     * @tparam N
     * @param key the key to be deleted
     * @param value the value associated with the key to be deleted
     * @param node the page contains the key/value to be deleted
     * @param transaction
     */
    INDEX_TEMPLATE_ARGUMENTS
    template <typename N>
    void BPLUSTREE_TYPE::remove_entry(const KeyType &key, ValueType &value,
                                      N *node, Transaction *transaction) {
        //todo: fix code here
        BPlusTreePage *page = reinterpret_cast<BPlusTreePage*>(node);
        reinterpret_cast<LEAFPAGE_TYPE*>(page)->RemoveAndDeleteRecord
                (key, comparator_);
        if (page->IsRootPage() && page->GetSize() == 1){
            B_PLUS_TREE_INTERNAL_PAGE_TYPE *root
                    =reinterpret_cast<B_PLUS_TREE_INTERNAL_PAGE_TYPE*>(page);
            ValueType new_root = root->ValueAt(0);
            root_page_id_ = static_cast<RID>(new_root).GetPageId();
            buffer_pool_manager_->UnpinPage(page->GetPageId(), false);
            buffer_pool_manager_->DeletePage(page->GetPageId());
        }else{
            if (needCoalesceOrRedist(page->GetSize(), page->GetMaxSize())){
                CoalesceOrRedistribute(node, transaction);
            }
        }
    }


    //todo: need to clear this method and remove_entry
    INDEX_TEMPLATE_ARGUMENTS
    template <typename N>
    void BPLUSTREE_TYPE::remove_entry_internal(const KeyType &key, page_id_t
    &value, N *node, Transaction *transaction) {
        BPlusTreePage *page = reinterpret_cast<BPlusTreePage*>(node);
        INTERNALPAGE_TYPE *page_internal =
                reinterpret_cast<INTERNALPAGE_TYPE*>(page);
        size_t keyIndex = page_internal->ValueIndex(value);
        page_internal->Remove(keyIndex);

        if (page->IsRootPage() && page->GetSize() == 1){
            B_PLUS_TREE_INTERNAL_PAGE_TYPE *root
                    =reinterpret_cast<B_PLUS_TREE_INTERNAL_PAGE_TYPE*>(page);
            page_id_t new_root = root->ValueAt(0);
            root_page_id_ = new_root;
            buffer_pool_manager_->UnpinPage(page->GetPageId(), false);
            buffer_pool_manager_->DeletePage(page->GetPageId());
        }else{
            if (needCoalesceOrRedist(page->GetSize(), page->GetMaxSize())){
                CoalesceOrRedistribute(node, transaction);
            }
        }
    }




    /**
     * User needs to first find the sibling of input page. If sibling's size
     * + input page's size > page's max size, then redistribute. Otherwise, merge.
     * Using template N to represent either internal page or leaf page.
     *
     * @tparam KeyType
     * @tparam ValueType
     * @tparam KeyComparator
     * @tparam N
     * @param node
     * @param transaction
     * @return true means target leaf page should be deleted, false means no
     * deletion happens
     */
    INDEX_TEMPLATE_ARGUMENTS
    template<typename N>
    void
    BPLUSTREE_TYPE::CoalesceOrRedistribute(N *node, Transaction *transaction) {
        if (!try_coalesce(node, transaction)){
            try_redistribute(node, transaction);
        }
    }


    /**
     * check whether deletion of this page calls for coalesce or redistribution
     *
     * @tparam KeyType
     * @tparam ValueType
     * @tparam KeyComparator
     * @param parent_size
     * @param parent_max_size
     * @return
     */
    INDEX_TEMPLATE_ARGUMENTS
    bool
    BPLUSTREE_TYPE::needCoalesceOrRedist(size_t size, size_t max_size) {
        return !(size > std::ceil( max_size /2) && size <= (max_size -1));
    }

    /**
     * try all siblings of the node to see if coalesce is possible
     *
     * @tparam KeyType
     * @tparam ValueType
     * @tparam KeyComparator
     * @tparam N
     * @param node
     * @param parent
     * @param tran
     * @return
     */
    //todo: basically impossible to reuse typedef here? page_id_t and RID mixes!
    INDEX_TEMPLATE_ARGUMENTS
    template<typename N>
    bool BPLUSTREE_TYPE::try_coalesce(N *node, Transaction *tran) {
        BPlusTreePage *page = reinterpret_cast<BPlusTreePage*>(node);
        page_id_t parent_page_id = page->GetParentPageId();
        B_PLUS_TREE_INTERNAL_PAGE_TYPE *parent =
                reinterpret_cast<B_PLUS_TREE_INTERNAL_PAGE_TYPE*>
                (buffer_pool_manager_->FetchPage(parent_page_id)->GetData());
        size_t keyIndex = parent->ValueIndex(page->GetPageId());
        KeyType separate_key = parent->KeyAt(keyIndex);
        if (keyIndex >= 1){
            page_id_t young_sib_id = parent->ValueAt(keyIndex - 1);
            Page *young_sib_page=buffer_pool_manager_->FetchPage(young_sib_id);
            BPlusTreePage *sib_page = reinterpret_cast<BPlusTreePage*>
            (young_sib_page->GetData());
            if (Coalesce(sib_page, page, parent, keyIndex, tran)){
                page_id_t separate_value = parent->ValueAt(keyIndex);
                remove_entry_internal(separate_key, separate_value, parent, tran);
                buffer_pool_manager_->DeletePage(page->GetPageId());
                buffer_pool_manager_->UnpinPage(parent_page_id, true);
                buffer_pool_manager_->UnpinPage(young_sib_id, true);
                return true;
            }
        }
        if (keyIndex < static_cast<size_t>(parent->GetSize() - 1)){
            page_id_t old_sib_id = parent->ValueAt(keyIndex + 1);
            Page *old_sib_page =buffer_pool_manager_->FetchPage(old_sib_id);
            BPlusTreePage *sib_page = reinterpret_cast<BPlusTreePage*>
            (old_sib_page->GetData());
            if (Coalesce(page, sib_page, parent, keyIndex, tran)){
                page_id_t separate_value = parent->ValueAt(keyIndex);
                remove_entry_internal(separate_key, separate_value, parent, tran);
                buffer_pool_manager_->DeletePage(sib_page->GetPageId());
                buffer_pool_manager_->UnpinPage(parent_page_id, true);
                buffer_pool_manager_->UnpinPage(old_sib_id, true);
                return true;
            }
        }
        buffer_pool_manager_->UnpinPage(parent_page_id, false);
        return false;
    }

    /**
     * Move all the key & value pairs from one page to its sibling page, and notify
     * buffer pool manager to delete this page. Parent page must be adjusted to
     * take info of deletion into account. Remember to deal with coalesce or
     * redistribute recursively if necessary.
     * Using template N to represent either internal page or leaf page.
     * @tparam KeyType
     * @tparam ValueType
     * @tparam KeyComparator
     * @tparam N
     * @param neighbor_node sibling page of input "node"
     * @param node input from method coalesceOrRedistribute()
     * @param parent parent page of input "node"
     * @param index the index of the current page in the parent page
     * @param transaction
     * @return
     */
    INDEX_TEMPLATE_ARGUMENTS
    template<typename N>
    bool BPLUSTREE_TYPE::Coalesce(
            N *&neighbor_node, N *&node,
            BPlusTreeInternalPage<KeyType, ValueType, KeyComparator> *&parent,
            int index, Transaction *transaction) {
        BPlusTreePage *page = reinterpret_cast<BPlusTreePage*>(node);
        BPlusTreePage *young_sib =
                reinterpret_cast<BPlusTreePage*>(neighbor_node);
        if (!coalesceable(young_sib->GetSize(), page->GetSize(),
                         page->GetMaxSize())){
            return false;
        }

        KeyType separate_key = parent->KeyAt(index);
        if (!page->IsLeafPage()){
            B_PLUS_TREE_INTERNAL_PAGE_TYPE *page_internal =
                    reinterpret_cast<B_PLUS_TREE_INTERNAL_PAGE_TYPE*>(page);
            B_PLUS_TREE_INTERNAL_PAGE_TYPE *sib_internal =
                    reinterpret_cast<B_PLUS_TREE_INTERNAL_PAGE_TYPE*>(young_sib);
            page_internal->MoveAllTo(sib_internal, index,
                                     buffer_pool_manager_);
        }else{
            LEAFPAGE_TYPE *page_leaf =
                    reinterpret_cast<LEAFPAGE_TYPE*>(page);
            LEAFPAGE_TYPE *sib_leaf =
                    reinterpret_cast<LEAFPAGE_TYPE*>(young_sib);
            page_leaf->MoveAllTo(sib_leaf, index,
                                 buffer_pool_manager_);
        }
        return true;
    }

    INDEX_TEMPLATE_ARGUMENTS
    template<typename N>
    bool BPLUSTREE_TYPE::try_redistribute(N *node, Transaction *tran){
        BPlusTreePage *page = reinterpret_cast<BPlusTreePage*>(node);
        page_id_t parent_page_id = page->GetParentPageId();
        B_PLUS_TREE_INTERNAL_PAGE_TYPE *parent = getParentPage
                (parent_page_id, buffer_pool_manager_);
        size_t keyIndex = parent->ValueIndex(page->GetPageId());
        if (keyIndex >= 1){
            // young sibling existing case
            page_id_t young_sib_id = parent->ValueAt(keyIndex - 1);
            Page *young_sib_page=buffer_pool_manager_->FetchPage(young_sib_id);
            BPlusTreePage *sib_page = reinterpret_cast<BPlusTreePage*>
            (young_sib_page->GetData());
            Redistribute(sib_page, page, 1);
            buffer_pool_manager_->UnpinPage(young_sib_id, true);
            buffer_pool_manager_->UnpinPage(parent_page_id, true);
            return true;
        }
        if (keyIndex < static_cast<size_t >(parent->GetSize() - 1)){
            // old sibling existing case
            page_id_t old_sib_id = parent->ValueAt(keyIndex + 1);
            Page *old_sib_page =buffer_pool_manager_->FetchPage(old_sib_id);
            BPlusTreePage *sib_page = reinterpret_cast<BPlusTreePage*>
            (old_sib_page->GetData());
            Redistribute(page, sib_page, 0);
            buffer_pool_manager_->UnpinPage(old_sib_id, true);
            buffer_pool_manager_->UnpinPage(parent_page_id, true);
            return true;
        }
        return false; // the code should be un-reacheable!
    }

    /**
     * Redistribute key & value pairs from one page to its sibling page. If
     * index == 0, move sibling page's first key & value pair into end of
     * input "node", otherwise move sibling page's last key & value pair into
     * head of input "node".
     *
     * Using template N to represent either internal page or leaf page.
     *
     * @tparam KeyType
     * @tparam ValueType
     * @tparam KeyComparator
     * @tparam N
     * @param neighbor_node sibling page of input "node"
     * @param node input from method coalesceOrRedistribute()
     * @param index
     */
    INDEX_TEMPLATE_ARGUMENTS
    template<typename N>
    void BPLUSTREE_TYPE::Redistribute(N *neighbor_node, N *node, int index) {
        BPlusTreePage *sib_node = reinterpret_cast<BPlusTreePage*>
        (neighbor_node);
        BPlusTreePage *page = reinterpret_cast<BPlusTreePage*>(node);
        B_PLUS_TREE_INTERNAL_PAGE_TYPE *parent = getParentPage
                (page->GetParentPageId(), buffer_pool_manager_);
        size_t keyIndex = parent->ValueIndex(page->GetPageId());
        if (!page->IsLeafPage()){
            B_PLUS_TREE_INTERNAL_PAGE_TYPE *sib_page =
                    reinterpret_cast<B_PLUS_TREE_INTERNAL_PAGE_TYPE*>(sib_node);
            B_PLUS_TREE_INTERNAL_PAGE_TYPE *cur_page =
                    reinterpret_cast<B_PLUS_TREE_INTERNAL_PAGE_TYPE*>(page);
            if (index == 0){
                sib_page->MoveFirstToEndOf(cur_page, buffer_pool_manager_);
            }else{
                sib_page->MoveLastToFrontOf(cur_page, keyIndex,
                                            buffer_pool_manager_);
            }
        }else{
            B_PLUS_TREE_LEAF_PAGE_TYPE *sib_page =
                    reinterpret_cast<B_PLUS_TREE_LEAF_PAGE_TYPE*>(sib_node);
            B_PLUS_TREE_LEAF_PAGE_TYPE *cur_page =
                    reinterpret_cast<B_PLUS_TREE_LEAF_PAGE_TYPE*>(page);
            if (index == 0){
                sib_page->MoveFirstToEndOf(cur_page, buffer_pool_manager_);
            }else{
                sib_page->MoveLastToFrontOf(cur_page, keyIndex,
                                            buffer_pool_manager_);
            }
        }
        buffer_pool_manager_->UnpinPage(page->GetParentPageId(), true);
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
        //todo:
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
    INDEXITERATOR_TYPE BPLUSTREE_TYPE::Begin() { return INDEXITERATOR_TYPE(); }

/*
 * Input parameter is low key, find the leaf page that contains the input key
 * first, then construct index iterator
 * @return : index iterator
 */
    INDEX_TEMPLATE_ARGUMENTS
    INDEXITERATOR_TYPE BPLUSTREE_TYPE::Begin(const KeyType &key) {
        return INDEXITERATOR_TYPE();
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
 * Call this method everytime root page id is changed.
 * @parameter: insert_record      defualt value is false. When set to true,
 * insert a record <index_name, root_page_id> into header page instead of
 * updating it.
 */
    INDEX_TEMPLATE_ARGUMENTS
    void BPLUSTREE_TYPE::UpdateRootPageId(int insert_record) {
        HeaderPage *header_page = reinterpret_cast<HeaderPage *>(
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
 * print out whole b+tree sturcture, rank by rank
 */
    INDEX_TEMPLATE_ARGUMENTS
    std::string BPLUSTREE_TYPE::ToString(bool verbose) { return "Empty tree"; }

/*
 * This method is used for test only
 * Read data from file and insert one by one
 */
    INDEX_TEMPLATE_ARGUMENTS
    void BPLUSTREE_TYPE::InsertFromFile(const std::string &file_name,
                                        Transaction *transaction) {
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

/*****************************************************************************
 * helper methods
 * **************************************************************************
 */
    INDEX_TEMPLATE_ARGUMENTS
    B_PLUS_TREE_INTERNAL_PAGE_TYPE* BPLUSTREE_TYPE::getParentPage(
            page_id_t id, BufferPoolManager *buffer_pool_manager) {
        Page *parent_page = buffer_pool_manager->FetchPage(id);
        B_PLUS_TREE_INTERNAL_PAGE_TYPE *parent =
                reinterpret_cast<B_PLUS_TREE_INTERNAL_PAGE_TYPE*>
                (parent_page->GetData());
        return parent;
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
