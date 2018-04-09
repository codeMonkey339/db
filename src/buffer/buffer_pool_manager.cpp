#include <include/common/logger.h>
#include "buffer/buffer_pool_manager.h"

namespace cmudb {

/*
 * BufferPoolManager Constructor
 * When log_manager is nullptr, logging is disabled (for test purpose)
 * WARNING: Do Not Edit This Function
 */
    BufferPoolManager::BufferPoolManager(size_t pool_size,
                                         DiskManager *disk_manager,
                                         LogManager *log_manager)
            : pool_size_(pool_size), disk_manager_(disk_manager),
              log_manager_(log_manager) {
        // a consecutive memory space for buffer pool
        pages_ = new Page[pool_size_];
        page_table_ = new ExtendibleHash<page_id_t, Page *>(BUCKET_SIZE);
        replacer_ = new LRUReplacer<Page *>;
        free_list_ = new std::list<Page *>;
        memset(pages_, 0, pool_size_ * sizeof(Page));
        // put all the pages into free list
        for (size_t i = 0; i < pool_size_; ++i) {
            free_list_->push_back(&pages_[i]);
        }
    }

/*
 * BufferPoolManager Deconstructor
 * WARNING: Do Not Edit This Function
 */
    BufferPoolManager::~BufferPoolManager() {
        delete[] pages_;
        delete page_table_;
        delete replacer_;
        delete free_list_;
    }

    /**
     * 1. search hash table.
     *  1.1 if exist, pin the page and return immediately
     *  1.2 if no exist, find a replacement entry from either free list or lru
     *      replacer. (NOTE: always find from free list first)
     * 2. If the entry chosen for replacement is dirty, write it back to disk.
     * 3. Delete the entry for the old page from the hash table and insert an
     * entry for the new page.
     * 4. Update page metadata, read page content from disk file and return page
     * pointer
     *
     * @param page_id
     * @return
     */
    Page *BufferPoolManager::FetchPage(page_id_t page_id) {
        Page *page;
        if (page_id == INVALID_PAGE_ID){
            return nullptr;
        }
        if (page_table_->Find(page_id, page)){
            if (page->pin_count_ == 0){
                // pin_count is 0, still in use, in able-to-evict list
                replacer_->Erase(page);
            }
            pin(page);
            return page;
        }else{
            if (!free_list_->empty()){
                page = free_list_->front();
                free_list_->pop_front();
                assert(page->pin_count_ == 0);
                assert(page->page_id_ == INVALID_PAGE_ID);
                assert(!page->is_dirty_);
                pin(page);
            }else{
                if (replacer_->Victim(page)){
                    if (page->is_dirty_){
                        disk_manager_->WritePage(page->page_id_, page->data_);
                        page->is_dirty_ = false;
                    }
                    assert(page->pin_count_ == 0);
                    assert(!page->is_dirty_);
                    page_table_->Remove(page->page_id_);
                    pin(page);
                }else{ // no page to evict
                    LOG_INFO("unable to evict a page for FetchPage\nL");
                    return nullptr;
                }
            }
            page_table_->Insert(page_id, page);
            disk_manager_->ReadPage(page_id, page->data_);
            page->page_id_ = page_id;
            return page;
        }
    }

    /**
     * Implementation of unpin page
     * if pin_count>0, decrement it and if it becomes zero, put it back to
     * replacer if pin_count<=0 before this call, return false. is_dirty: set
     * the dirty flag of this page
     * @param page_id
     * @param is_dirty
     * @return
     */
    bool BufferPoolManager::UnpinPage(page_id_t page_id, bool is_dirty) {
        Page *page;
        if (!page_table_->Find(page_id, page)){
            LOG_INFO("unpin a page not in use\n");
            return false;
        }else{
            if (page->pin_count_ <= 0){
                LOG_INFO("unpin a page with a count <= 0\n");
                return false;
            }else{
                unpin(page);
                page->is_dirty_ = is_dirty || page->is_dirty_;
                if (page->pin_count_ == 0){
                    replacer_->Insert(page);
                }
                return true;
            }
        }
    }

    /**
     * Used to flush a particular page of the buffer pool to disk. Should call the
     * write_page method of the disk manager
     * if page is not found in page table, return false
     * NOTE: make sure page_id != INVALID_PAGE_ID
     * @param page_id
     * @return
     */
    bool BufferPoolManager::FlushPage(page_id_t page_id) {
        Page *page;
        if (page_id == INVALID_PAGE_ID){
            return false;
        }
        if (!page_table_->Find(page_id, page)){
            return false;
        }else{
            disk_manager_->WritePage(page_id, page->data_);
            return true;
        }
    }

    /**
     * User should call this method for deleting a page. This routine will call
     * disk manager to deallocate the page. First, if page is found within page
     * table, buffer pool manager should be reponsible for removing this
     * entry out of page table, reseting page metadata and adding back to
     * free list. Second, call disk manager's DeallocatePage() method to
     * delete from disk file. If the page is found within page table, but
     * pin_count != 0, return false
     * @param page_id
     * @return
     */
    bool BufferPoolManager::DeletePage(page_id_t page_id) {
        Page *page;
        if (page_id == INVALID_PAGE_ID){
            return false;
        }

        if (!page_table_->Find(page_id, page)){
            return false;
        }else{
            assert(page->page_id_ == 0);
            if (page->pin_count_ != 0){
                return false;
            }else{
                page->page_id_ = INVALID_PAGE_ID;
                disk_manager_->DeallocatePage(page_id);
                page_table_->Remove(page_id);
                assert(!page->is_dirty_);
                assert(page->pin_count_ == 0);
                assert(page->page_id_ == INVALID_PAGE_ID);
                free_list_->push_back(page);
                return true;
            }
        }
    }

    /**
     * User should call this method if needs to create a new page. This routine
     * will call disk manager to allocate a page.
     * Buffer pool manager should be responsible to choose a victim page either
     * from free list or lru replacer(NOTE: always choose from free list first),
     * update new page's metadata, zero out memory and add corresponding entry
     * into page table. return nullptr if all the pages in pool are pinned

     * @param page_id
     * @return
     */
    Page *BufferPoolManager::NewPage(page_id_t &page_id) {
        Page *page;
        if (!free_list_->empty()){
            page = free_list_->front();
            free_list_->pop_front();
            assert(!page->is_dirty_);
            assert(page->pin_count_ == 0);
            memset(page->data_, 0, sizeof(page->data_) / sizeof
                                                         (page->data_[0]));
        }else{
            if (!replacer_->Victim(page)){
                return nullptr;
            }else{
                if (page->is_dirty_){
                    disk_manager_->WritePage(page->page_id_, page->data_);
                }
                assert(page->pin_count_ == 0);
                page->is_dirty_ = false;
                page_table_->Remove(page->page_id_);
            }
        }
        page_id = disk_manager_->AllocatePage();
        pin(page);
        page->page_id_ = page_id;
        page_table_->Insert(page_id, page);
        return page;
    }


    void BufferPoolManager::pin(Page *p) {
        p->pin_count_++;
    }

    void BufferPoolManager::unpin(Page *p) {
        p->pin_count_--;
    }
} // namespace cmudb
