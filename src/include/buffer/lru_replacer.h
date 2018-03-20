/**
 * lru_replacer.h
 *
 * Functionality: The buffer pool manager must maintain a LRU list to collect
 * all the pages that are unpinned and ready to be swapped. The simplest way to
 * implement LRU is a FIFO queue, but remember to dequeue or enqueue pages when
 * a page changes from unpinned to pinned, or vice-versa.
 */

#pragma once

#include <deque>
#include <memory>
#include <mutex>
#include <unordered_map>

#include "buffer/replacer.h"
#include "hash/extendible_hash.h"
namespace cmudb {

    template<typename T>
    class LRUReplacer : public Replacer<T> {
    public:
        // do not change public interface
        LRUReplacer();

        ~LRUReplacer();

        void Insert(const T &value);

        bool Victim(T &value);

        bool Erase(const T &value);

        size_t Size();

    private:
        //todo: why deque doesn't have to be newed?
        std::deque<T> elems; // FIFO deque to store elements
        HashTable<T, int> *entries; // record existing elements
        //std::unordered_map<T, int> entries;
        std::mutex lock;

        /* section for private methods */
        bool exists(const T &value);
        void insert(const T &value);
        bool victim(T &value);
        bool erase(const T &value);
        size_t size();
    };

} // namespace cmudb
