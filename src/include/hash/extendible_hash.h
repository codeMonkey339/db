/*
 * extendible_hash.h : implementation of in-memory hash table using extendible
 * hashing
 *
 * Functionality: The buffer pool manager must maintain a page table to be able
 * to quickly map a PageId to its corresponding memory location; or alternately
 * report that the PageId does not match any currently-buffered page.
 */

#pragma once

#include <cstdlib>
#include <vector>
#include <string>

#include "hash/hash_table.h"

namespace cmudb {

    template<typename K, typename V>
    class ExtendibleHash : public HashTable<K, V> {
    public:
        // constructor
        ExtendibleHash(size_t size);

        // helper function to generate hash addressing
        size_t HashKey(const K &key);

        // helper function to get global & local depth
        int GetGlobalDepth() const;

        int GetLocalDepth(int bucket_id) const;

        int GetNumBuckets() const;

        // lookup and modifier
        bool Find(const K &key, V &value) override;

        bool Remove(const K &key) override;

        void Insert(const K &key, const V &value) override;

    private:
        // the struct to hold an entry in the bucket
        struct entry{
            //todo: find out the type of key and value
            //this hash table uses a template class --> generic programming?
        };

        // the struct to hold the bucket object
        struct bucket{
            size_t local_bits; // local # of bits use for selecting index
            entry *entries; // pointer to the array of entries in a bucket

        };


        size_t bucket_num_; // number of buckets in the hash table
        size_t array_size_; // fixed array size for each bucket
        size_t global_bits_; // global # of bits used for selecting index
        std::vector<bucket*> *buckets; //the vector to hold the array of buckets
        const int DEFAULT_BUCKET_NUM = 4; // default bucket number
        const int DEFAULT_ENTRY_NUM = 8; // default <k,v> entry in a bucket

    };
} // namespace cmudb
