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
        ~ExtendibleHash();

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
        /* section for private struct */
        // the struct to hold the bucket object
        struct Bucket{
        public:
            size_t len; // # of pairs in this list
            size_t local_depth; // local # of bits use for selecting index
            //todo: implementation is wrong --> linked list instead of array
            std::pair<K, V> *pairs; // pointer to the list of entries in bucket
            Bucket();
            bool add(const K &key, const V &value);
            bool remove(const K &key);
            std::pair<K,V>* find(const K &key);
        };

        /* section for private variables */
        size_t bucket_num_; // number of buckets in the hash table
        size_t array_size_; // fixed array size for each bucket
        size_t global_depth_; // global # of bits used for selecting index
        std::vector<Bucket*> *buckets; //the vector to hold the array of buckets
        static const int DEFAULT_BUCKET_NUM = 8; // default # of buckets
        static const int DEFAULT_LOCAL_BITS = 1; // default # of bits for index

        /* section for private methods */
        void Expand();
        size_t GetBucketIndex(size_t hash, size_t depth);
        bool FindValue(Bucket *bucket, const K &key, V &value);
        Bucket *findBucket(const K &key);

    };
} // namespace cmudb
