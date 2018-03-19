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
#include <mutex>
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
    protected:
    private:

        /* section for private struct */
        // struct to hold a node for a key/value pair
        struct Node{
            Node *prev;
            Node *next;
            std::pair<K,V> *p;
            Node(const K &key, const V &value);
        };
        // struct to hold the linked for key/value pairs
        //todo: if possible, make this a template and reuse it
        struct List{
            List();
        public:
            Node *head; // pointer to the first Node
            size_t len; // # of node in the linked list
            size_t arr_size; // max # of key/value pairs allowed
            Node* find(const K &key);
            bool remove(const K &key);
            bool unlink(const K &key);
            bool add(const K &key, const V &value);
            bool comKeys(const K &k1, const K &k2);
            List(size_t array_size);
            ~List();
        };

        // the struct to hold the bucket object
        struct Bucket{
        private:
            Bucket();
        public:
            size_t local_depth; // local # of bits use for selecting index
            List *pairs; // linked list to hold the key/value pairs
            Bucket *next; // pointer to next overflow Bucket
            size_t arr_size; // max # of key/value pairs
            size_t id; // the index # of the bucket
            bool add(const K &key, const V &value);
            bool remove(const K &key);
            bool evict(const K &key);
            std::pair<K,V>* find(const K &key);
            size_t len();
            Node *head();
            void squashBuckets();
            Bucket(size_t l_depth,size_t array_size, size_t index);
            ~Bucket();
        };
        /* section for private variables */
        size_t bucket_num_; // number of buckets in the hash table
        size_t array_size_; // fixed array size for each bucket
        size_t global_depth_; // global # of bits used for selecting index
        std::vector<Bucket*> *buckets; //the vector to hold the array of buckets
        mutable std::mutex write_lock;

        /* section for private methods */
        void Expand(const K &key);
        bool SplitBucket(const K &key);
        size_t RedistKeys(Bucket *b1, Bucket *b2, size_t i);
        void AddOverflowBucket(Bucket *b1, Bucket *b2);
        size_t GetBucketIndex(size_t hash, size_t depth);
        bool FindValue(Bucket *bucket, const K &key, V &value);
        Bucket *FindBucket(const K &key);
        //todo: remove this comparison method here
        bool comKeys(const K &k1, const K &k2);
        int getGlobalDepth() const;
        int getLocalDepth(int bucket_id) const;
        int getNumBuckets() const;
        void insert(const K &key, const V &value);
        Bucket *insertNewBucket(const K &key);
    };
} // namespace cmudb
