#include <cmath>
#include <list>
#include <vector>

#include "hash/extendible_hash.h"
#include "page/page.h"

namespace cmudb {

/*
 * constructor
 * array_size: fixed array size for each bucket
 */
    template<typename K, typename V>
    ExtendibleHash<K, V>::ExtendibleHash(size_t size) {
        bucket_num_ = DEFAULT_BUCKET_NUM;
        array_size_ = BUCKET_SIZE;
        global_depth_ = std::log2(DEFAULT_BUCKET_NUM);
        buckets = new std::vector<Bucket *>(bucket_num_);
        for (size_t i = 0; i < bucket_num_; i++) {
            buckets->push_back(new Bucket());
        }
    }

    /**
     * destructor
     * @tparam K
     * @tparam V
     */
    template<typename K, typename V>
    ExtendibleHash<K, V>::~ExtendibleHash() {
        for (std::vector<Bucket *>::iterator it = buckets->begin(); it !=
                buckets->end(); it++)
            delete (*it);
        buckets->clear();
        delete(buckets);
    }

    /**
     * constructor
     * @tparam K
     * @tparam V
     */
    template<typename K, typename V>
    ExtendibleHash<K, V>::Bucket::Bucket() {
        local_depth = DEFAULT_LOCAL_BITS;
        pairs = new std::pair<K, V>();
        len = 0;
    }

/*
 * helper function to calculate the hashing address of input key
 */
    template<typename K, typename V>
    size_t ExtendibleHash<K, V>::HashKey(const K &key) {
        return 0;
    }

/*
 * helper function to return global depth of hash table
 * NOTE: you must implement this function in order to pass test
 */
    template<typename K, typename V>
    int ExtendibleHash<K, V>::GetGlobalDepth() const {
        return global_depth_;
    }

/*
 * helper function to return local depth of one specific bucket
 * NOTE: you must implement this function in order to pass test
 */
    template<typename K, typename V>
    int ExtendibleHash<K, V>::GetLocalDepth(int bucket_id) const {
        return 0;
    }

/*
 * helper function to return current number of bucket in hash table
 */
    template<typename K, typename V>
    int ExtendibleHash<K, V>::GetNumBuckets() const {
        return bucket_num_;
    }

    /**
     * lookup function fo find value associated with unqiue key/value pairs
     * @tparam K
     * @tparam V
     * @param key
     * @param value
     * @return
     */
    template<typename K, typename V>
    bool ExtendibleHash<K, V>::Find(const K &key, V &value) {
        Bucket *bucket = findBucket(key);
        return FindValue(bucket, key, value);
    }

    /**
     * remove a key/value pair with key as Key
     * @tparam K
     * @tparam V
     * @param key
     * @return return true on success, and false on non-existent key
     */
    template<typename K, typename V>
    bool ExtendibleHash<K, V>::Remove(const K &key) {
        Bucket *bucket = findBucket(key);
        bool removed = bucket->remove(key);
        if (removed){
            return true;
        }else{
            return false;
        }
    }

    /**
     * insert <key,value> entry in hash table
     * Split & Redistribute bucket when there is overflow and if necessary
     * increase lobal depth
     * @tparam K
     * @tparam V
     * @param key
     * @param value
     */
    template<typename K, typename V>
    void ExtendibleHash<K, V>::Insert(const K &key, const V &value) {
    }

    // private methods
    /**
     * helper method to expand the table on overflow
     * @return
     */
    template<typename K, typename V>
    void ExtendibleHash<K, V>::Expand() {
    };

    /**
     * find the bucket index given hash and the local depth
     * @param hash
     * @param depth
     * @return
     */
    size_t ExtendibleHash::GetBucketIndex(size_t hash, size_t depth) {
        return (hash << (sizeof(size_t) * 8 - depth)) >> depth;
    }

    template<typename K, typename V>
    bool ExtendibleHash::FindValue(Bucket *bucket, const K &key, V &value) {
        std::pair<K,V> *p = bucket->find(key);
        if (p != NULL){
            value = p->second;
            return true;
        }else{
            return false;
        }
    }

    /**
     * given a key, find the bucket that would containt the key/value pair
     * @tparam K
     * @tparam V
     * @param key
     * @return pointer to the Bucket
     */
    template<typename K, typename V>
    Bucket* ExtendibleHash::findBucket(const K &key) {
        //todo: need to consider global and local bits to find the right one
        size_t hash = HashKey(key);
        size_t bucketIdx = GetBucketIndex(hash, GetGlobalDepth());
        Bucket *bucket = buckets->at(bucketIdx);
        return bucket;
    }

    /**
     * add a new pair of key/value pair in the Bucket.
     * @tparam K
     * @tparam V
     * @param key
     * @param value
     * @return return true on success, and false on Bucket splitting
     */
    template<typename K, typename V>
    bool ExtendibleHash::Bucket::add(const K &key, const V &value){
        //todo: need to implement pair add function
        return false;
    };

    /**
     * remove a pair of key/value pair in the bucket
     * @tparam K
     * @tparam V
     * @param key
     * @return return true on success, and false on non-existent key
     */
    template<typename K, typename V>
    bool ExtendibleHash::Bucket::remove(const K &key) {
        //todo: need to implement pair remove function
        return false;
    }

    /**
     * check whether a key/value with K as key exists in the linked list
     * @tparam K
     * @tparam V
     * @param key
     * @return
     */
    template<typename K, typename V>
    std::pair<K,V>* ExtendibleHash::Bucket::find(const K &key) {
        std::pair<K,V> *p = pairs;
        while (p != NULL){
            //todo: what is the right way to compare in templates?
            if (std::memcmp(&p->first, &key, sizeof(K)) == 0){
                return p;
            }else{
                p++;
            }
        }
        return NULL;
    }

    template
    class ExtendibleHash<page_id_t, Page *>;

    template
    class ExtendibleHash<Page *, std::list<Page *>::iterator>;

    //todo: find out the meaning of such usage?
// test purpose
    template
    class ExtendibleHash<int, std::string>;

    template
    class ExtendibleHash<int, std::list<int>::iterator>;

    template
    class ExtendibleHash<int, int>;
} // namespace cmudb
