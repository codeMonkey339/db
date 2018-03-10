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
        local_bits = DEFAULT_LOCAL_BITS;
        pairs = new std::pair<K, V>();
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
     * lookup function fo find value associated with input key and value
     * @tparam K
     * @tparam V
     * @param key
     * @param value
     * @return
     */
    template<typename K, typename V>
    bool ExtendibleHash<K, V>::Find(const K &key, V &value) {
        size_t hash = HashKey(key);
        size_t bucketIdx = GetIndex(hash, GetGlobalDepth());
        Bucket *bucket = buckets->at(bucketIdx);
        return FindPair(bucket, key, value);
    }

/*
 * delete <key,value> entry in hash table
 * Shrink & Combination is not required for this project
 */
    template<typename K, typename V>
    bool ExtendibleHash<K, V>::Remove(const K &key) {
        return false;
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
    size_t ExtendibleHash::GetIndex(size_t hash, size_t depth) {
        return (hash << (sizeof(size_t) * 8 - depth)) >> depth;
    }

    template<typename K, typename V>
    bool ExtendibleHash::FindPair(Bucket *bucket, const K &key,
                                  V &value) {
        std::pair<K,V> *p = bucket->pairs;
        while (p != NULL){
            //todo: what is the right way to compare in templates?
            // p->first == key && p->second == value
            if (std::memcmp(&p->first, &key, sizeof(K)) == 0){
                if (std::memcmp(&p->second, &value, sizeof(V)) == 0){
                    value = p->second;
                    return true;
                }else{
                    p++;
                }
            }else{
                p++;
            }
        }
        return false;
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
