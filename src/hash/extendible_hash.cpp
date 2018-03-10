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
        bucket_num_ = BUCKET_SIZE;
        array_size_ = DEFAULT_ENTRY_NUM;
        global_bits_ = 1;
        buckets = new std::vector<Bucket *>(bucket_num_);
        for (size_t i = 0; i < bucket_num_; i++){
            buckets->push_back(new Bucket());
        }
    }

    /**
     * destructor
     * @tparam K
     * @tparam V
     */
    template<typename K, typename V>
    ExtendibleHash<K,V>::~ExtendibleHash() {
        for (std::vector<Bucket*>::iterator it = buckets->begin(); it !=
                buckets->end(); it++)
            delete(*it);
        buckets->clear();
    }

    /**
     * constructor
     * @tparam K
     * @tparam V
     */
    template<typename K, typename V>
    ExtendibleHash<K, V>::Bucket::Bucket() {
        local_bits = DEFAULT_LOCAL_BITS;
        pairs = new std::pair<K,V>();
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
        return 0;
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
        return 0;
    }

/*
 * lookup function to find value associate with input key
 */
    template<typename K, typename V>
    bool ExtendibleHash<K, V>::Find(const K &key, V &value) {
        return false;
    }

/*
 * delete <key,value> entry in hash table
 * Shrink & Combination is not required for this project
 */
    template<typename K, typename V>
    bool ExtendibleHash<K, V>::Remove(const K &key) {
        return false;
    }

/*
 * insert <key,value> entry in hash table
 * Split & Redistribute bucket when there is overflow and if necessary increase
 * global depth
 */
    template<typename K, typename V>
    void ExtendibleHash<K, V>::Insert(const K &key, const V &value) {}


    /**
     * helper method to expand the table on overflow
     * @return
     */
    template<typename K, typename V>
    void ExtendibleHash<K,V>::expand() {
    };

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
