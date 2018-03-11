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
        array_size_ = size;
        global_depth_ = std::log2(DEFAULT_BUCKET_NUM);
        buckets = new std::vector<Bucket *>();
        bucket_ptr = 0;
        for (size_t i = 0; i < bucket_num_; i++) {
            buckets->push_back(new Bucket(0, array_size_));
        }
    }

    /**
     * destructor
     * @tparam K
     * @tparam V
     */
    template<typename K, typename V>
    ExtendibleHash<K, V>::~ExtendibleHash() {
        delete(buckets);
    }

    /**
     * constructor of Bucket
     * @tparam K
     * @tparam V
     */
    template<typename K, typename V>
    ExtendibleHash<K, V>::Bucket::Bucket(size_t l_depth, size_t array_size) {
        local_depth = l_depth;
        arr_size = array_size;
        pairs = new List(arr_size);
    }


    /**
     * destructor of Bucket
     * @tparam K
     * @tparam V
     */
    template<typename K, typename V>
    ExtendibleHash<K,V>::Bucket::~Bucket() {
        delete(pairs);
    };

    template<typename K, typename V>
    ExtendibleHash<K,V>::List::~List(){
        while (head != NULL){
            Node *next = head->next;
            delete(head);
            head = next;
        }
    };

    /**
     * constructor for Linked List
     * @tparam K
     * @tparam V
     */
    template<typename K, typename V>
    ExtendibleHash<K,V>::List::List(size_t array_size){
        len = 0;
        head = NULL;
        arr_size = array_size;
    };

    template<typename K, typename V>
    ExtendibleHash<K,V>::Node::Node(const K &key, const V &value) {
        prev = NULL;
        next = NULL;
        p = new std::pair<K,V>(key, value);
    }

/*
 * helper function to calculate the hashing address of input key
 */
    template<typename K, typename V>
    size_t ExtendibleHash<K, V>::HashKey(const K &key) {
        return std::hash<K>{}(key);
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
        Bucket *b = buckets->at(bucket_id);
        return b->local_depth;
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
        Bucket *bucket = FindBucket(key);
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
        Bucket *bucket = FindBucket(key);
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
        Bucket *b = FindBucket(key);
        bool added = b->add(key, value);
        if (added){
            return;
        }else{
            if (!SplitBucket(key)){
                Expand(key);
            }
            Insert(key, value);
        }
    }

    // private methods
    template<typename K,typename V>
    bool ExtendibleHash<K,V>::SplitBucket(const K &key){
        Bucket *b = FindBucket(key);
        size_t bucket_id = GetBucketIndex(HashKey(key), GetGlobalDepth());
        if (b->local_depth == (size_t)GetGlobalDepth()){
            return false;
        }else{
            b->local_depth++;
            Bucket *next = new Bucket(b->local_depth, array_size_);
            if(RedistKeys(b, next, bucket_id) > 0){
                buckets->at(bucket_id + 1) = next;
            }else{
                b->local_depth--;
                next->local_depth--;
                AddOverflowBucket(b, next);
            }
            return true;
        }
    };

    /**
     * helper method to expand the table on overflow
     * @return
     */
    template<typename K, typename V>
    void ExtendibleHash<K, V>::Expand(const K &key) {
        size_t bucket_idx = GetBucketIndex(HashKey(key), GetGlobalDepth());
        global_depth_++;
        std::vector<Bucket*> *new_b = new std::vector<Bucket*>(2 * bucket_num_);
        for (size_t i = 0; i < bucket_num_; i++){
            if (i == bucket_idx){
                Bucket *b = buckets->at(i);
                b->local_depth++;
                Bucket *next = new Bucket(b->local_depth, array_size_);
                RedistKeys(b, next, bucket_idx);
                new_b->push_back(b);
                new_b->push_back(next);
                if (RedistKeys(b, next, bucket_idx) == 0){
                    b->local_depth--;
                    next->local_depth--;
                }
            }else{
                new_b->push_back(buckets->at(i));
                new_b->push_back(buckets->at(i));
            }
        }
        delete(buckets);
        buckets = new_b;
    };

    /**
     * find the bucket index given hash and the local depth
     * @param hash
     * @param depth
     * @return
     */
    template<typename K, typename V>
    size_t ExtendibleHash<K,V>::GetBucketIndex(size_t hash, size_t depth) {
        return (hash >> (sizeof(size_t) * 8 - depth));
    }

    template<typename K, typename V>
    bool ExtendibleHash<K,V>::FindValue(Bucket *bucket, const K &key, V &value) {
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
    typename ExtendibleHash<K,V>::Bucket* ExtendibleHash<K,V>::FindBucket
            (const K
                                                                  &key) {
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
    bool ExtendibleHash<K,V>::Bucket::add(const K &key, const V &value){
        Bucket *cur = this;
        while(cur != NULL){
            bool added = cur->pairs->add(key, value);
            if (added){
                return true;
            }else{
                cur = cur->next;
            }
        }
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
    bool ExtendibleHash<K,V>::Bucket::remove(const K &key) {
        return pairs->remove(key);
    }

    /**
     * check whether a key/value with K as key exists in the linked list
     * @tparam K
     * @tparam V
     * @param key
     * @return
     */
    template<typename K, typename V>
    std::pair<K,V>* ExtendibleHash<K,V>::Bucket::find(const K &key) {
        Node *n = pairs->find(key);
        if (n != NULL){
            return n->p;
        }else{
            return NULL;
        }
    }

    /**
     * return the head Node of the pair linked list
     * @tparam K
     * @tparam V
     * @return
     */
    template<typename K, typename V>
    typename ExtendibleHash<K,V>::Node* ExtendibleHash<K,V>::Bucket::head() {
        return pairs->head;
    }

    /**
     * redistribute key/value pairs between 2 neighboring buckets
     * @tparam K
     * @tparam V
     * @param b1
     * @param b2
     * @param i
     * @return the # of key/value pairs have been re-distributed
     */
    template<typename K, typename V>
    size_t ExtendibleHash<K,V>::RedistKeys(Bucket *b1, Bucket *b2, size_t i) {
        size_t nMoved = 0;
        Bucket *b1_o = b1, *b2_o = b2;
        while (b1 != NULL){
            Node *head = b1->head();
            while (head != NULL){
                size_t idx = GetBucketIndex(HashKey(head->p->first),
                                            GetGlobalDepth());
                if (idx != i){
                    if (!b2->add(head->p->first, head->p->second)){
                        Bucket *next = new Bucket(b2->local_depth,b2->arr_size);
                        AddOverflowBucket(b2, next);
                        b2 = next;
                        b2->add(head->p->first, head->p->second);
                    };
                    nMoved++;
                }
                head = head->next;
            }
            Node *n_head = b2->head();
            while (n_head != NULL){
                b1->remove(n_head->p->first);
                n_head = n_head->next;
            }
            b1 = b1->next;
        }
        b1_o->squashBuckets();
        b2_o->squashBuckets();
        return nMoved;
    }


    template<typename K, typename V>
    void ExtendibleHash<K,V>::Bucket::squashBuckets() {
        //todo: need to squash key/value pairs into different pages

    }

    template<typename K, typename V>
    void ExtendibleHash<K,V>::AddOverflowBucket(Bucket *b1, Bucket *b2) {
        b1->next = b2;
        return;
    }

    template<typename K, typename V>
    size_t ExtendibleHash<K,V>::Bucket::len() {
        return pairs->len;
    }

    template<typename K, typename V>
    typename ExtendibleHash<K,V>::Node* ExtendibleHash<K,V>::List::find(const K
                                                                  &key) {
        if (head == NULL){
            return NULL;
        }
        Node *n = head;
        while (n != NULL){
            if (comKeys(n->p->first, key)){
                return n;
            }
        }
        return NULL;
    }

    template<typename K, typename V>
    bool ExtendibleHash<K,V>::List::remove(const K &key){
        if (head == NULL){
            return false;
        }
        Node *n = head;
        while(n != NULL){
            if (comKeys(n->p->first, key)){
                len--;
                if (n == head){
                    delete(head);
                    head = n;
                    return true;
                }else{
                    Node *prev = n->prev;
                    prev->next = n->next;
                    n->next->prev = prev;
                    delete(n);
                    return true;
                }
            }else{
                n = n->next;
            }
        }
        return false;
    };

    template<typename K, typename V>
    bool ExtendibleHash<K,V>::List::add(const K &key, const V &value) {
        if (len == arr_size){
            return false;
        }
        Node *n = head;
        if (n == NULL){
            head = new Node(key, value);
        }else{
            while (n->next != NULL){
                n = n->next;
            }
            Node *newNode = new Node(key, value);
            n->next = newNode;
            newNode->prev = n;
        }
        return true;
    }


    /**
     * compare whether two template keys are equal
     * @tparam K
     * @tparam V
     * @param k1
     * @param k2
     * @return
     */
    template<typename K, typename V>
    bool ExtendibleHash<K,V>::comKeys(const K &k1, const K &k2){
        //todo: need to find the right way to compare keys
        return std::memcmp(&k1, &k2, sizeof(K)) == 0;
    };

    template<typename K, typename V>
    bool ExtendibleHash<K,V>::List::comKeys(const K &k1, const K &k2){
        //todo: need to find the right way to compare keys
        return std::memcmp(&k1, &k2, sizeof(K)) == 0;
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
