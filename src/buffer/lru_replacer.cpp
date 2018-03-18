/**
 * LRU implementation
 */
#include <queue>
#include "buffer/lru_replacer.h"
#include "page/page.h"

namespace cmudb {

    template<typename T>
    LRUReplacer<T>::LRUReplacer() {}

    template<typename T>
    LRUReplacer<T>::~LRUReplacer() {}

/**
 * Insert into LRU. When duplicate keys are inserted, then the element should
 * be removed and then inserted again.
 * @tparam T
 * @param value
 */
    template<typename T>
    void LRUReplacer<T>::Insert(const T &value) {
        std::lock_guard<std::mutex> lg(lock);
        insert(value);
    }

    template<typename T>
    void LRUReplacer<T>::insert(const T &value){
        if (exists(value)){
            erase(value);
        }
        elems.push_back(value);
    }

    /**
     * if LRU is non-empty, pop the head member from LRU to argument "value",
     * and return true. If LRU is empty, return false
     * @tparam T
     * @param value
     * @return
     */
    template<typename T>
    bool LRUReplacer<T>::Victim(T &value) {
        std::lock_guard<std::mutex> lg(lock);
        return victim(value);
    }

    template<typename T>
    bool LRUReplacer<T>::victim(T &value) {
        if (!exists(value)){
            return false;
        }else{
            value = elems.front();
            elems.pop_front();
            return true;
        }
    }

    /**
     * Remove value from LRU. If removal is successful, return true,
     * otherwise return false
     * @tparam T
     * @param value
     * @return
     */
    template<typename T>
    bool LRUReplacer<T>::Erase(const T &value) {
        std::lock_guard<std::mutex> lg(lock);
        return erase(value);
    }

    template<typename T>
    bool LRUReplacer<T>::erase(const T &value){
        if (!exists(value)){
            return false;
        }else{
            std::deque<T> backup;
            while(elems.size() > 0){
                T ele = elems.front();
                elems.pop_front();
                backup.push_back(ele);
                if (memcmp(&ele, &value, sizeof(T)) == 0){
                    backup.pop_back();
                    break;
                }
            }
            while(backup.size() > 0){
                T ele = backup.back();
                backup.pop_back();
                elems.push_front(ele);
            }
            return true;
        }
    }

    template<typename T>
    size_t LRUReplacer<T>::Size() {
        std::lock_guard<std::mutex> lg(lock);
        return elems.size();
    }

    template
    class LRUReplacer<Page *>;


    /**
     * check whether a value exists in the LRUReplacer
     * @tparam T
     * @param value
     * @return
     */
    template<typename T>
    bool LRUReplacer<T>::exists(const T &value){
        typename std::deque<T>::iterator itr = elems.begin();
        while (itr != elems.end()){
            T val = *itr;
            if (memcpy(&val, &value, sizeof(T)) == 0){
                return true;
            }else{
                itr++;
            }
        }
        return false;
    }

// test only
    template
    class LRUReplacer<int>;

} // namespace cmudb
