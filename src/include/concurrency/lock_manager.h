/**
 * lock_manager.h
 *
 * Tuple level lock manager, use wait-die to prevent deadlocks
 */

#pragma once

#include <condition_variable>
#include <list>
#include <memory>
#include <mutex>
#include <unordered_map>
#include <set>
#include <future>

#include "common/rid.h"
#include "concurrency/transaction.h"

namespace cmudb {
enum class WaitState { INIT, SHARED, EXCLUSIVE };

class WaitList {
 public:
  WaitList(const WaitState &) = delete;
  WaitList &operator=(const WaitList &) = delete;
  WaitList(txn_id_t id, WaitState target) : oldest(id), state(target) {
    granted.insert(id);
  }
  WaitList() {}
  txn_id_t oldest = -1; //oldest txn that holds the lock
  WaitState state = WaitState::INIT;
  std::set<txn_id_t> granted;

  class WaitItem {
   public:
    WaitItem(const WaitItem &) = delete;
    WaitItem &operator=(const WaitItem &) = delete;
    txn_id_t tid;
    std::shared_ptr<std::promise<bool> > p = std::make_shared<std::promise<bool>>();
    WaitState targetState;
    WaitItem(txn_id_t id, WaitState ws) : tid(id), targetState(ws) {}
  };
  std::list<WaitItem> lst;
};

class LockManager {

 public:
  LockManager(bool strict_2PL);
  /*** below are APIs need to implement ***/
  // lock:
  // return false if transaction is aborted
  // it should be blocked on waiting and should return true when granted
  // note the behavior of trying to lock locked rids by same txn is undefined
  // it is transaction's job to keep track of its current locks
  bool LockShared(Transaction *txn, const RID &rid);
  bool LockExclusive(Transaction *txn, const RID &rid);
  bool LockUpgrade(Transaction *txn, const RID &rid);

  // unlock:
  // release the lock hold by the txn
  bool Unlock(Transaction *txn, const RID &rid);
  /*** END OF APIs ***/

 private:

  bool strict_2PL_;
  std::mutex mtx;//sync method calling on hash table
  std::unordered_map<RID, std::shared_ptr<WaitList>> hash;
  bool isValidToAcquireLock(Transaction *txn);
};

} // namespace cmudb
