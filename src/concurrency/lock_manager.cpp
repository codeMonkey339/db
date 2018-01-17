/**
 * lock_manager.cpp
 */

#include <assert.h>
#include <algorithm>
#include <future>
#include "concurrency/lock_manager.h"

namespace cmudb {

bool LockManager::checkTxn(Transaction *txn) {
  if (txn->GetState() == TransactionState::ABORTED) { return false; }
  if (txn->GetState() == TransactionState::COMMITTED) { return false; }
  if (txn->GetState() == TransactionState::SHRINKING) {
    txn->SetState(TransactionState::ABORTED);
    return false;
  }
  return true;
}
/**
 * request a shared lock on rid.
 * if current txn is aborted, return false.
 * if current txn is shrinking, this violate 2pl. Txn should be set to abort and return false
 * if current txn is committed, return false.
 * if current txn is growing, do the following.
 *
 * check if rid is already locked.
 * if not, mark the rid wih shared lock. return true.
 * if there's already a lock being hold
 *      check if this requirement will block.
 *      if not, i.e. there is a shared lock granted, grant lock and return true
 *      this requirement will block:
 *          do deadlock prevention using wait-die method first.
 *              if current txn should abort, change the txn state and return false
 *          mark current locking request on rid's wait list and block
 *          if waken up, return true.
 *
 *
 * @param txn transaction that requesting shared lock
 * @param rid target id related to txn
 * @return true if granted, false otherwise
 */
bool LockManager::LockShared(Transaction *txn, const RID &rid) {
  if (!checkTxn(txn)) { return false; }

  assert(txn->GetState() == TransactionState::GROWING);
  std::unique_lock<std::mutex> guard(mtx);
  std::shared_ptr<WaitList> ptr;
  auto exists = hash->Find(rid, ptr);

  if (!exists) {
    //create a waitlist and add into hash table
    ptr = std::make_shared<WaitList>(txn->GetTransactionId(),WaitState::SHARED);
    assert(ptr);
    hash->Insert(rid, ptr);
    //grant lock to the txn
    //current txn is the first one asked for lock.
    //so it should have the shared lock
    return true;
  }

  assert(ptr);
  assert(ptr->state == WaitState::SHARED || ptr->state == WaitState::EXCLUSIVE);
  //should block?
  if (ptr->state == WaitState::EXCLUSIVE) {
    //should block
    //if requirement will not result in a deadlock, block until lock granted and return true
    if (txn->GetTransactionId() >= ptr->oldest) {
      txn->SetState(TransactionState::ABORTED);
      return false;
    }
    //append to wait list
    ptr->lst.emplace_back(txn->GetTransactionId(), WaitState::SHARED);
    auto p = ptr->lst.back().p;
    guard.release();
    p->get_future();
    return true;
  }

  assert(ptr->state == WaitState::SHARED);
  ptr->granted.insert(txn->GetTransactionId());
  ptr->oldest = std::max(ptr->oldest, txn->GetTransactionId());
  return true;
}

bool LockManager::LockExclusive(Transaction *txn, const RID &rid) {
  if (!checkTxn(txn)) { return false; }

  assert(txn->GetState() == TransactionState::GROWING);
  std::unique_lock<std::mutex> guard(mtx);
  std::shared_ptr<WaitList> ptr;
  auto exists = hash->Find(rid, ptr);

  if (!exists) {
    //create a waitlist and add into hash table
    ptr = std::make_shared<WaitList>(txn->GetTransactionId(),WaitState::EXCLUSIVE);
    assert(ptr);
    hash->Insert(rid, ptr);
    return true;
  }
  assert(ptr);
  assert(ptr->state == WaitState::SHARED || ptr->state == WaitState::EXCLUSIVE);
  //should block anyway
  //if requirement will not result in a deadlock, block until lock granted and return true
  if (txn->GetTransactionId() >= ptr->oldest) {
    txn->SetState(TransactionState::ABORTED);
    return false;
  }
  //append to wait list
  ptr->lst.emplace_back(txn->GetTransactionId(), WaitState::EXCLUSIVE);
  auto p = ptr->lst.back().p;
  guard.release();
  p->get_future();
  return true;
}

/**
 * upgrade from shared lock to exclusive lock
 * txn should have shared lock first
 * @param txn
 * @param rid
 * @return
 */
bool LockManager::LockUpgrade(Transaction *txn, const RID &rid) {
  if (!checkTxn(txn)) {
    return false;
  }
  assert(txn->GetState() == TransactionState::GROWING);
  std::unique_lock<std::mutex> guard(mtx);
  std::shared_ptr<WaitList> ptr;
  auto exists = hash->Find(rid, ptr);
  if (!exists) {
    return false;
  }
  assert(ptr);
  if (ptr->granted.find(txn->GetTransactionId()) == ptr->granted.end()) {
    return false;
  }

  //if its the only one lock holder, upgrade now.
  assert(Unlock(txn, rid));
  assert(LockExclusive(txn, rid));
  return true;
}

bool LockManager::Unlock(Transaction *txn, const RID &rid) {
  if (!(txn->GetState() == TransactionState::GROWING || txn->GetState() == TransactionState::SHRINKING)) {
    return false;
  }
  std::unique_lock<std::mutex> guard(mtx);
  std::shared_ptr<WaitList> ptr;
  auto exists = hash->Find(rid, ptr);
  if (!exists) {
    assert(false);
    return false;
  }
  assert(ptr);
  assert(ptr->granted.find(txn->GetTransactionId()) != ptr->granted.end());
  ptr->granted.erase(txn->GetTransactionId());
  if (!ptr->granted.empty()) {
    return true;
  }

  if (ptr->lst.empty()) {
    hash->Remove(rid);
    return true;
  }

  ptr->oldest = -1;
  ptr->state = WaitState::INIT;

  ptr->oldest = ptr->lst.front().tid;
  ptr->granted.insert(ptr->oldest);
  ptr->state = ptr->lst.front().targetState;
  ptr->lst.front().p->set_value(true);
  ptr->lst.erase(ptr->lst.begin());
  return true;

//  return false;? I havn't get it.
}

} // namespace cmudb
