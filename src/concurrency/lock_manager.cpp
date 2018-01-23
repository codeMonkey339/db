/**
 * lock_manager.cpp
 */

#include <assert.h>
#include <algorithm>
#include <future>
#include "concurrency/lock_manager.h"

namespace cmudb {

LockManager::LockManager(bool strict_2PL) : strict_2PL_(strict_2PL) {
};

bool LockManager::isValidToAcquireLock(Transaction *txn) {
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
  if (!isValidToAcquireLock(txn)) { return false; }
  assert(txn->GetState() == TransactionState::GROWING);

  std::unique_lock<std::mutex> guard(mtx);

  if (hash.find(rid) == hash.end()) {
    //create a waitlist and add into hash table
    auto ptr = std::make_shared<WaitList>(txn->GetTransactionId(), WaitState::SHARED);
    assert(ptr);

    hash[rid] = ptr;
    //grant lock to the txn
    //current txn is the first one asked for lock.
    //so it should have the shared lock
    txn->InsertIntoSharedLockSet(rid);
    return true;
  }

  auto ptr = hash[rid];
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
    txn->InsertIntoSharedLockSet(rid);
    return true;
  }

  assert(ptr->state == WaitState::SHARED);
  ptr->granted.insert(txn->GetTransactionId());
  ptr->oldest = std::max(ptr->oldest, txn->GetTransactionId());
  txn->InsertIntoSharedLockSet(rid);
  return true;
}

bool LockManager::LockExclusive(Transaction *txn, const RID &rid) {
  if (!isValidToAcquireLock(txn)) { return false; }

  assert(txn->GetState() == TransactionState::GROWING);
  std::unique_lock<std::mutex> guard(mtx);
  if (hash.find(rid) == hash.end()) {
    //create a waitlist and add into hash table
    auto ptr = std::make_shared<WaitList>(txn->GetTransactionId(), WaitState::EXCLUSIVE);
    assert(ptr);
    hash[rid] = ptr;
    txn->InsertIntoExclusiveLockSet(rid);
    return true;
  }
  auto ptr = hash[rid];
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
  txn->InsertIntoExclusiveLockSet(rid);
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
  if (!isValidToAcquireLock(txn)) {
    return false;
  }
  assert(txn->GetState() == TransactionState::GROWING);
  std::unique_lock<std::mutex> guard(mtx);

  if (hash.find(rid) == hash.end()) {
    return false;
  }
  auto ptr = hash[rid];
  assert(ptr);
  if (ptr->granted.find(txn->GetTransactionId()) == ptr->granted.end()) {
    return false;
  }

  guard.release();
  auto rUnlock = Unlock(txn, rid);
  assert(rUnlock);
  auto rLockEx = LockExclusive(txn, rid);
  assert(rLockEx);
  return true;
}

/**
 * I think the difference is:
 * strict 2pl will return true if txn is in abort state or commit state
 * 2pl will do so it txn is in growing(change to shrinking), shrinking, abort or commit state.
 * @param txn
 * @param rid
 * @return
 */
bool LockManager::Unlock(Transaction *txn, const RID &rid) {
  if(strict_2PL_){
    if (!(txn->GetState() == TransactionState::COMMITTED || txn->GetState() == TransactionState::ABORTED)) {
      return false;
    }
  }

  std::unique_lock<std::mutex> guard(mtx);
  if (hash.find(rid) == hash.end()) {
    assert(false);
    return false;
  }
  auto ptr = hash[rid];
  assert(ptr);
  assert(ptr->granted.find(txn->GetTransactionId()) != ptr->granted.end());
  ptr->granted.erase(txn->GetTransactionId());
  if(ptr->state == WaitState::EXCLUSIVE){
    assert(txn->GetExclusiveLockSet()->erase(rid) == 1);
  }else{
    assert(txn->GetSharedLockSet()->erase(rid) == 1);
  }

  if(strict_2PL_ == false &&  txn->GetState() == TransactionState::GROWING){
    txn->SetState(TransactionState::SHRINKING);
  }

  if (!ptr->granted.empty()) {
    return true;
  }

  if (ptr->lst.empty()) {
    hash.erase(rid);
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
}

} // namespace cmudb
