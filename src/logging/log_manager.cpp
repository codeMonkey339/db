/**
 * log_manager.cpp
 */

#include "logging/log_manager.h"
#include <unistd.h>

namespace cmudb {
/*
 * set ENABLE_LOGGING = true
 * Start a separate thread to execute flush to disk operation periodically
 * The flush can be triggered when the log buffer is full or buffer pool
 * manager wants to force flush (it only happens when the flushed page has a
 * larger LSN than persistent LSN)
 */
void LogManager::RunFlushThread() {
  std::lock_guard<std::mutex> guard(latch_);
  if (flush_thread_on == false) {
    ENABLE_LOGGING = true;
    flush_thread_on = true;
    flush_thread_ = new std::thread(&LogManager::bgFsync, this);
  }
}

/**
 * case 1. : periodical wake up. write out flush_buffer and swap flush_buffer with log_buffer
 *           (I mean swap ptr and idx by swap ptrs)
 * case 2. : be waken up on 1)log_buffer is full 2)something in log_buffer need to be flushed
 *              1) empty flush_buffer first and swap ptrs, it's done
 *              2) empty flush_buffer and swap ptrs and write flush_buffers out, return after
 *              these <- this is triggered in another function 'flushnowblocking'
 * wake-up is implemented by a condition variable.
 * <del>need more info on which scenario is going on with case 2. i.e. to flush both buffers.</del>
 * <del>seems that a single bool value is suffice.</del>
 * <del>marked means flushing both, not marked means no.</del>
 * using another cond to info completion (named : flushed)
 *
 * that is :
 * while bg thread running:
 *      wait for time out or blocking
 *      if time out or waken up:
 *          1.write flush_buffer
 *          2.swap pointers
 *          3.signal completion
 *
 *
 * check flush_buffer_size to deal with superfluous wake up
 */
void LogManager::bgFsync() {
  while (flush_thread_on) {
    {
      std::unique_lock<std::mutex> lock(latch_);
      while (flush_buffer_size_ == 0) {
        auto ret = cv_.wait_for(lock, std::chrono::seconds(LOG_TIMEOUT));
        if (ret == std::cv_status::no_timeout) {
          //required for force flushing
          break;
        }
      }
    }
    disk_manager_->WriteLog(flush_buffer_, flush_buffer_size_);
    std::unique_lock<std::mutex> lock(latch_);
    SwapBuffer();
    flushed.notify_all();
  }
}
/*
 * Stop and join the flush thread, set ENABLE_LOGGING = false
 */
void LogManager::StopFlushThread() {
  std::lock_guard<std::mutex> guard(latch_);
  if (flush_thread_on == true) {
    flush_thread_on = false;
    ENABLE_LOGGING = false;
    {
      //wake up working thread, or it may take a long time waiting before it's been joined
      cv_.notify_all();
    }
    assert(flush_thread_->joinable());
    flush_thread_->join();
    delete flush_thread_;
  }
}

void LogManager::FlushNowBlocking() {
  //write out current flush buffer
  {
    GetBgTaskToWork();
    WaitUntilBgTaskFinish();
  }
  //write out flush buffer which is previous log buffer
  {
    GetBgTaskToWork();
    WaitUntilBgTaskFinish();
  }
}

void LogManager::SwapBuffer() {
  using std::swap;
  swap(flush_buffer_, log_buffer_);
  flush_buffer_size_ = log_buffer_size_;
  log_buffer_size_ = 0;
}

void LogManager::GetBgTaskToWork() {
  cv_.notify_all();
}

void LogManager::WaitUntilBgTaskFinish() {
  std::unique_lock<std::mutex> condWait(latch_);
  while (flush_buffer_size_ != 0) {
    flushed.wait(condWait);
  }
}
/*
 * append a log record into log buffer
 * you MUST set the log record's lsn within this method
 * @return: lsn that is assigned to this log record
 *
 *
 * example below
 * // First, serialize the must have fields(20 bytes in total)
 * log_record.lsn_ = next_lsn_++;
 * memcpy(log_buffer_ + offset_, &log_record, 20);
 * int pos = offset_ + 20;
 *
 * if (log_record.log_record_type_ == LogRecordType::INSERT) {
 *    memcpy(log_buffer_ + pos, &log_record.insert_rid_, sizeof(RID));
 *    pos += sizeof(RID);
 *    // we have provided serialize function for tuple class
 *    log_record.insert_tuple_.SerializeTo(log_buffer_ + pos);
 *  }
 *
 */
/**
 * about concurrency.
 * in case there're many threads calling this method,
 * appending procedure must be done one after another.
 * latch_ is hold until the appending is done.
 *
 * when log_buffer cannot hold the incoming record,
 * it needs the bg thread to write out flush_buffer and make swap of ptrs.
 * after that log_buffer is empty
 * @param log_record
 * @return
 */
lsn_t LogManager::AppendLogRecord(LogRecord &log_record) {
  log_record.lsn_ = next_lsn_++;
  auto size = log_record.GetSize();
  std::unique_lock<std::mutex> guard(log_mtx_);//this is used only to sync AppendLogRecord function call
  std::unique_lock<std::mutex> guard2(latch_);
  if (size + log_buffer_size_ > LOG_BUFFER_SIZE) {
    //1.make sure flush_buffer is written out
    //wake up bg thread
    GetBgTaskToWork();
    guard2.unlock();
    //wait until bg finish writing
    WaitUntilBgTaskFinish();
    assert(log_buffer_size_ == 0);
    guard2.lock();
  }
  int pos = log_buffer_size_;
  memcpy(log_buffer_ + pos, &log_record, LogRecord::HEADER_SIZE);
  pos += LogRecord::HEADER_SIZE;

  if (log_record.log_record_type_ == LogRecordType::INSERT) {
    memcpy(log_buffer_ + pos, &log_record.insert_rid_, sizeof(RID));
    pos += sizeof(RID);
    // we have provided serialize function for tuple class
    log_record.insert_tuple_.SerializeTo(log_buffer_ + pos);
  } else if (log_record.log_record_type_ == LogRecordType::APPLYDELETE
      || log_record.log_record_type_ == LogRecordType::MARKDELETE
      || log_record.log_record_type_ == LogRecordType::ROLLBACKDELETE) {
    memcpy(log_buffer_ + pos, &log_record.delete_rid_, sizeof(RID));
    pos += sizeof(RID);
    log_record.delete_tuple_.SerializeTo(log_buffer_ + pos);
  } else if (log_record.log_record_type_ == LogRecordType::UPDATE) {
//    RID update_rid_;
//    Tuple old_tuple_;
//    Tuple new_tuple_;
    memcpy(log_buffer_ + pos, &log_record.update_rid_, sizeof(RID));
    pos += sizeof(RID);
    log_record.old_tuple_.SerializeTo(log_buffer_ + pos);
    pos += log_record.old_tuple_.GetLength() + sizeof(int32_t);
    log_record.new_tuple_.SerializeTo(log_buffer_ + pos);
  } else {
    assert(log_record.log_record_type_ == LogRecordType::NEWPAGE);
//    page_id_t prev_page_id_ = INVALID_PAGE_ID;
    memcpy(log_buffer_ + pos, &log_record.prev_page_id_, sizeof(log_record.prev_page_id_));
  }
  log_buffer_size_ += log_record.GetSize();
  return log_record.lsn_;
}

} // namespace cmudb
