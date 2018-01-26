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
  if (flush_thread_on == false) {
    ENABLE_LOGGING = true;
    flush_thread_on = true;
    flush_thread_ = new std::thread(&LogManager::bgFsync, this);
  } else {
    std::lock_guard<std::mutex> guard(force_flush_mutex_);
    force_flush_cv_.notify_one();
  }
}

void LogManager::bgFsync() {
  while (flush_thread_on) {
    std::unique_lock<std::mutex> lock(force_flush_mutex_);
    while(flush_buffer_size_ == 0){
      auto ret = force_flush_cv_.wait_for(lock, std::chrono::seconds(LOG_TIMEOUT));
      if(ret == std::cv_status::no_timeout){
        //required for force flushing
        break;
      }
    }
    disk_manager_->WriteLog(flush_buffer_, flush_buffer_size_);
    flush_buffer_size_ = 0;
  }
}
/*
 * Stop and join the flush thread, set ENABLE_LOGGING = false
 */
void LogManager::StopFlushThread() {
  if (flush_thread_on == true) {
    flush_thread_on = false;
    ENABLE_LOGGING = false;
    {
      //wake up working thread, or it may take a long time waiting before it's been joined
      std::lock_guard<std::mutex> guard(force_flush_mutex_);
      force_flush_cv_.notify_one();
    }
    assert(flush_thread_->joinable());
    flush_thread_->join();
    delete flush_thread_;
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
lsn_t LogManager::AppendLogRecord(LogRecord &log_record) {
  log_record.lsn_ = next_lsn_++;
  auto size = log_record.GetSize();
  if (size + log_buffer_size_ > LOG_BUFFER_SIZE) {
    RunFlushThread();
    std::swap(flush_buffer_, log_buffer_);
    std::swap(flush_buffer_size_, log_buffer_size_);
    log_buffer_size_ = 0;
  }
  size_t pos = log_buffer_size_;
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
    // we have provided serialize function for tuple class
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
