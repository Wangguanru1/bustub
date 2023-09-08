//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// lru_k_replacer.cpp
//
// Identification: src/buffer/lru_k_replacer.cpp
//
// Copyright (c) 2015-2022, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "buffer/lru_k_replacer.h"
#include "common/exception.h"

namespace bustub {

LRUKReplacer::LRUKReplacer(size_t num_frames, size_t k) : replacer_size_(num_frames), k_(k) {}

auto LRUKReplacer::Evict(frame_id_t *frame_id) -> bool {
  std::lock_guard<std::mutex> lock(latch_);
  bool less_than_k = false;
  size_t oldest = 9223372036854775807L;
  frame_id_t id = -1;
  for (auto iter : node_store_) {
    if (!iter.second.is_evictable_) {
      continue;
    }
    if (iter.second.history_.size() >= k_) {
      if (less_than_k) {
        continue;
      }
      if (iter.second.history_.front() < oldest) {
        oldest = iter.second.history_.front();
        id = iter.second.fid_;
      }
    } else {
      if (!less_than_k) {
        less_than_k = true;
        oldest = 9223372036854775807L;
      }
      if (iter.second.history_.front() < oldest) {
        oldest = iter.second.history_.front();
        id = iter.second.fid_;
      }
    }
  }
  if (id == -1) {
    return false;
  }
  node_store_.erase(id);
  curr_size_--;
  *frame_id = id;
  return true;
}

void LRUKReplacer::RecordAccess(frame_id_t frame_id, [[maybe_unused]] AccessType access_type) {
  std::lock_guard<std::mutex> lock(latch_);
  if (static_cast<size_t>(frame_id) >= replacer_size_) {
    throw Exception("frame_id invalid");
  }
  current_timestamp_++;
  if (node_store_.count(frame_id) == 0) {
    LRUKNode node(k_, frame_id);
    node_store_.insert({frame_id, LRUKNode(k_, frame_id)});
  }
  node_store_.at(frame_id).PushTimestamp(current_timestamp_);
}

void LRUKReplacer::SetEvictable(frame_id_t frame_id, bool set_evictable) {
  std::lock_guard<std::mutex> lock(latch_);
  if (static_cast<size_t>(frame_id) >= replacer_size_ || node_store_.count(frame_id) == 0) {
    throw Exception("frame_id invalid222");
  }
  if (node_store_.at(frame_id).is_evictable_ == set_evictable) {
    return;
  }
  if (set_evictable) {
    curr_size_++;
  } else {
    curr_size_--;
  }
  node_store_.at(frame_id).is_evictable_ = set_evictable;
}

void LRUKReplacer::Remove(frame_id_t frame_id) {
  std::lock_guard<std::mutex> lock(latch_);
  if (static_cast<size_t>(frame_id) >= replacer_size_) {
    throw Exception("frame_id invalid333");
  }
  if (node_store_.count(frame_id) == 0) {
    return;
  }
  if (node_store_.at(frame_id).is_evictable_) {
    curr_size_--;
  } else {
    throw Exception("frame_id invalid444");
  }
  node_store_.erase(frame_id);
}

auto LRUKReplacer::Size() -> size_t { return curr_size_; }

}  // namespace bustub
