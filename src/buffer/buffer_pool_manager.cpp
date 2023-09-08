//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// buffer_pool_manager.cpp
//
// Identification: src/buffer/buffer_pool_manager.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "buffer/buffer_pool_manager.h"

#include "common/exception.h"
#include "common/macros.h"
#include "storage/page/page_guard.h"

namespace bustub {

BufferPoolManager::BufferPoolManager(size_t pool_size, DiskManager *disk_manager, size_t replacer_k,
                                     LogManager *log_manager)
    : pool_size_(pool_size), disk_manager_(disk_manager), log_manager_(log_manager) {
  // TODO(students): remove this line after you have implemented the buffer pool manager

  // we allocate a consecutive memory space for the buffer pool
  pages_ = new Page[pool_size_];
  replacer_ = std::make_unique<LRUKReplacer>(pool_size, replacer_k);

  // Initially, every page is in the free list.
  for (size_t i = 0; i < pool_size_; ++i) {
    free_list_.emplace_back(static_cast<int>(i));
  }
}

BufferPoolManager::~BufferPoolManager() { delete[] pages_; }

auto BufferPoolManager::NewPage(page_id_t *page_id) -> Page * {
  // find frame_id
  std::lock_guard<std::mutex> lock(latch_);
  frame_id_t fid = -1;
  if (!free_list_.empty()) {
    fid = free_list_.front();
    free_list_.pop_front();
  } else {
    replacer_->Evict(&fid);
  }
  if (fid == -1) {
    return nullptr;
  }

  page_id_t pid = pages_[fid].GetPageId();
  page_table_.erase(pid);
  if (pages_[fid].IsDirty()) {
    disk_manager_->WritePage(pid, pages_[fid].GetData());
  }

  page_id_t new_page = AllocatePage();
  page_table_[new_page] = fid;
  pages_[fid].ResetMemory();
  pages_[fid].pin_count_ = 1;
  pages_[fid].is_dirty_ = false;
  pages_[fid].page_id_ = new_page;
  replacer_->RecordAccess(fid);
  replacer_->SetEvictable(fid, false);

  *page_id = new_page;
  return &pages_[fid];
}

auto BufferPoolManager::FetchPage(page_id_t page_id, [[maybe_unused]] AccessType access_type) -> Page * {
  std::lock_guard<std::mutex> lock(latch_);
  if (page_table_.count(page_id) > 0) {
    frame_id_t fid = page_table_.at(page_id);
    pages_[fid].pin_count_++;
    replacer_->RecordAccess(fid);
    replacer_->SetEvictable(fid, false);
    return &pages_[fid];
  }

  frame_id_t fid = -1;
  if (!free_list_.empty()) {
    fid = free_list_.front();
    free_list_.pop_front();
  } else {
    replacer_->Evict(&fid);
  }
  if (fid == -1) {
    return nullptr;
  }

  page_id_t pid = pages_[fid].GetPageId();
  page_table_.erase(pid);
  if (pages_[fid].IsDirty()) {
    disk_manager_->WritePage(pid, pages_[fid].GetData());
    pages_[fid].is_dirty_ = false;
  }

  page_table_[page_id] = fid;
  pages_[fid].ResetMemory();
  disk_manager_->ReadPage(page_id, pages_[fid].data_);
  pages_[fid].pin_count_ = 1;
  pages_[fid].page_id_ = page_id;
  replacer_->RecordAccess(fid);
  replacer_->SetEvictable(fid, false);

  return &pages_[fid];
}

auto BufferPoolManager::UnpinPage(page_id_t page_id, bool is_dirty, [[maybe_unused]] AccessType access_type) -> bool {
  std::lock_guard<std::mutex> lock(latch_);
  if (page_table_.count(page_id) == 0 || pages_[page_table_.at(page_id)].GetPinCount() <= 0) {
    return false;
  }
  pages_[page_table_.at(page_id)].pin_count_--;

  if (is_dirty) {
    pages_[page_table_.at(page_id)].is_dirty_ = true;
  }
  if (pages_[page_table_.at(page_id)].GetPinCount() == 0) {
    replacer_->SetEvictable(page_table_.at(page_id), true);
  }

  return true;
}

auto BufferPoolManager::FlushPage(page_id_t page_id) -> bool {
  std::lock_guard<std::mutex> lock(latch_);
  if (page_table_.count(page_id) == 0) {
    return false;
  }
  disk_manager_->WritePage(page_id, pages_[page_table_.at(page_id)].GetData());
  pages_[page_table_.at(page_id)].is_dirty_ = false;
  return true;
}

void BufferPoolManager::FlushAllPages() {
  std::lock_guard<std::mutex> lock(latch_);
  for (auto iter : page_table_) {
    frame_id_t fid = iter.second;
    disk_manager_->WritePage(iter.first, pages_[fid].GetData());
    pages_[fid].is_dirty_ = false;
  }
}

auto BufferPoolManager::DeletePage(page_id_t page_id) -> bool {
  std::lock_guard<std::mutex> lock(latch_);
  if (page_table_.count(page_id) == 0) {
    return true;
  }
  frame_id_t fid = page_table_.at(page_id);
  if (pages_[fid].GetPinCount() > 0) {
    return false;
  }
  replacer_->Remove(fid);
  page_table_.erase(page_id);

  pages_[fid].ResetMemory();
  pages_[fid].pin_count_ = 0;
  pages_[fid].is_dirty_ = false;
  free_list_.push_back(fid);

  DeallocatePage(page_id);

  return true;
}

auto BufferPoolManager::AllocatePage() -> page_id_t { return next_page_id_++; }

auto BufferPoolManager::FetchPageBasic(page_id_t page_id) -> BasicPageGuard { return {this, FetchPage(page_id)}; }

auto BufferPoolManager::FetchPageRead(page_id_t page_id) -> ReadPageGuard {
  Page *p = FetchPage(page_id);
  p->RLatch();
  return {this, p};
}

auto BufferPoolManager::FetchPageWrite(page_id_t page_id) -> WritePageGuard {
  Page *p = FetchPage(page_id);
  p->WLatch();
  return {this, p};
}

auto BufferPoolManager::NewPageGuarded(page_id_t *page_id) -> BasicPageGuard {
  Page *p = NewPage(page_id);
  return {this, p};
}

}  // namespace bustub
