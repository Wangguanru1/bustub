#include "storage/page/page_guard.h"
#include "buffer/buffer_pool_manager.h"

namespace bustub {

BasicPageGuard::BasicPageGuard(BasicPageGuard &&that) noexcept {
  bpm_ = that.bpm_;
  if (page_ != nullptr) {
    this->Drop();
  }
  page_ = that.page_;
  is_dirty_ = that.is_dirty_;
  is_dropped_ = that.is_dropped_;
  that.is_dropped_ = true;
}

void BasicPageGuard::Drop() {
  if (!is_dropped_) {
    bpm_->UnpinPage(PageId(), is_dirty_);
    is_dirty_ = false;
    is_dropped_ = true;
  }
}

auto BasicPageGuard::operator=(BasicPageGuard &&that) noexcept -> BasicPageGuard & {
  bpm_ = that.bpm_;
  if (page_ != nullptr) {
    this->Drop();
  }
  page_ = that.page_;
  is_dirty_ = that.is_dirty_;
  is_dropped_ = that.is_dropped_;
  that.is_dropped_ = true;

  return *this;
}

BasicPageGuard::~BasicPageGuard() {
  if (!is_dropped_) {
    bpm_->UnpinPage(PageId(), is_dirty_);
  }
};  // NOLINT

ReadPageGuard::ReadPageGuard(ReadPageGuard &&that) noexcept { guard_ = std::move(that.guard_); }

auto ReadPageGuard::operator=(ReadPageGuard &&that) noexcept -> ReadPageGuard & {
  if (this->guard_.page_ != nullptr) {
    this->guard_.page_->RUnlatch();
  }

  guard_ = std::move(that.guard_);
  return *this;
}

void ReadPageGuard::Drop() {
  guard_.Drop();
  guard_.page_->RUnlatch();
}

ReadPageGuard::~ReadPageGuard() {
  if (!guard_.is_dropped_) {
    guard_.page_->RUnlatch();
  }
}  // NOLINT

WritePageGuard::WritePageGuard(WritePageGuard &&that) noexcept { guard_ = std::move(that.guard_); }

auto WritePageGuard::operator=(WritePageGuard &&that) noexcept -> WritePageGuard & {
  if (this->guard_.page_ != nullptr) {
    guard_.page_->WUnlatch();
  }
  guard_ = std::move(that.guard_);
  return *this;
}

void WritePageGuard::Drop() {
  guard_.Drop();
  guard_.page_->WUnlatch();
}

WritePageGuard::~WritePageGuard() {
  if (!guard_.is_dropped_) {
    guard_.page_->WUnlatch();
  }
}  // NOLINT

}  // namespace bustub
