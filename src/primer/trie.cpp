#include "primer/trie.h"
#include <string_view>
#include <vector>
#include "common/exception.h"

namespace bustub {

template <class T>
auto Trie::Get(std::string_view key) const -> const T * {
  int len = key.size();
  if (!root_) {
    return nullptr;
  }
  std::shared_ptr<const TrieNode> ptr = root_;

  for (int i = 0; i < len; i++) {
    if (ptr->children_.count(key[i]) == 0) {
      return nullptr;
    }
    ptr = ptr->children_.at(key[i]);
  }

  auto value = dynamic_cast<const TrieNodeWithValue<T> *>(ptr.get());
  if (value == nullptr) {
    return nullptr;
  }
  return value->value_.get();
  // throw NotImplementedException("Trie::Get is not implemented.");

  // You should walk through the trie to find the node corresponding to the key. If the node doesn't exist, return
  // nullptr. After you find the node, you should use `dynamic_cast` to cast it to `const TrieNodeWithValue<T> *`. If
  // dynamic_cast returns `nullptr`, it means the type of the value is mismatched, and you should return nullptr.
  // Otherwise, return the value.
}

template <class T>
auto Trie::Put(std::string_view key, T value) const -> Trie {
  int len = key.length();
  // 如果value是unique_ptr类型，函数调用后value=nullptr
  auto val = std::make_shared<T>(std::move(value));

  std::shared_ptr<TrieNode> root = root_ == nullptr ? std::make_shared<TrieNode>() : root_->Clone();
  if (len == 0) {
    root = std::make_shared<TrieNodeWithValue<T>>(root->children_, val);
    return Trie(root);
  }
  std::shared_ptr<TrieNode> ptr = root;

  int cnt = 0;
  for (; cnt < len - 1; cnt++) {
    if (ptr->children_.count(key[cnt]) == 0) {
      break;
    }
    ptr->children_[key[cnt]] = ptr->children_[key[cnt]]->Clone();
    ptr = std::const_pointer_cast<TrieNode>(ptr->children_[key[cnt]]);
  }
  for (; cnt < len - 1; cnt++) {
    ptr->children_[key[cnt]] = std::make_shared<TrieNode>();
    ptr = std::const_pointer_cast<TrieNode>(ptr->children_[key[cnt]]);
  }
  if (ptr->children_.count(key.back()) == 0) {
    ptr->children_[key[len - 1]] = std::make_shared<const TrieNodeWithValue<T>>(val);
  } else {
    ptr->children_[key[len - 1]] =
        std::make_shared<const TrieNodeWithValue<T>>(ptr->children_[key[len - 1]]->children_, val);
  }

  return Trie(root);
  // Note that `T` might be a non-copyable type. Always use `std::move` when creating `shared_ptr` on that value.

  // You should walk through the trie and create new nodes if necessary. If the node corresponding to the key already
  // exists, you should create a new `TrieNodeWithValue`.
}

auto Trie::Remove(std::string_view key) const -> Trie {
  // std::move(root_->Clone) goes wrong
  std::shared_ptr<TrieNode> root = root_ == nullptr ? std::make_shared<TrieNode>() : root_->Clone();

  std::shared_ptr<TrieNode> ptr = root;

  std::vector<std::shared_ptr<TrieNode>> vec;

  vec.push_back(root);
  // 找到key对应的node
  int len = key.length();
  if (len == 0) {
    root = std::make_shared<TrieNode>(root->children_);
    return Trie(root);
  }
  for (int i = 0; i < len - 1; i++) {
    auto temp = ptr->children_.find(key[i]);
    if (temp == ptr->children_.end()) {
      return {};
    }
    ptr->children_[key[i]] = temp->second->Clone();
    ptr = std::const_pointer_cast<TrieNode>(ptr->children_[key[i]]);
    vec.push_back(ptr);
  }
  if (ptr->children_.empty() || !ptr->children_[key.back()]->is_value_node_) {
    return {};
  }

  ptr->children_[key.back()] = std::make_shared<TrieNode>(ptr->children_[key.back()]->children_);

  int n = vec.size();
  for (int i = n - 1; i >= 1; i--) {
    if (vec[i]->children_.empty() && !vec[i]->is_value_node_) {
      vec[i - 1]->children_.erase(key[i - 1]);
    } else {
      break;
    }
  }
  return Trie(root);

  // throw NotImplementedException("Trie::Remove is not implemented.");

  // You should walk through the trie and remove nodes if necessary. If the node doesn't contain a value any more,
  // you should convert it to `TrieNode`. If a node doesn't have children any more, you should remove it.
}

// Below are explicit instantiation of template functions.
//
// Generally people would write the implementation of template classes and functions in the header file. However, we
// separate the implementation into a .cpp file to make things clearer. In order to make the compiler know the
// implementation of the template functions, we need to explicitly instantiate them here, so that they can be picked up
// by the linker.

template auto Trie::Put(std::string_view key, uint32_t value) const -> Trie;
template auto Trie::Get(std::string_view key) const -> const uint32_t *;

template auto Trie::Put(std::string_view key, uint64_t value) const -> Trie;
template auto Trie::Get(std::string_view key) const -> const uint64_t *;

template auto Trie::Put(std::string_view key, std::string value) const -> Trie;
template auto Trie::Get(std::string_view key) const -> const std::string *;

// If your solution cannot compile for non-copy tests, you can remove the below lines to get partial score.

using Integer = std::unique_ptr<uint32_t>;

template auto Trie::Put(std::string_view key, Integer value) const -> Trie;
template auto Trie::Get(std::string_view key) const -> const Integer *;

template auto Trie::Put(std::string_view key, MoveBlocked value) const -> Trie;
template auto Trie::Get(std::string_view key) const -> const MoveBlocked *;

}  // namespace bustub
