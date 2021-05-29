#pragma once

#include "lmdb-wrapper/env.hpp"
#include <memory>
#include <vector>
#include <numeric>
#include <algorithm>

namespace lmdb {

template <class K, class V>
class db_iterator {
    
    std::vector<cursor<K, V>> cursors_;
    std::vector<std::optional<std::pair<K, V>>> kv_;
    size_t next_;

public:
    db_iterator() = default;

    db_iterator(const txn_base& txn, const std::vector<dbi>& dbis);

    db_iterator(const db_iterator&) = default;
    
    db_iterator& operator=(const db_iterator&) = default;

    ~db_iterator() = default;

    const std::pair<K, V>& operator*() const;

    const std::pair<K, V>* operator->() const;

    db_iterator& operator++();

    bool operator==(const db_iterator& it) const;

    bool operator!=(const db_iterator& it) const;

    size_t dbi_index() const;

    db_iterator& seek_range(const K& key);

    db_iterator& seek_both(const K& key, const V& value);

private:
    void update_next();

};


template <class K, class V>
db_iterator<K, V>::db_iterator(const txn_base& txn, const std::vector<dbi>& dbis) {

    for (const auto& dbi : dbis) {
        try {
            cursors_.emplace_back(txn.handle(), dbi.handle());
        } catch (const std::runtime_error&) {
            cursors_.emplace_back(); // create an empty cursor for this db
        }
    }

    if (!cursors_.empty()) {
        for (auto& cur : cursors_) {
            kv_.push_back(cur.get(MDB_FIRST));
        }
        update_next();
    }
}

template <class K, class V>
const std::pair<K, V>& db_iterator<K, V>::operator*() const {
    if (kv_.empty() || !kv_[next_]) {
        throw std::runtime_error("invalid iterator");
    }
    return *kv_[next_];
}

template <class K, class V>
const std::pair<K, V>* db_iterator<K, V>::operator->() const {
    if (kv_.empty() || !kv_[next_]) {
        throw std::runtime_error("invalid iterator");
    }
    return &(*kv_[next_]);
}

template <class K, class V>
db_iterator<K, V>& db_iterator<K, V>::operator++() {
    if (kv_.empty() || !kv_[next_]) {
        return *this;
    }
    kv_[next_] = cursors_[next_].get(MDB_NEXT);
    update_next();
    return *this;
}

template <class K, class V>
bool db_iterator<K, V>::operator==(const db_iterator<K, V>& it) const {
    if (kv_.empty() || !kv_[next_]) {
        return it.kv_.empty() || !it.kv_[it.next_];
    }
    if (it.kv_.empty() || !it.kv_[it.next_]) {
        return false;
    }
    return kv_[next_]->first == it.kv_[it.next_]->first;
}

template <class K, class V>
bool db_iterator<K, V>::operator!=(const db_iterator<K, V>& it) const {
    return ! (*this == it);
}

template <class K, class V>
size_t db_iterator<K, V>::dbi_index() const {
    return next_;
}

template <class K, class V>
db_iterator<K, V>& db_iterator<K, V>::seek_range(const K& key) {
    
    kv_.clear();
    if (cursors_.empty()) {
        throw std::runtime_error("invalid iterator");
    }

    V dummy;
    for (auto& cur : cursors_) {
        auto data = cur.get(key, dummy, MDB_SET_RANGE);
        if (data) {
            kv_.push_back(cur.get(MDB_GET_CURRENT));
        } else {
            kv_.emplace_back();
        }
    }
    update_next();
    return *this;
}

template <class K, class V>
db_iterator<K, V>& db_iterator<K, V>::seek_both(const K& key, const V& value) {
    kv_.clear();
    if (cursors_.empty()) {
        throw std::runtime_error("invalid iterator");
    }

    for (auto& cur : cursors_) {
        auto data = cur.get(key, value, MDB_GET_BOTH);
        if (data) {
            kv_.push_back(data);
        } else {
            kv_.emplace_back();
        }
    }
    update_next();
    return *this;
}

template <class K, class V>
void db_iterator<K, V>::update_next() {
    std::vector<size_t> indices(kv_.size());
    std::iota(indices.begin(), indices.end(), 0);
    auto next = std::min_element(indices.begin(), indices.end(), [&](size_t x, size_t y) -> bool {
        if (kv_[x] && kv_[y]) {
            return kv_[x]->first < kv_[y]->first;
        }
        return bool(kv_[x]);
    });
    next_ = *next;
}

}