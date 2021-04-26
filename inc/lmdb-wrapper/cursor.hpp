#ifndef _CURSOR_HPP_
#define _CURSOR_HPP_

#include "lmdb-wrapper/value.hpp"
#include <optional>

namespace lmdb {

template <class K, class T>
class cursor {
public:
    cursor(MDB_txn* t, MDB_dbi d) {
        mdb_cursor_open(t, d, &cursor_);
    }

    ~cursor() {
        if (cursor_) {
            mdb_cursor_close(cursor_);
            cursor_ = nullptr;
        }
    }

    cursor(const cursor&) = delete;
    cursor& operator=(const cursor&) = delete;

    cursor(cursor&& other):cursor_{other.cursor_} {
        other.cursor_ = nullptr;
    }

    cursor& operator=(cursor&& other) {
        if (cursor_) {
            mdb_cursor_close(cursor_);
        }
        cursor_ = other.cursor_;
        other.cursor_ = nullptr;
        return *this;
    }

    std::optional<std::pair<K, T>> get(MDB_cursor_op op) {
        std::optional<std::pair<K, T>> result;
        MDB_val key, data;
        if (!mdb_cursor_get(cursor_, &key, &data, op)) {
            result = std::make_pair(value::unpack<K>(key), value::unpack<T>(data));
        }
        return result;
    }

private:
    MDB_cursor *cursor_;
};

}

#endif