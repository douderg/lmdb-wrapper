#ifndef _CURSOR_HPP_
#define _CURSOR_HPP_

#include "lmdb-wrapper/value.hpp"
#include <optional>
#include <iostream>

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

    /**
        @param op one of the following op codes
        MDB_FIRST, MDB_FIRST_DUP, MDB_GET_BOTH, MDB_GET_BOTH_RANGE,
        MDB_GET_CURRENT, MDB_GET_MULTIPLE, MDB_LAST, MDB_LAST_DUP,
        MDB_NEXT, MDB_NEXT_DUP, MDB_NEXT_MULTIPLE, MDB_NEXT_NODUP,
        MDB_PREV, MDB_PREV_DUP, MDB_PREV_NODUP, MDB_SET,
        MDB_SET_KEY, MDB_SET_RANGE
    */
    std::optional<std::pair<K, T>> get(MDB_cursor_op op) const {
        std::optional<std::pair<K, T>> result;
        MDB_val key, data;
        int err = mdb_cursor_get(cursor_, &key, &data, op);
        if (!err) {
            object<T> obj(data);
            result = std::make_pair(value::unpack<K>(key), obj.value());
        }
        return result;
    }

    std::optional<std::pair<K, T>> get(const K& key, const T& value, MDB_cursor_op op) const {
        std::optional<std::pair<K, T>> result;
        MDB_val mdb_key = value::pack<K>(key);
        object<T> obj(value);
        if (!mdb_cursor_get(cursor_, &mdb_key, obj.data(), op)) {
            result = std::make_pair(value::unpack<K>(mdb_key), obj.value());
        }
        return result;
    }

private:
    MDB_cursor *cursor_;
};

}

#endif