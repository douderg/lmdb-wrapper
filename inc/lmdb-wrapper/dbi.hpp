#ifndef _DBI_HPP_
#define _DBI_HPP_

#include "lmdb-wrapper/value.hpp"
#include "lmdb-wrapper/cursor.hpp"

#include <stdexcept>

namespace lmdb {

class dbi {
    template <class Key>
    class store;

public:
    class factory;

    enum class flags : unsigned int {
        reverse_key = MDB_REVERSEKEY,
        dup_sort = MDB_DUPSORT,
        integer_key = MDB_INTEGERKEY,
        dup_fixed = MDB_DUPFIXED,
        integer_dup = MDB_INTEGERDUP,
        reverse_dup = MDB_REVERSEDUP,
        create = MDB_CREATE
    };

    dbi(MDB_txn*, unsigned int);

    dbi(MDB_txn*, const std::string&, unsigned int);

    dbi(const dbi&) = default;

    dbi& operator=(const dbi&) = default;

    template <class T>
    T get(MDB_txn* txn, const std::string& key) const;

    template <class T>
    T get(MDB_txn* txn, const std::string& key, const T& def_val) const;

    template <class T>
    void put(MDB_txn* txn, const std::string& key, const T& val, unsigned int flags) const;

    template <class T>
    T get(MDB_txn* txn, size_t key) const;

    template <class T>
    T get(MDB_txn* txn, size_t key, const T& def_val) const;

    template <class T>
    void put(MDB_txn* txn, const size_t& key, const T& val, unsigned int flags) const;

    template <class K, class T>
    cursor<K, T> open_cursor(MDB_txn* t);

private:

    MDB_dbi dbi_;
};

class dbi::factory {
public:
    factory(MDB_txn* txn);
    factory& set(flags);
    factory& unset(flags);
    factory& unset_flags();
    dbi open(const std::string& name);
    dbi open();
private:
    MDB_txn *txn_;
    unsigned int flags_;
};

template <class Key>
class dbi::store {
public:
    typedef Key key_t;

    template <class T>
    static T get(MDB_txn *txn, MDB_dbi dbi, const key_t& key) {
        MDB_val k = value::pack(key);
        MDB_val result;
        auto err = mdb_get(txn, dbi, &k, &result);
        object<T> obj(result);
        switch (err) {
            case 0:
                return obj.value();
            case MDB_NOTFOUND: 
                throw std::runtime_error("key does not exist");
            default: 
                throw std::runtime_error("failed to get value");
        }
    }

    template <class T>
    static T get(MDB_txn *txn, MDB_dbi dbi, const key_t& key, const T& default_value) {
        MDB_val k = value::pack(key);
        MDB_val result;
        auto err = mdb_get(txn, dbi, &k, &result);
        object<T> obj(result);
        switch (err) {
            case 0:
                return obj.value(); 
            case MDB_NOTFOUND: 
                return default_value;
            default: 
                throw std::runtime_error("failed to get value");
        }
    }

    template <class T>
    static void put(MDB_txn *txn, MDB_dbi dbi, const key_t& key, const T& value, unsigned int flags) {
        MDB_val k = value::pack(key);
        object<T> obj(value);
        auto err = mdb_put(txn, dbi, &k, obj.data(), flags);
        switch (err) {
            case 0:
                break;
            case MDB_MAP_FULL: 
                throw std::runtime_error("db is full");
            case MDB_TXN_FULL: 
                throw std::runtime_error("txn has too many dirty pages");
            case MDB_KEYEXIST:
                throw std::runtime_error("key exists");
            default: 
                throw std::runtime_error("failed to put value");
        }
    }
};

template <class T>
inline T dbi::get(MDB_txn* txn, const std::string& key) const {
    return dbi::store<std::string>::template get<T>(txn, dbi_, key);
}

template <class T>
inline T dbi::get(MDB_txn* txn, const std::string& key, const T& def_val) const {
    return dbi::store<std::string>::template get<T>(txn, dbi_, key, def_val);
}

template <class T>
inline void dbi::put(MDB_txn* txn, const std::string& key, const T& val, unsigned int flags) const {
    dbi::store<std::string>::template put<T>(txn, dbi_, key, val, flags);
}

template <class T>
inline T dbi::get(MDB_txn* txn, size_t key) const {
    return dbi::store<size_t>::template get<T>(txn, dbi_, key);
}

template <class T>
inline T dbi::get(MDB_txn* txn, size_t key, const T& def_val) const {
    return dbi::store<size_t>::template get<T>(txn, dbi_, key, def_val);
}

template <class T>
inline void dbi::put(MDB_txn* txn, const size_t& key, const T& val, unsigned int flags) const {
    dbi::store<size_t>::template put<T>(txn, dbi_, key, val, flags);
}

template <class K, class T>
inline cursor<K, T> dbi::open_cursor(MDB_txn* t) {
    return std::move(cursor<K, T>(t, dbi_));
}

}

#endif