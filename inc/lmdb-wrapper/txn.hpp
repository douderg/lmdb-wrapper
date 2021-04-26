#ifndef _TXN_HPP_
#define _TXN_HPP_

#include "lmdb-wrapper/env.hpp"
#include "lmdb-wrapper/dbi.hpp"

namespace lmdb {

class read_txn;

template <class Impl>
class txn {
public:
    txn(MDB_env*, unsigned int flags);
    txn(MDB_env*, MDB_txn*, unsigned int flags);
    ~txn();

    txn(const txn&) = delete;
    txn& operator=(const txn&) = delete;

    txn(txn&&);
    txn& operator=(txn&&);

    Impl& commit();
    Impl& abort();
    Impl& reset();
    Impl& renew();

    read_txn nested_read();

    dbi::factory db();

    template <class T, class K>
    T get(const dbi& db, const K& key) {
        return db.template get<T>(txn_, key);
    }

    template <class T, class K>
    T get(const dbi& db, const K& key, const T& default_value) {
        return db.template get<T>(txn_, key, default_value);
    }

    MDB_env* env() const;
    MDB_txn* handle() const;

protected:
    MDB_txn *txn_;
};

class read_txn : public txn<read_txn> {
public:
    static constexpr unsigned int flags = MDB_RDONLY;
    read_txn(MDB_env*);
    read_txn(MDB_env*, MDB_txn*);
    ~read_txn() = default;
};

class write_txn : public txn<write_txn> {
public:
    static constexpr unsigned int flags = 0;
    write_txn(MDB_env*);
    write_txn(MDB_env*, MDB_txn*);
    ~write_txn() = default;

    write_txn nested_write();

    template <class T, class K>
    write_txn& put(const dbi& db, const K& key, const T& value, unsigned int flags) {
        db.template put<T>(txn_, key, value, flags);
        return *this;
    }

    template <class T, class K>
    write_txn& del(const dbi& db, const K& key, const T& value) {
        return *this;
    }

    template <class K>
    write_txn& del(const dbi& db, const K& key) {
        return *this;
    }
};

template <class Impl>
txn<Impl>::txn(MDB_env* env, unsigned int flags):txn(env, nullptr, flags) {
    
}

template <class Impl>
txn<Impl>::txn(MDB_env* env, MDB_txn* parent, unsigned int flags) {
    mdb_txn_begin(env, parent, flags, &txn_);
}

template <class Impl>
txn<Impl>::~txn() {
    if (txn_) {
        mdb_txn_abort(txn_);
        txn_ = nullptr;
    }
}

template <class Impl>
txn<Impl>::txn(txn&& other):txn_{other.txn_} {
    other.txn_ = nullptr;
}

template <class Impl>
txn<Impl>& txn<Impl>::operator=(txn&& other) {
    if (txn_) {
        mdb_txn_abort(txn_);
    }
    txn_ = other.txn_;
    other.txn_ = nullptr;
    return *this;
}

template <class Impl>
Impl& txn<Impl>::commit() {
    auto err = mdb_txn_commit(txn_);
    switch (err) {
        case 0: break;
        case EINVAL: throw std::runtime_error("invalid transaction");
        case ENOSPC: throw std::runtime_error("no dik space");
        case EIO: throw std::runtime_error("I/O error");
        case ENOMEM: throw std::runtime_error("out of memory");
        default: throw std::runtime_error("failed to commit transaction");
    }
    txn_ = nullptr;
    return static_cast<Impl&>(*this);
}

template <class Impl>
Impl& txn<Impl>::abort() {
    mdb_txn_abort(txn_);
    txn_ = nullptr;
    return static_cast<Impl&>(*this);
}

template <class Impl>
Impl& txn<Impl>::reset() {
    mdb_txn_reset(txn_);
    return static_cast<Impl&>(*this);
}

template <class Impl>
Impl& txn<Impl>::renew() {
    auto err = mdb_txn_renew(txn_);
    switch (err) {
        case 0: break;
        case MDB_PANIC: throw std::runtime_error("fatal error");
        case EINVAL: throw std::runtime_error("invalid transaction");
        default: throw std::runtime_error("failed to renew transaction");
    }
    return static_cast<Impl&>(*this);
}

template <class Impl>
MDB_env* txn<Impl>::env() const {
    return mdb_txn_env(txn_);
}

template <class Impl>
MDB_txn* txn<Impl>::handle() const {
    return txn_;
}

template <class Impl>
read_txn txn<Impl>::nested_read() {
    if (txn_) {
        return read_txn(mdb_txn_env(txn_), txn_);
    }
    throw std::runtime_error("invalid txn");
}

template <class Impl>
dbi::factory txn<Impl>::db() {
    return dbi::factory(txn_);
}

}

#endif