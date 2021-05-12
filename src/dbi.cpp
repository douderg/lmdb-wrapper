#include "lmdb-wrapper/dbi.hpp"

namespace lmdb {

dbi::dbi(MDB_txn* txn, const std::string& path, unsigned int flags) {
    auto err = mdb_dbi_open(txn, path.c_str(), flags, &dbi_);
    switch (err) {
        case 0:
            break;
        case MDB_NOTFOUND:
            throw std::runtime_error("db not found");
        case MDB_DBS_FULL:
            throw std::runtime_error("max dbs reached");
        default:
            throw std::runtime_error("failed to open dbi");
    }
}

dbi::dbi(MDB_txn* txn, unsigned int flags) {
    auto err = mdb_dbi_open(txn, NULL, flags, &dbi_);
    switch (err) {
        case 0:
            break;
        case MDB_NOTFOUND:
            throw std::runtime_error("db not found");
        case MDB_DBS_FULL:
            throw std::runtime_error("max dbs reached");
        default:
            throw std::runtime_error("failed to open dbi");
    }
}

dbi::factory::factory(MDB_txn* txn):txn_{txn}, flags_{0} {

}

dbi::factory& dbi::factory::set(dbi::flags flag) {
    flags_ |= static_cast<unsigned int>(flag);
    return *this;
}

dbi::factory& dbi::factory::unset(dbi::flags flag) {
    flags_ ^= static_cast<unsigned int>(flag);
    return *this;
}

dbi::factory& dbi::factory::unset_flags() {
    flags_ = 0;
    return *this;
}

dbi dbi::factory::open(const std::string& path) {
    return dbi(txn_, path, flags_);
}

dbi dbi::factory::open() {
    return dbi(txn_, flags_);
}

}