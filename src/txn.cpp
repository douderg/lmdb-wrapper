#include "lmdb-wrapper/txn.hpp"

namespace lmdb {

read_txn::read_txn(MDB_env* env): txn<read_txn>(env, MDB_RDONLY) {

}

read_txn::read_txn(MDB_env* env, MDB_txn* parent): txn<read_txn>(env, parent, MDB_RDONLY) {

}

write_txn::write_txn(MDB_env* env): txn<write_txn>(env, 0) {

}

write_txn::write_txn(MDB_env* env, MDB_txn* parent): txn<write_txn>(env, parent, 0) {

}

write_txn write_txn::nested_write() {
    return write_txn(mdb_txn_env(txn_), txn_);
}

}