#ifndef _DB_HPP_
#define _DB_HPP_

#include "txn.hpp"

#include <lmdb.h>
#include <memory>
#include <string>
#include <optional>

namespace lmdb {

class env {
public:
    class deleter;
    class factory;
    enum class flags : unsigned int {
        fixedmap = MDB_FIXEDMAP,
        nosubdir = MDB_NOSUBDIR,
        rdonly = MDB_RDONLY,
        writemap = MDB_WRITEMAP,
        nometasync = MDB_NOMETASYNC,
        nosync = MDB_NOSYNC,
        mapasync = MDB_MAPASYNC,
        notls = MDB_NOTLS,
        nolock = MDB_NOLOCK,
        nordahead = MDB_NORDAHEAD,
        nomeminit = MDB_NOMEMINIT
    };


    env() = default;
    env(std::shared_ptr<MDB_env> env);
    MDB_env* handle() const;

private:
    std::shared_ptr<MDB_env> env_;
};

class env::deleter {
public:
    void operator()(MDB_env *ptr);
};

class env::factory {
public:
    factory& set_map_size(size_t);
    factory& set_max_readers(unsigned int);
    factory& set_max_dbs(MDB_dbi);
    factory& set(env::flags);
    bool get(env::flags) const;
    factory& unset(env::flags);
    factory& unset_flags();

    env open(const std::string&, mdb_mode_t);
private:
    std::optional<size_t> map_size_;
    std::optional<unsigned int> max_readers_;
    std::optional<MDB_dbi> max_dbs_;
    unsigned int flags_;
};



// class dbi {
// public:
//     enum class opt: unsigned int {
//         reverse_key = MDB_REVERSEKEY,
//         dup_sort = MDB_DUPSORT,
//         integer_key = MDB_INTEGERKEY,
//         dup_fixed = MDB_DUPFIXED,
//         integer_dup = MDB_INTEGERDUP,
//         reverse_dup = MDB_REVERSEDUP,
//         create = MDB_CREATE
//     };

//     dbi(MDB_txn *txn, const std::string&, unsigned int);
//     ~dbi();
// private:
//     MDB_dbi id_;
// };



};

#endif