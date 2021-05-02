#include "lmdb-wrapper/env.hpp"


namespace lmdb {

env::env(std::shared_ptr<MDB_env> env):env_{env} {

}

MDB_env* env::handle() const {
    return env_.get();
}

void env::deleter::operator()(MDB_env *ptr) {
    if (ptr) {
        mdb_env_close(ptr);
    }
}

env::factory& env::factory::set_map_size(size_t size) {
    map_size_ = size;
    return *this;
}

env::factory& env::factory::set_max_readers(unsigned int count) {
    max_readers_ = count;
    return *this;
}

env::factory& env::factory::set_max_dbs(MDB_dbi dbs) {
    max_dbs_ = dbs;
    return *this;
}

env::factory& env::factory::set(env::flags flag) {
    flags_ |= static_cast<unsigned int>(flag);
    return *this;
}

bool env::factory::get(env::flags flag) const {
    return flags_ & static_cast<unsigned int>(flag);
}

env::factory& env::factory::unset(env::flags flag) {
    flags_ ^= static_cast<unsigned int>(flag);
    return *this;
}

env::factory& env::factory::unset_flags() {
    flags_ = 0;
    return *this;
}

env env::factory::open(const std::string& path, mdb_mode_t mode) {

    MDB_env *ptr;
    std::shared_ptr<MDB_env> result(mdb_env_create(&ptr)? nullptr : ptr, env::deleter());
    if (!result) {
        throw std::runtime_error("invalid env");
    }

    if (max_dbs_) {
        int err = mdb_env_set_maxdbs(result.get(), *max_dbs_);
        switch (err) {
            case 0: break;
            case EINVAL: throw std::runtime_error("invalid env");
            default: throw std::runtime_error("failed to set max dbs");
        }
    }

    if (max_readers_) {
        int err = mdb_env_set_maxreaders(result.get(), *max_readers_);
        switch (err) {
            case 0: break;
            case EINVAL: throw std::runtime_error("invalid env");
            default: throw std::runtime_error("failed to set max readers");
        }
    }

    if (map_size_) {
        int err = mdb_env_set_mapsize(result.get(), *map_size_);
        if (err) {
            throw std::runtime_error("failed to set map size");
        }
    }

    int err = mdb_env_open(result.get(), path.c_str(), flags_, mode);
    switch (err) {
        case 0:
            break; // success
        case MDB_VERSION_MISMATCH: 
            throw std::runtime_error("version mismatch");
        case MDB_INVALID: 
            throw std::runtime_error("corrupted env");
        case ENOENT: 
            throw std::runtime_error("directory does not exist");
        case EACCES: 
            throw std::runtime_error("permission denied");
        case EAGAIN: 
            throw std::runtime_error("env is locked");
        default:
            throw std::runtime_error("failed to open env");
    }
    return env{result};
}

};