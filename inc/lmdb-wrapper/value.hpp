#ifndef _VALUE_HPP_
#define _VALUE_HPP_

#include <lmdb.h>
#include <cstring>
#include <string>

namespace lmdb {
struct value {
    template <class T>
    static MDB_val pack(const T& val) {
        MDB_val result;
        result.mv_size = sizeof(T);
        result.mv_data = const_cast<T*>(&val);
        return result;
    }

    template <class T>
    static T unpack(const MDB_val& val) {
        T result;
        std::memcpy(&result, val.mv_data, sizeof(T));
        return result;
    }
};

template <>
inline MDB_val value::pack<std::string>(const std::string& val) {
    MDB_val result;
    result.mv_size = val.size();
    result.mv_data = const_cast<char*>(val.data());
    return result;
}

template <>
inline std::string value::unpack<std::string>(const MDB_val& val) {
    return std::string(static_cast<char*>(val.mv_data), val.mv_size);
}
}

#endif