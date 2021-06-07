#ifndef _VALUE_HPP_
#define _VALUE_HPP_

#include <lmdb.h>
#include <cstring>
#include <string>
#include <vector>
#include <iostream>

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

template <class T>
class object {
public:
    object(const T& val) {
        val_ = value::pack<T>(val);
    }

    object(MDB_val val):val_{val} {
    }

    T value() const {
        return value::unpack<T>(val_); 
    }

    MDB_val* data() {
        return &val_;
    }

private:
    MDB_val val_;
};


template <>
class object<std::vector<std::string>> {
public:
    object(const std::vector<std::string>& val) {
        val_.mv_size = 0;
        for (const auto& s : val) {
            val_.mv_size += s.size() + 1;
        }
        data_.reserve(val_.mv_size);
        for (const auto& s : val) {
            data_.insert(data_.end(), s.begin(), s.end());
            data_.push_back('\0');
        }
        val_.mv_data = data_.data();
    }

    object(MDB_val val):val_{val} {
    }

    std::vector<std::string> value() const {
        size_t index = 0;
        std::vector<std::string> result;
        auto ptr = static_cast<const char*>(val_.mv_data);
        for (size_t i = 0; i < val_.mv_size; ++i) {
            if (ptr[i] == '\0') {
                result.emplace_back(ptr + index, i - index);
                index = i + 1;
            }
        }
        return result;
    }

    MDB_val* data() {
        return &val_;
    }
private:
    MDB_val val_;
    std::vector<char> data_;
};


}

#endif
