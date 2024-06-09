#pragma once

#include <string>
#include <fcntl.h>
#include <queue>
#include <cassert>

#define VLOGPADDING 15

#define MAGIC 0xff

#define VLOGPADDING 15

//SSTable 的大小不超过16kB
#define SSTABLESIZE (1 << 14)

#define HEADERSIZE 32

#define KOVSIZE 20

#define BLOOMSIZE 8192

#define BUFFER_SIZE (1024 * 64 + 5)

#define HEAD -0x7ffffff

#define HEADHAED 0

#define NONE "NONE"

#define MINKEY 0x7fffffff

struct kv {
    std::pair<uint64_t, std::string> kv_pair; // 键值对
    uint64_t stamp; // 时间戳
    int i; // 索引

    // 运算符重载 <
    bool operator<(const kv& other) const {
        // 如果两个 kv 的键相同，则比较它们的时间戳，时间戳小的排在前面
        if (kv_pair.first == other.kv_pair.first) {
            return stamp < other.stamp;
        } else {
            // 否则，比较两个 kv 的键，键大的排在前面
            return kv_pair.first > other.kv_pair.first;
        }
    }
};

struct kv_info {
    uint64_t key; // 键
    uint64_t valueLen; // 值的长度
    uint64_t stamp; // 时间戳
    off_t offset; // 偏移量
    int i; // 索引

    // 运算符重载 <
    bool operator<(const kv_info& other) const {
        if (key == other.key) {
            return stamp < other.stamp;
        } else {
            return key > other.key;
        }
    }
};

struct sst_info {
    int level;
    int id;
    std::string file;

    // 运算符重载 <
    bool operator<(const sst_info& other) const {
        if (level == other.level) {
            return id > other.id;
        } else {
            return level > other.level;
        }
    }
};
