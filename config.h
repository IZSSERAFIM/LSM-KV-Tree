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

#define BUFFER_SIZE (1024 * 48 + 5)

#define HEAD -0x7ffffff

#define MINKEY 0x7fffffff

using key_type = uint64_t;
using value_type = std::string;

struct kv {
    std::pair<key_type, value_type> kv_pair;//键值对
    uint64_t stamp;//时间戳
    int i;//索引
};

struct kv_info {
    key_type key;//键
    uint64_t valueLen;//值的长度
    uint64_t stamp;//时间戳
    off_t offset;//偏移量
    int i;//索引
};

struct sst_info {
    int level;
    int id;
    std::string file;
};
