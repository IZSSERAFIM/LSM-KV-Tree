#pragma once

#include "kvstore_api.h"
#include "memtable.hpp"
#include "sstable.hpp"

//SSTable 的大小不6kB
#define SSTABLESIZE (1 << 14)
using key_type = uint64_t;
using value_type = std::string;

class KVStore : public KVStoreAPI {
    // You can add your implementation here
private:
    uint64_t stamp;//时间戳
    uint64_t head;//头部
    uint64_t tail;//尾部
    uint64_t bloomSize;//布隆过滤器大小
    int test_mode;
    MemTable<key_type, value_type> *memTable;
    std::vector<std::vector<SSTable<key_type, value_type>*>> layers;
    std::string dir_path;
    std::string vlog_path;

    void compaction(int level);

public:
	KVStore(const std::string &dir, const std::string &vlog);

	~KVStore();

	void put(uint64_t key, const std::string &s) override;

	std::string get(uint64_t key) override;

	bool del(uint64_t key) override;

	void reset() override;

	void scan(uint64_t key1, uint64_t key2, std::list<std::pair<uint64_t, std::string>> &list) override;

	void gc(uint64_t chunk_size) override;
};
