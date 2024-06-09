#pragma once

#include "kvstore_api.h"
#include "memtable.h"
#include "sstable.h"
#include "config.h"


class KVStore : public KVStoreAPI {
    // You can add your implementation here
private:
    uint64_t stamp;//时间戳
    uint64_t head;//头部
    uint64_t tail;//尾部
    uint64_t bloomSize;//布隆过滤器大小
    int test_type;
    MemTable  *memTable;
    std::vector<std::vector<SSTable *>> layers;//存储每一层的 SSTable
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
