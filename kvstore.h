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
    std::string dir_path;
    std::string vlog_path;
    int test_type;
    MemTable  *memTable;
    std::vector<std::vector<SSTable *>> layers;//存储每一层的 SSTable
    void process_vlog();
    void process_sst(std::vector<std::string>& files, std::priority_queue<sst_info>& sstables);
    void write_sst(std::priority_queue<sst_info>& sstables);
    void checkAndConvertMemTable();
    void doCompaction();
    std::string getValueFromMemTable(uint64_t key);
    std::string getValueFromSSTable(uint64_t key);
    void deleteAllSSTables();
    void deleteAllFilesInDir();
    void getPairsFromMemTable(uint64_t key1, uint64_t key2, std::vector<std::vector<std::pair<uint64_t, std::string>>>& scanRes, std::vector<int>& it, std::priority_queue<kv>& kvs);
    void getPairsFromSSTable(uint64_t key1, uint64_t key2, std::vector<std::vector<std::pair<uint64_t, std::string>>>& scanRes, std::vector<int>& it, std::priority_queue<kv>& kvs);
    void removeDeletedPairs(std::list<std::pair<uint64_t, std::string>>& list, std::vector<std::vector<std::pair<uint64_t, std::string>>>& scanRes, std::vector<int>& it, std::priority_queue<kv>& kvs);
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
