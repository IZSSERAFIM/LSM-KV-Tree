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
    uint64_t readVlogAndWriteToMemTable(uint64_t chunk_size, int fd, char* buf, uint64_t& read_len);
    void convertMemTableToSSTable();
    int determineCompactSize(int level);
    void updateMinMaxKeys(int compact_size, uint64_t& min_key, uint64_t& max_key, int level);
    void prepareNextLayer(int level);
    void collectOverlappingSSTables(int level, uint64_t min_key, uint64_t max_key, std::vector<int>& index, std::vector<int>& it);
    void addKVToPriorityQueue(int level, int compact_size, std::vector<int>& it, std::priority_queue<kv_info>& kvs, std::vector<int>& index);
    std::vector<kv_info> collectKVList(std::vector<int>& it, std::priority_queue<kv_info>& kvs, int level, std::vector<int>& index, int compact_size);
    void deleteOldSSTables(int level, std::vector<int>& index, int compact_size);
    void updateSSTableIndices(int level);
    void compaction(int level);
    void createNewSSTables(int level, std::vector <kv_info> &kv_list) {
        int max_kvnum = (SSTABLESIZE - bloomSize - HEADERSIZE) / 20;
        for (int i = 0; i < kv_list.size(); i += max_kvnum) {
            uint64_t max_key = 0;
            uint64_t min_key = MINKEY;
            uint64_t new_step = 0;
            uint64_t kv_num = std::min(max_kvnum, (int) kv_list.size() - i);
            std::vector <uint64_t> keys;
            std::vector <uint64_t> offsets, valueLens;
            bloomFilter *bloom_p = new bloomFilter(bloomSize, 3);
            for (int j = i; j < std::min(i + max_kvnum, (int) kv_list.size()); j++) {
                max_key = std::max(max_key, kv_list[j].key);
                min_key = std::min(min_key, kv_list[j].key);
                new_step = std::max(new_step, kv_list[j].stamp);
                keys.push_back(kv_list[j].key);
                offsets.push_back(kv_list[j].offset);
                valueLens.push_back(kv_list[j].valueLen);
                bloom_p->insert(kv_list[j].key);
            }
            SSTable *sst = new SSTable({new_step, kv_num, max_key, min_key}, level + 1, layers[level + 1].size(), bloom_p,
                                       keys, offsets, valueLens, dir_path, vlog_path);
            sst->write_disk();
            layers[level + 1].push_back(sst);
        }
    }



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

