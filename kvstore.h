#pragma once

#include "kvstore_api.h"
#include "memtable.h"
#include "sstable.h"
#include "config.h"
#include <vector>
#include <list>
#include <queue>
#include <string>

class KVStore : public KVStoreAPI {
private:
    uint64_t stamp;       // 时间戳
    uint64_t head;        // 头部
    uint64_t tail;        // 尾部
    uint64_t bloomSize;   // 布隆过滤器大小
    std::string dir_path;
    std::string vlog_path;
    MemTable* memTable;
    std::vector<std::vector<SSTable*>> layers; // 存储每一层的 SSTable

    // 私有函数声明
    void process_sst(std::vector<std::string>& files, std::priority_queue<sst_info>& sstables);
    void write_sst(std::priority_queue<sst_info>& sstables);
    void checkAndConvertMemTable();
    void doCompaction();
    std::string getValueFromMemTable(uint64_t key);
    std::string getValueFromSSTable(uint64_t key);
    void deleteAllSSTables();
    void deleteAllFilesInDir();
    bool needCompaction(int level) const;
    bool isMemTableFull() const;
    void convertMemTableToSSTable();
    uint64_t readVlogAndWriteToMemTable(uint64_t chunk_size, int fd, char* buf, uint64_t& read_len);
    int determineCompactSize(int level);
    void updateMinMaxKeys(int compact_size, uint64_t& min_key, uint64_t& max_key, int level);
    void prepareNextLayer(int level);
    void collectOverlappingSSTables(int level, uint64_t min_key, uint64_t max_key, std::vector<int>& index, std::vector<int>& it);
    void addKVToPriorityQueue(int level, int compact_size, std::vector<int>& it, std::priority_queue<kv_info>& kvs, std::vector<int>& index);
    void deleteOldSSTables(int level, std::vector<int>& index, int compact_size);
    void updateSSTableIndices(int level);
    void compaction(int level);
    void createNewSSTables(int level, std::vector<kv_info>& kv_list);
    void process_vlog();
    std::vector<kv_info> collectKVList(std::vector<int>& it, std::priority_queue<kv_info>& kvs, int level, std::vector<int>& index, int compact_size);
    void removeDeletedPairs(std::list<std::pair<uint64_t, std::string>>& list, std::vector<std::vector<std::pair<uint64_t, std::string>>>& scanRes, std::vector<int>& it, std::priority_queue<kv>& kvs);
    void getPairsFromMemTable(uint64_t key1, uint64_t key2, std::vector<std::vector<std::pair<uint64_t, std::string>>>& scanRes, std::vector<int>& it, std::priority_queue<kv>& kvs);
    void getPairsFromSSTable(uint64_t key1, uint64_t key2, std::vector<std::vector<std::pair<uint64_t, std::string>>>& scanRes, std::vector<int>& it, std::priority_queue<kv>& kvs);

    int determineCompactSize(int level, uint64_t& min_key, uint64_t& max_key, uint64_t& max_stamp);
    void prepareNextLevel(int level);
//    void collectOverlappingSSTables(int level, uint64_t min_key, uint64_t max_key, std::vector<int>& index, std::vector<int>& it);
    void mergeAndWriteSSTables(int level, int compact_size, std::vector<int>& index, std::vector<int>& it);

public:
    KVStore(const std::string& dir, const std::string& vlog);
    ~KVStore();
    void put(uint64_t key, const std::string& s) override;
    std::string get(uint64_t key) override;
    bool del(uint64_t key) override;
    void reset() override;
    void scan(uint64_t key1, uint64_t key2, std::list<std::pair<uint64_t, std::string>>& list) override;
    void gc(uint64_t chunk_size) override;
};