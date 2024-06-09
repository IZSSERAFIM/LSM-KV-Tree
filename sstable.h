#ifndef SSTABLE_H
#define SSTABLE_H

#pragma once

#include <vector>
#include <string>
#include <sstream>
#include <unistd.h>
#include <fcntl.h>
#include "bloomfilter.hpp"
#include "utils.h"
#include "config.h"


struct head_type {
    uint64_t stamp;//时间戳
    uint64_t num_kv;//键值对数量
    uint64_t max_key;//最大键
    uint64_t min_key;//最小键
};


class SSTable {

private:
    int id;
    int level;
    head_type head;
    bloomFilter *bloomfilter;
    std::vector <uint64_t> keys;
    std::vector <uint64_t> offsets;
    std::vector <uint64_t> valueLens;
    std::string dir_path;//SSTable 文件所在的目录
    std::string vlog_path;//vlog 文件所在的目录路径

    void write_sst() const;//将 SSTable 写入磁盘

public:
    //初始化 SSTable 的各个成员变量
    SSTable(head_type head, int level, int id, bloomFilter *bloomFilter, std::vector <uint64_t> keys,
            std::vector <uint64_t> offsets, std::vector <uint64_t> valueLens, std::string dir_path,
            std::string vlog_path);

    //从磁盘读取 SSTable 的数据并初始化成员变量
    SSTable(int level, int id, std::string sstFilename, std::string dir_path, std::string vlog_path,
            uint64_t bloomSize);

    //析构函数
    ~SSTable();

    //从内存中获取指定键对应的值
    std::string get(uint64_t key) const;

    //从磁盘中获取键对应的值
    std::string get_fromdisk(uint64_t key) const;

    //获取键对应的偏移量
    uint64_t get_offset(uint64_t key) const;

    //扫描指定键范围内的所有键值对，并返回一个包含这些键值对的向量
    std::vector <std::pair<uint64_t, std::string>> scan(uint64_t key1, uint64_t key2);

    bool query(uint64_t);

    void write_disk() const;

    void delete_disk() const;

    void set_id(int new_id);

    uint64_t get_numkv() const;

    uint64_t getStamp() const;

    uint64_t get_maxkey() const;

    uint64_t get_minkey() const;

    std::vector <uint64_t> get_keys() const;

    std::vector <uint64_t> get_offsets() const;

    std::vector <uint64_t> get_valueLens() const;
};

#endif //SSTABLE_H