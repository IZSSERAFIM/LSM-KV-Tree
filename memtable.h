#ifndef MEMTABLE_H
#define MEMTABLE_H

#pragma once

#include <cstdint>
#include <cassert>
#include <vector>
#include <string>
#include <random>
#include <time.h>
#include <list>
#include <iostream>
#include "sstable.h"
#include "utils.h"
#include "config.h"

class MemTable {

private:
    //用于控制新节点提升层数的概率
    double p;
    uint64_t bloomSize;
    int max_layer;
    int num_kv;

    struct Node {
        uint64_t key;
        std::string value;
        Node *down, *next;

        Node(uint64_t k, std::string v, Node *d, Node *n) {
            key = k;
            value = v;
            down = d;
            next = n;
        }
    };

    //存储每一层的头节点
    std::vector<Node *> head;
    //随机数生成器
    std::mt19937_64 randSeed;
    //用于生成 [0, 1) 之间的均匀分布的随机数
    std::uniform_real_distribution<double> rand_double;

    //将节点写入 vlog 文件
    void write_vlog(Node *p, off_t &offset, int fd);

    //获取新节点的层数
    int getlayer();

    void initializeConversion(const std::string &vlog, off_t &offset, int &fd, std::vector <uint64_t> &keys,
                              std::vector <uint64_t> &offsets, std::vector <uint64_t> &valueLens,
                              bloomFilter *&bloom_p);

    void processNodes(int fd, off_t &offset, std::vector <uint64_t> &keys, std::vector <uint64_t> &offsets,
                      std::vector <uint64_t> &valueLens, bloomFilter *bloom_p, uint64_t &max_k, uint64_t &min_k);

    void
    finalizeConversion(int fd, SSTable *&sst, int id, uint64_t stamp, const std::string &dir, const std::string &vlog,
                       bloomFilter *bloom_p, const std::vector <uint64_t> &keys, const std::vector <uint64_t> &offsets,
                       const std::vector <uint64_t> &valueLens, uint64_t max_k, uint64_t min_k);

    void prepareBuffer(Node *p, char *buf, size_t vlog_len);

    void writeBuffer(int fd, char *buf, size_t vlog_len); // 修改参数类型为 char*
    uint16_t generateChecksum(uint64_t key, const std::string &value);

    void findAndUpdate(uint64_t key, const std::string &val, Node *former[]);
    void insertNewNodes(uint64_t key, const std::string &val, Node *former[], int new_layer);
    void updateHeadNodes(uint64_t key, const std::string &val, int new_layer);

    void deleteLayerNodes(Node* head);
    void deleteAllNodes();

public:
    //构造函数
    explicit MemTable(double p, uint64_t bloomSize);

    //析构函数
    ~MemTable();

    //在表中插入一个键值对
    void put(uint64_t key, const std::string &val);

    //删除一个键值对
    bool del(uint64_t key);

    //获取指定键对应的值
    std::string get(uint64_t key) const;

    //扫描指定键范围内的所有键值对，并返回一个包含这些键值对的向量
    std::vector <std::pair<uint64_t, std::string>> scan(uint64_t key1, uint64_t key2) const;

    //获取一个sstable大小
    int size();

    //获取键值对数量
    int get_numkv();

    //将 memtable 转换为 sstable
    SSTable *convertSSTable(int id, uint64_t stamp, const std::string &dir, const std::string &vlog);
};

#endif //MEMTABLE_H