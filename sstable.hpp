#ifndef SSTABLE_H
#define SSTABLE_H

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

template<typename key_type, typename value_type>
class SSTable {

private:
    int id;
    int level;
    head_type head;
    bloomFilter *bloomfilter;
    std::vector <key_type> keys;
    std::vector <uint64_t> offsets;
    std::vector <uint64_t> valueLens;
    std::string dir_path;//SSTable 文件所在的目录
    std::string vlog_path;//vlog 文件所在的目录路径

    void write_sst() const;//将 SSTable 写入磁盘

public:
    //初始化 SSTable 的各个成员变量
    SSTable(head_type head, int level, int id, bloomFilter *bloomFilter, std::vector <key_type> keys, std::vector <uint64_t> offsets, std::vector <uint64_t> valueLens, std::string dir_path, std::string vlog_path);

    //从磁盘读取 SSTable 的数据并初始化成员变量
    SSTable(int level, int id, std::string sstFilename, std::string dir_path, std::string vlog_path, uint64_t bloomSize);

    //析构函数
    ~SSTable();

    //从内存中获取指定键对应的值
    value_type get(key_type key) const;

    //从磁盘中获取键对应的值
    value_type get_fromdisk(key_type key) const;

    //获取键对应的偏移量
    uint64_t get_offset(key_type key) const;

    //扫描指定键范围内的所有键值对，并返回一个包含这些键值对的向量
    std::vector <std::pair<key_type, value_type>> scan(key_type key1, key_type key2);

    bool query(key_type);

    void write_disk() const;

    void delete_disk() const;

    void set_id(int new_id);

    uint64_t get_numkv() const;

    uint64_t getStamp() const;

    key_type get_maxkey() const;

    key_type get_minkey() const;

    std::vector <key_type> get_keys() const;

    std::vector <uint64_t> get_offsets() const;

    std::vector <uint64_t> get_valueLens() const;
};


template<typename key_type, typename value_type>
SSTable<key_type, value_type>::SSTable(head_type head, int level, int id, bloomFilter *bloomFilter, std::vector <key_type> keys, std::vector <uint64_t> offsets, std::vector <uint64_t> valueLens, std::string dir_path, std::string vlog_path)
{
    this->head = head;
    this->level = level;
    this->id = id;
    this->bloomfilter = bloomFilter;
    this->keys = keys;
    this->offsets = offsets;
    this->valueLens = valueLens;
    this->dir_path = dir_path;
    this->vlog_path = vlog_path;
}

template<typename key_type, typename value_type>
SSTable<key_type, value_type>::SSTable(int level, int id, std::string sstFilename, std::string dir_path,std::string vlog_path, uint64_t bloomSize){
    this->level = level;
    this->id = id;
    this->dir_path = dir_path;
    this->vlog_path = vlog_path;
    sstFilename = dir_path + "/" + sstFilename;
    //定义一个缓冲区 `buf`，用于读取文件内容。
    char buf[64] = {0};
    //使用 open 函数以读写模式打开文件，文件权限为 0644
    int fd = open(sstFilename.c_str(), O_RDWR, 0644);
    //读取文件的前 32 字节，存入 `buf` 中。
    utils::read_file(fd, -1, 32, buf);
    //将 `buf` 中的内容分别赋值给 `head` 的成员变量。
    //`stamp` 为时间戳，`num_kv` 为键值对数量，`min_key` 为最小键，`max_key` 为最大键。
    head.stamp = *(uint64_t *) buf;
    head.num_kv = *(uint64_t * )(buf + 8);
    head.min_key = *(uint64_t * )(buf + 16);
    head.max_key = *(uint64_t * )(buf + 24);
    //创建一个布隆过滤器 `bloomfilter`，大小为 `bloomSize`，哈希函数个数为 3。
    bloomfilter = new bloomFilter(bloomSize, 3);
    //使用 utils::read_file 函数读取布隆过滤器的数据到 bloomfilter 的数组中
    utils::read_file(fd, -1, bloomfilter->getM(), bloomfilter->getSet());
    key_type key;
    uint64_t offset;
    uint64_t valueLen;
    //循环 head.num_kv 次，每次读取 20 个字节到缓冲区 buf 中，然后将 buf 中的内容分别赋值给 key、offset、valueLen。
    for (int i = 0; i < head.num_kv; i++) {
        utils::read_file(fd, -1, 20, buf);
        key = *(uint64_t *) buf;
        offset = *(uint64_t * )(buf + 8);
        valueLen = *(uint32_t * )(buf + 16);
        keys.push_back(key);
        offsets.push_back(offset);
        valueLens.push_back(valueLen);
    }
    close(fd);
}

template<class key_type, class value_type>
SSTable<key_type, value_type>::~SSTable() {
    //析构函数只析构 bloomfilter，因为 bloomfilter 是动态分配的内存(bloomfilter = new bloomFilter(bloomSize, 3))，需要显式释放。而其他成员变量要么是栈上分配的，要么是标准库类型，会自动管理其内部资源，不需要显式析构
    delete bloomfilter;
}

template<typename key_type, typename value_type>
value_type SSTable<key_type, value_type>::get(key_type key) const {
    //使用 std::lower_bound 在 keys 向量中查找不小于 key 的第一个元素的位置
    typename std::vector<key_type>::const_iterator iter = std::lower_bound(keys.begin(), keys.end(), key);
    //检查键是否存在于 keys 向量中
    if (iter != keys.end() && *iter == key) {
        //计算目标键在 keys 向量中的索引 index
        int index = int(iter - keys.begin());
        //使用索引 index 获取对应的偏移量 offset 和值的长度 size
        off_t offset = offsets[index];
        size_t size = valueLens[index];
        //如果 size 不为零，说明值存在
        if (size) {
            //定义一个大小为 VLOGPADDING + size + 5 的缓冲区 buf，并初始化为零
            char buf[VLOGPADDING + size + 5] = {0};
            //使用 utils::read_file 函数从 vlog_path 路径下的 vlog 文件中读取 size + VLOGPADDING 字节的数据即Vlog Entry对到缓冲区 buf
            utils::read_file(vlog_path, offset, VLOGPADDING + size, buf);
            //返回缓冲区中从 VLOGPADDING 偏移开始的字符串，表示Vlog Entry中的Value
            return std::string(buf + VLOGPADDING);
        }
            //如果 size 为零，说明值已被删除
        else {
            return std::string("~DELETED~");
        }
    }
    //如果没有找到目标键，返回一个空字符串
    return std::string("");
}

template<typename key_type, typename value_type>
value_type SSTable<key_type, value_type>::get_fromdisk(key_type key) const {
    //将 dir_path、level 和 id 拼接成 SST 文件的完整路径
    std::string sstFilename = dir_path + "/" + std::to_string(level) + "-" + std::to_string(id) + ".sst";
    char buf[64] = {0};
    int fd = open(sstFilename.c_str(), O_RDWR, 0644);
    utils::read_file(fd, 0, HEADERSIZE, buf);
    //从缓冲区中解析出键值对的数量 num_kv ,前面的时间戳有8位
    uint64_t num_kv = *(uint64_t * )(buf + 8);
    //使用 lseek 函数将文件指针移动到键值对数据区域的起始位置。这个位置是布隆过滤器大小加上header的大小（32 字节）
    lseek(fd, bloomfilter->getM() + 32, SEEK_SET);
    key_type now_key;
    uint64_t offset;
    uint64_t valueLen;
    //循环 head.num_kv 次，每次读取 20 个字节到缓冲区 buf 中，然后将 buf 中的内容分别赋值给 key、offset、valueLen。
    for (int i = 0; i < head.num_kv; i++) {
        utils::read_file(fd, -1, 20, buf);
        now_key = *(uint64_t *) buf;
        offset = *(uint64_t * )(buf + 8);
        valueLen = *(uint32_t * )(buf + 16);
        if (now_key == key) {
            //如果找到目标键 key，使用 offset 和 valueLen 读取对应的值
            close(fd);
            if (valueLen) {
                char buf[VLOGPADDING + valueLen + 5] = {0};
                utils::read_file(vlog_path, offset, VLOGPADDING + valueLen, buf);
                return std::string(buf + VLOGPADDING);
            }
                //如果 size 为零，说明值已被删除
            else {
                return std::string("~DELETED~");
            }
        }
    }
    close(fd);
    //如果没有找到目标键，返回一个空字符串
    return std::string("");
}

template<typename key_type, typename value_type>
uint64_t SSTable<key_type, value_type>::get_offset(key_type key) const {
    typename std::vector<key_type>::const_iterator iter = std::lower_bound(keys.begin(), keys.end(), key);
    if (iter != keys.end() && *iter == key) {
        int index = int(iter - keys.begin());
        if (!valueLens[index]) {
            return 2;
        }
            //如果不为零，返回对应的偏移量 offsets[index]
        else {
            return offsets[index];
        }
    }
    return 1;
}

template<typename key_type, typename value_type>
std::vector <std::pair<key_type, value_type>> SSTable<key_type, value_type>::scan(key_type key1, key_type key2) {
    //初始化一个空的结果向量 list
    std::vector <std::pair<key_type, value_type>> list;
    //如果 key1 大于 key2，返回空的结果向量
    if (key1 > key2) {
        return list;
    }
    typename std::vector<key_type>::iterator iter1 = std::lower_bound(keys.begin(), keys.end(), key1);
    typename std::vector<key_type>::iterator iter2 = std::lower_bound(keys.begin(), keys.end(), key2);
    //如果 iter2 指向的元素等于 key2，将 iter2 向后移动一个位置，以确保范围是 [key1, key2]
    if (iter2 != keys.end() && *iter2 == key2) {
        iter2++;
    }
    //计算 iter1 和 iter2 在 keys 向量中的索引 index1 和 index2
    int index1 = int(iter1 - keys.begin());
    int index2 = int(iter2 - keys.begin());
    int fd = open(vlog_path.c_str(), O_RDWR, 0644);
    for (int i = index1; i < index2; i++) {
        off_t offset = offsets[i];
        size_t size = valueLens[i];
        if (size) {
            char buf[VLOGPADDING + size + 5] = {0};
            utils::read_file(fd, offset, size + VLOGPADDING, buf);
            list.push_back(std::make_pair(keys[i], std::string(buf + VLOGPADDING)));
        } else {
            list.push_back(std::make_pair(keys[i], std::string("~DELETED~")));
        }
    }
    close(fd);
    return list;
}

template<typename key_type, typename value_type>
bool SSTable<key_type, value_type>::query(key_type key) {
    return bloomfilter->query(key);
}

template<typename key_type, typename value_type>
void SSTable<key_type, value_type>::write_disk() const {
    write_sst();
}

template<typename key_type, typename value_type>
void SSTable<key_type, value_type>::delete_disk() const {
    std::string sstFilename = dir_path + "/" + std::to_string(level) + "-" + std::to_string(id) + ".sst";
    assert(utils::fileExists(sstFilename));
    utils::rmfile(sstFilename);
}

template<typename key_type, typename value_type>
void SSTable<key_type, value_type>::set_id(int new_id) {
    std::string old_sst = dir_path + "/" + std::to_string(level) + "-" + std::to_string(id) + ".sst";
    std::string new_sst = dir_path + "/" + std::to_string(level) + "-" + std::to_string(new_id) + ".sst";
    id = new_id;
    assert(utils::fileExists(old_sst));
    std::rename(old_sst.c_str(), new_sst.c_str());
}

template<typename key_type, typename value_type>
void SSTable<key_type, value_type>::write_sst() const {
    std::string sstFilename =
            dir_path + std::string("/") + std::to_string(level) + std::string("-") + std::to_string(id) +
            std::string(".sst");
    char buf[64] = {0};
    *(uint64_t *) buf = head.stamp;
    *(uint64_t * )(buf + 8) = head.num_kv;
    *(uint64_t * )(buf + 16) = head.min_key;
    *(uint64_t * )(buf + 24) = head.max_key;
    utils::write_file(sstFilename, -1, 32, buf);
    utils::write_file(sstFilename, -1, bloomfilter->getM(), bloomfilter->getSet());
    for (int i = 0; i < keys.size(); i++) {
        *(uint64_t *) buf = keys[i];
        *(uint64_t * )(buf + 8) = offsets[i];
        *(uint32_t * )(buf + 16) = valueLens[i];
        utils::write_file(sstFilename, -1, 20, buf);
    }
}

template<typename key_type, typename value_type>
uint64_t SSTable<key_type, value_type>::get_numkv() const {
    return head.num_kv;
}

template<typename key_type, typename value_type>
uint64_t SSTable<key_type, value_type>::getStamp() const {
    return head.stamp;
}

template<typename key_type, typename value_type>
key_type SSTable<key_type, value_type>::get_maxkey() const {
    return head.max_key;
}

template<typename key_type, typename value_type>
key_type SSTable<key_type, value_type>::get_minkey() const {
    return head.min_key;
}

template<typename key_type, typename value_type>
std::vector <key_type> SSTable<key_type, value_type>::get_keys() const {
    return keys;
}

template<typename key_type, typename value_type>
std::vector <uint64_t> SSTable<key_type, value_type>::get_offsets() const {
    return offsets;
}

template<typename key_type, typename value_type>
std::vector <uint64_t> SSTable<key_type, value_type>::get_valueLens() const {
    return valueLens;
}

#endif //SSTABLE_H