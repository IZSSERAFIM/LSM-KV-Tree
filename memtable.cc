#include "memtable.h"
#include "memtable.hpp"

MemTable::MemTable(double p, uint64_t bloomSize) {
    this->p = p;
    this->bloomSize = bloomSize;
    max_layer = 1;
    num_kv = 0;
    rand_double = std::uniform_real_distribution<double>(0, 1);
    //初始化头节点
    head.push_back(new Node(HEAD, "NONE", nullptr, nullptr));
    //初始化随机数生成器
    randSeed.seed(time(0));
}

MemTable::~MemTable() {
    // 定义两个指针变量 ptr 和 now_p，用于遍历和删除节点
    MemTable::Node *ptr, *now_p;
    // 遍历每一层的头节点，从最高层到最低层
    for (auto it = head.rbegin(); it != head.rend(); ++it) {
        // 初始化 ptr 为当前层的头节点的下一个节点
        ptr = (*it)->next;
        while (ptr) {
            now_p = ptr;
            ptr = ptr->next;
            delete now_p;
        }
    }
}


void MemTable::put(uint64_t key, const std::string &val) {
    //ptr 指向当前层的头节点
    MemTable::Node *ptr = head[max_layer - 1];
    //former 数组用于存储每一层中最后一个小于或等于 key 的节点
    MemTable::Node *former[max_layer];
    //从最高层开始，逐层向下查找插入位置
    for (int layer = max_layer; layer; layer--) {
        //在每一层中，遍历节点直到找到最后一个小于或等于 key 的节点，并将其存储在 former 数组中
        while (ptr->next && ptr->next->key <= key) {
            ptr = ptr->next;
        }
        former[layer - 1] = ptr;
        //如果找到一个节点的键等于 key，则更新该节点及其下层节点的值，并返回
        if (ptr->key == key) {
            while (ptr) ptr->value = val, ptr = ptr->down;
            return;
        }
            //如果当前节点的下层不为空，则向下移动
        else if (ptr->down) {
            ptr = ptr->down;
        }
    }
    //增加键值对的数量
    num_kv++;
    //调用 getlayer 函数确定新节点的层数
    int new_layer = getlayer();
    //在每一层中插入新节点，更新指针
    ptr = NULL;
    for (int layer = 1; layer <= std::min(max_layer, new_layer); layer++) {
        ptr = former[layer - 1]->next = new MemTable::Node(key, val, ptr, former[layer - 1]->next);
    }
    //如果新节点的层数超过当前最大层数，则增加层数，并创建新的头节点
    for (int layer = max_layer + 1; layer <= new_layer; layer++) {
        MemTable::Node *new_head = new MemTable::Node(HEAD, "NONE", head.back(), NULL);
        ptr = new_head->next = new MemTable::Node(key, val, ptr, NULL);
        head.push_back(new_head);
    }
    //更新 max_layer 为新的最大层数
    max_layer = std::max(max_layer, new_layer);
}

int MemTable::getlayer() {
    int layer = 1;
    while (rand_double(randSeed) < p) {
        layer++;
    }
    return layer;
}


bool MemTable::del(uint64_t key) {
    std::string val = get(key);
    if (val == "~DELETED~" || val == "") {
        return false;
    }
    put(key, "~DELETED~");
    return true;
}


std::string MemTable::get(uint64_t key) const {
    MemTable::Node *ptr = head[max_layer - 1];
    for (int layer = max_layer; layer; layer--) {
        while (ptr->next && ptr->next->key <= key) {
            ptr = ptr->next;
        }
        if (ptr->key == key) {
            return ptr->value;
        } else if (ptr->down) {
            ptr = ptr->down;
        }
    }
    return std::string("");
}


std::vector <std::pair<uint64_t, std::string>> MemTable::scan(uint64_t key1, uint64_t key2) const {
    MemTable::Node *ptr = head[max_layer - 1];
    std::vector <std::pair<uint64_t, std::string>> result;
    //查找起始位置
    while (1) {
        while (ptr->next && ptr->next->key < key1) {
            ptr = ptr->next;
        }
        if (ptr->down) {
            ptr = ptr->down;
        } else break;
    }
    //扫描范围内的键值对
    ptr = ptr->next;
    while (ptr && ptr->key >= key1 && ptr->key <= key2) {
        result.push_back(std::make_pair(ptr->key, ptr->value));
        ptr = ptr->next;
    }
    return result;
}


int MemTable::size() {
    return HEADERSIZE + bloomSize + num_kv * KOVSIZE;
}


int MemTable::get_numkv() {
    return num_kv;
}

SSTable *MemTable::convertSSTable(int id, uint64_t stamp, const std::string &dir, const std::string &vlog) {
    off_t offset;
    int fd;
    uint64_t max_k = 0;
    uint64_t min_k = MINKEY;
    std::vector <uint64_t> keys, offsets, valueLens;
    bloomFilter *bloom_p;

    initializeConversion(vlog, offset, fd, keys, offsets, valueLens, bloom_p);
    processNodes(fd, offset, keys, offsets, valueLens, bloom_p, max_k, min_k);
    SSTable *sst;
    finalizeConversion(fd, sst, id, stamp, dir, vlog, bloom_p, keys, offsets, valueLens, max_k, min_k);

    return sst;
}


void MemTable::write_vlog(Node *p, off_t &offset, int fd) {
    size_t vlog_len = p->value.length() + VLOGPADDING;
    char buf[vlog_len + 5];
    prepareBuffer(p, buf, vlog_len);
    writeBuffer(fd, buf, vlog_len); // 传递 char* 类型的指针
    offset += vlog_len;
}

