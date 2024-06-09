#include "memtable.h"

MemTable::MemTable(double p, uint64_t bloomSize) {
    this->p = p;
    this->bloomSize = bloomSize;
    max_layer = 1;
    num_kv = 0;
    rand_double = std::uniform_real_distribution<double>(0, 1);
    //初始化头节点
    MemTable::Node *new_head = new MemTable::Node(HEAD, std::string("NONE"), NULL, NULL);
    //添加头节点到头节点向量
    head.push_back(new_head);
    //初始化随机数生成器
    randSeed.seed(time(0));
}

MemTable::~MemTable() {
    //定义两个指针变量 ptr 和 now_p，用于遍历和删除节点
    MemTable::Node *ptr, *now_p;
    //遍历每一层的头节点
    for (int layer = max_layer; layer; layer--) {
        //初始化 ptr 为当前层的头节点的下一个节点
        MemTable::Node *ptr = head[layer - 1]->next;
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
        MemTable::Node *new_head = new MemTable::Node(HEAD, std::string("NONE"), head.back(), NULL);
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
    put(key, std::string("~DELETED~"));
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


void MemTable::print_self() {
    if (num_kv > 5) return;
    std::cout << "max_layer " << max_layer << std::endl;
    for (int layer = max_layer; layer; layer--) {
        for (MemTable::Node *ptr = head[layer - 1]; ptr; ptr = ptr->next) {
            std::cout << '[' << ptr->key << ", " << ptr->value << "] ";
        }
        std::cout << std::endl;
    }
}


SSTable *
MemTable::convertSSTable(int id, uint64_t stamp, const std::string &dir,
                         const std::string &vlog) {
    //获取vlog文件的末尾偏移量
    off_t offset = (off_t) utils::get_end_offset(vlog);
    uint64_t max_k = 0;
    uint64_t min_k = MINKEY;
    std::vector <uint64_t> keys;
    std::vector <uint64_t> offsets;
    std::vector <uint64_t> valueLens;
    bloomFilter *bloom_p = new bloomFilter(bloomSize, 3);
    MemTable::Node *ptr = head[0];
    int fd = open(vlog.c_str(), O_RDWR | O_CREAT | O_APPEND, 0644);
    //将文件指针移动到末尾偏移量 offset
    lseek(fd, offset, SEEK_SET);
    while (ptr->next) {
        MemTable::Node *p = ptr->next;
        bloom_p->insert(p->key);
        keys.push_back(p->key);
        offsets.push_back(offset);
        if (p->value != "~DELETED~") {
            if (p->key > max_k) {
                max_k = p->key;
            }
            if (p->key < min_k) {
                min_k = p->key;
            }
            valueLens.push_back(p->value.length());
            write_vlog(p, offset, fd);
        } else {
            valueLens.push_back(0);
        }
        ptr = ptr->next;
    }
    close(fd);
    //创建一个新的 SSTable 对象，传入元数据、布隆过滤器、键、偏移量和值的长度等参数
    SSTable *sst = new SSTable({stamp, num_kv, max_k, min_k}, 0, id,
                               bloom_p, keys, offsets, valueLens, dir,
                               vlog);
    //将 SSTable 写入磁盘
    sst->write_disk();
    //返回创建的 SSTable 对象指针
    return sst;
}


void MemTable::write_vlog(Node *p, off_t &offset, int fd) {
    size_t vlog_len = p->value.length() + VLOGPADDING;
    char buf[vlog_len + 5];
    strcpy(buf + VLOGPADDING, p->value.c_str());
    //开始符号 Magic (1 Byte)
    buf[0] = MAGIC;
    //最后一个字节为 0
    buf[vlog_len] = 0;
    //Checksum (2 Byte)
    *(uint16_t * )(buf + 1) = utils::generate_checksum(p->key, p->value.length(), p->value);
    //Key  (8  Byte)
    *(uint64_t * )(buf + 3) = p->key;
    //vlen (4 Byte)
    *(uint32_t * )(buf + 11) = (uint32_t) p->value.length();
    //调用 utils::write_file 函数将缓冲区 buf 中的vlog entry记录写入文件 fd
    utils::write_file(fd, vlog_len, buf);
    //新偏移量 offset，增加写入的长度 vlog_len
    offset += vlog_len;
}
